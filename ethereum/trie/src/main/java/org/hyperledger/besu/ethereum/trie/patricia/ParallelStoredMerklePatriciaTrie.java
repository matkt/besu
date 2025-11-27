/*
 * Copyright contributors to Hyperledger Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.trie.patricia;

import static org.hyperledger.besu.ethereum.trie.CompactEncoding.bytesToPath;

import org.hyperledger.besu.ethereum.trie.CommitVisitor;
import org.hyperledger.besu.ethereum.trie.MerkleStorage;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.PathNodeVisitor;
import org.hyperledger.besu.ethereum.trie.StoredNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A {@link MerkleTrie} that persists trie nodes to a {@link MerkleStorage} key/value store.
 *
 * @param <V> The type of values stored by this trie.
 */
@SuppressWarnings("rawtypes")
public class ParallelStoredMerklePatriciaTrie<K extends Bytes, V>
    extends StoredMerklePatriciaTrie<K, V> {

  private static final int NCPU = Runtime.getRuntime().availableProcessors();
  private static final ExecutorService executorService = Executors.newFixedThreadPool(NCPU * 2);
  private final Map<K, Optional<V>> pendingUpdates = new HashMap<>();

  public ParallelStoredMerklePatriciaTrie(
      final NodeLoader nodeLoader,
      final Function<V, Bytes> valueSerializer,
      final Function<Bytes, V> valueDeserializer) {
    super(nodeLoader, valueSerializer, valueDeserializer);
  }

  public ParallelStoredMerklePatriciaTrie(
      final NodeLoader nodeLoader,
      final Bytes32 rootHash,
      final Bytes rootLocation,
      final Function<V, Bytes> valueSerializer,
      final Function<Bytes, V> valueDeserializer) {
    super(nodeLoader, rootHash, rootLocation, valueSerializer, valueDeserializer);
  }

  public ParallelStoredMerklePatriciaTrie(
      final NodeLoader nodeLoader,
      final Bytes32 rootHash,
      final Function<V, Bytes> valueSerializer,
      final Function<Bytes, V> valueDeserializer) {
    super(nodeLoader, rootHash, valueSerializer, valueDeserializer);
  }

  public ParallelStoredMerklePatriciaTrie(
      final StoredNodeFactory<V> nodeFactory, final Bytes32 rootHash) {
    super(nodeFactory, rootHash);
  }

  @Override
  public void put(final K key, final V value) {
    pendingUpdates.put(key, Optional.of(value));
  }

  @Override
  public void remove(final K key) {
    pendingUpdates.put(key, Optional.empty());
  }

  @Override
  public void commit(final NodeUpdater nodeUpdater) {
    if (!pendingUpdates.isEmpty()) {
      try {
        loadRootNode();
        if (root instanceof BranchNode<V>) {
          processUpdatesInParallel(Optional.of(nodeUpdater));
        } else {
          System.out.println("Committing node in sequential mode");
          processUpdatesSequentially(Optional.of(nodeUpdater));
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException("Failed to process parallel updates", e);
      }
      pendingUpdates.clear();
    }
  }

  @Override
  public Bytes32 getRootHash() {
    if (pendingUpdates.isEmpty()) {
      return root.getHash();
    }
    try {
      loadRootNode();
      if (root instanceof BranchNode<V>) {
        processUpdatesInParallel(Optional.empty());
      } else {
        processUpdatesSequentially(Optional.empty());
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Failed to process parallel updates", e);
    }
    pendingUpdates.clear();
    return root.getHash();
  }

  private void processUpdatesInParallel(final Optional<NodeUpdater> maybeNodeUpdater)
      throws InterruptedException, ExecutionException {
    final Map<Byte, List<Map.Entry<K, Optional<V>>>> groupedByNibble =
        pendingUpdates.entrySet().stream()
            .collect(Collectors.groupingBy(entry -> getFirstNibble(entry.getKey())));

    final RootBranchNodeWrapper nodeWrapper = new RootBranchNodeWrapper((BranchNode<V>) root);
    final List<CompletableFuture<Void>> futures = new ArrayList<>(groupedByNibble.size());

    for (final Map.Entry<Byte, List<Map.Entry<K, Optional<V>>>> group :
        groupedByNibble.entrySet()) {
      final byte nibble = group.getKey();
      final List<Map.Entry<K, Optional<V>>> updates = group.getValue();

      final CompletableFuture<Void> future =
          CompletableFuture.runAsync(
              () -> processGroupUpdates(nodeWrapper, nibble, updates, maybeNodeUpdater),
              executorService);

      futures.add(future);
    }

    // Wait for all parallel tasks to complete
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    this.root = nodeWrapper.applyUpdates();
    if (maybeNodeUpdater.isPresent()) {
      // Make sure root node was stored
      final Bytes32 rootHash = root.getHash();
      maybeNodeUpdater.get().store(Bytes.EMPTY, rootHash, root.getEncodedBytes());
      // Reset root so dirty nodes can be garbage collected
      this.root =
          rootHash.equals(EMPTY_TRIE_NODE_HASH)
              ? NullNode.instance()
              : new StoredNode<>(nodeFactory, Bytes.EMPTY, rootHash);
    }
  }

  private void processUpdatesSequentially(final Optional<NodeUpdater> maybeNodeUpdater) {
    for (final Map.Entry<K, Optional<V>> entry : pendingUpdates.entrySet()) {
      final Optional<V> value = entry.getValue();
      if (value.isPresent()) {
        super.put(entry.getKey(), value.get());
      } else {
        super.remove(entry.getKey());
      }
    }

    if (maybeNodeUpdater.isPresent()) {
      super.commit(maybeNodeUpdater.get());
    }
  }

  private void processGroupUpdates(
      final RootBranchNodeWrapper nodeWrapper,
      final byte nibble,
      final List<Map.Entry<K, Optional<V>>> updates,
      final Optional<NodeUpdater> maybeNodeUpdater) {

    Node<V> child = nodeWrapper.getPendingChildren().get(nibble);

    for (final Map.Entry<K, Optional<V>> entry : updates) {
      final Bytes path = bytesToPath(entry.getKey()).slice(1); // Remove first nibble
      final Optional<V> value = entry.getValue();

      final PathNodeVisitor<V> visitor =
          value.isPresent() ? getPutVisitor(value.get()) : getRemoveVisitor();

      child = child.accept(visitor, path);
    }

    if (maybeNodeUpdater.isPresent()) {
      child.accept(Bytes.of(nibble), new CommitVisitor<>(maybeNodeUpdater.get()));
    } else {
      Objects.requireNonNull(child.getHash()); // force getHash
    }

    nodeWrapper.setChildren(nibble, child);
  }

  private byte getFirstNibble(final K key) {
    final Bytes path = bytesToPath(key);
    if (path.isEmpty()) {
      return 0;
    }
    return (byte) (path.get(0) & 0xFF);
  }

  private void loadRootNode() {
    this.root =
        this.root.accept(
            new PathNodeVisitor<V>() {
              @Override
              public Node<V> visit(ExtensionNode<V> extensionNode, Bytes path) {
                return extensionNode;
              }

              @Override
              public Node<V> visit(BranchNode<V> branchNode, Bytes path) {
                return branchNode;
              }

              @Override
              public Node<V> visit(LeafNode<V> leafNode, Bytes path) {
                return leafNode;
              }

              @Override
              public Node<V> visit(NullNode<V> nullNode, Bytes path) {
                return nullNode;
              }
            },
            Bytes.EMPTY);
  }

  class RootBranchNodeWrapper {
    private final BranchNode<V> root;
    private final List<Node<V>> pendingChildren;

    public RootBranchNodeWrapper(final BranchNode<V> root) {
      this.root = root;
      loadRootNode();
      this.pendingChildren = Collections.synchronizedList(new ArrayList<>(root.getChildren()));
    }

    public List<Node<V>> getPendingChildren() {
      return pendingChildren;
    }

    public void setChildren(final byte index, final Node<V> children) {
      this.pendingChildren.set(index, children);
    }

    public Node<V> applyUpdates() {
      return this.root.replaceAllChildren(pendingChildren, true);
    }
  }
}
