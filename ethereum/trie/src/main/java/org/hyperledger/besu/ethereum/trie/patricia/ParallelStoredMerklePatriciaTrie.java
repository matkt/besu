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

import org.hyperledger.besu.ethereum.trie.MerkleStorage;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.PathNodeVisitor;

import java.util.ArrayList;
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

  private static final ExecutorService executorService = Executors.newFixedThreadPool(16);
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
  public Bytes32 getRootHash() {
    if (pendingUpdates.isEmpty()) {
      return root.getHash();
    }

    pendingUpdates.forEach((k, v) -> {
        System.out.println(root.getHash()+" "+k+" "+v);
    });
    try {
      Objects.requireNonNull(root.getChildren()); // force load children
      if (root.getChildren().size()==150) {
        processUpdatesInParallel();
      } else {
        processUpdatesSequentially();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Failed to process parallel updates", e);
    }
    pendingUpdates.clear();
    return root.getHash();
  }

  private void processUpdatesInParallel() throws InterruptedException, ExecutionException {
    final Map<Byte, List<Map.Entry<K, Optional<V>>>> groupedByNibble =
        pendingUpdates.entrySet().stream()
            .collect(Collectors.groupingBy(entry -> getFirstNibble(entry.getKey())));

    final List<CompletableFuture<Void>> futures = new ArrayList<>(groupedByNibble.size());

    for (final Map.Entry<Byte, List<Map.Entry<K, Optional<V>>>> group :
        groupedByNibble.entrySet()) {
      final byte nibble = group.getKey();
      final List<Map.Entry<K, Optional<V>>> updates = group.getValue();

      final CompletableFuture<Void> future =
          CompletableFuture.runAsync(() -> processGroupUpdates(nibble, updates), executorService);

      futures.add(future);
    }

    // Wait for all parallel tasks to complete
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    final BranchNode<V> branchRoot = (BranchNode<V>) root;
    List<Node<V>> children = root.getChildren();
    for (int i = 0; i < branchRoot.maxChild(); i++) {
      root = ((BranchNode<V>) root).replaceChild((byte) i, children.get(i));
    }
  }

  private void processUpdatesSequentially() {
    for (final Map.Entry<K, Optional<V>> entry : pendingUpdates.entrySet()) {
      final Bytes path = bytesToPath(entry.getKey());
      final Optional<V> value = entry.getValue();

      final PathNodeVisitor<V> visitor;
      if (value.isPresent()) {
        visitor = getPutVisitor(value.get());
      } else {
        visitor = getRemoveVisitor();
      }
      root = root.accept(visitor, path);
    }
  }

  private void processGroupUpdates(
      final byte nibble, final List<Map.Entry<K, Optional<V>>> updates) {

    Node<V> child = root.getChildren().get(nibble);

    for (final Map.Entry<K, Optional<V>> entry : updates) {
      final Bytes path = bytesToPath(entry.getKey()).slice(1); // Remove first nibble
      final Optional<V> value = entry.getValue();

      final PathNodeVisitor<V> visitor =
          value.isPresent() ? getPutVisitor(value.get()) : getRemoveVisitor();

      child = child.accept(visitor, path);
    }

    Objects.requireNonNull(child.getHash());

    final BranchNode<V> branchRoot = (BranchNode<V>) root;
    branchRoot.getChildren().set(nibble, child);
  }

  private byte getFirstNibble(final K key) {
    final Bytes path = bytesToPath(key);
    if (path.isEmpty()) {
      return 0;
    }
    return (byte) (path.get(0) & 0xFF);
  }
}
