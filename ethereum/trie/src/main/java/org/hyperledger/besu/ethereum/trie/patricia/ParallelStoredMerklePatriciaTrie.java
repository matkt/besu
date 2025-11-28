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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A parallel implementation of StoredMerklePatriciaTrie that processes updates in parallel when the
 * root is a BranchNode, with two-level parallelism for large groups.
 *
 * @param <K> The type of keys
 * @param <V> The type of values stored by this trie
 */
@SuppressWarnings("rawtypes")
public class ParallelStoredMerklePatriciaTrie<K extends Bytes, V>
    extends StoredMerklePatriciaTrie<K, V> {

  private static final ExecutorService EXECUTOR = Executors.newWorkStealingPool(16);
  private static final int SMALL_GROUP_THRESHOLD = 50;
    private static final int MIN_UPDATES_FOR_PARALLEL = 100;
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
    processPendingUpdates(Optional.of(nodeUpdater));
  }

  @Override
  public Bytes32 getRootHash() {
    if (pendingUpdates.isEmpty()) {
      return root.getHash();
    }
    processPendingUpdates(Optional.empty());
    return root.getHash();
  }

  private void processPendingUpdates(final Optional<NodeUpdater> maybeNodeUpdater) {
    if (pendingUpdates.isEmpty()) {
      return;
    }

    try {
      if (pendingUpdates.size() >= MIN_UPDATES_FOR_PARALLEL && loadRootNode() instanceof BranchNode<V>) {
        processUpdatesInParallel(maybeNodeUpdater);
      } else {
        processUpdatesSequentially(maybeNodeUpdater);
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Failed to process updates", e);
    } finally {
      pendingUpdates.clear();
    }
  }

  private void processUpdatesInParallel(final Optional<NodeUpdater> maybeNodeUpdater)
      throws InterruptedException, ExecutionException {

    final Map<Byte, List<Map.Entry<K, Optional<V>>>> groupedByNibble =
        pendingUpdates.entrySet().stream()
            .collect(Collectors.groupingBy(entry -> getNibble(entry.getKey(), 0)));

    final BranchNodeWrapper nodeWrapper = new BranchNodeWrapper((BranchNode<V>) root);

    final List<CompletableFuture<Void>> futures =
        processGroupsBySize(
            groupedByNibble, entry -> processFirstLevelGroup(nodeWrapper, entry, maybeNodeUpdater));

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    this.root = nodeWrapper.applyUpdates();

    maybeNodeUpdater.ifPresent(this::storeAndResetRoot);
  }

  private void processFirstLevelGroup(
      final BranchNodeWrapper nodeWrapper,
      final Map.Entry<Byte, List<Map.Entry<K, Optional<V>>>> entry,
      final Optional<NodeUpdater> maybeNodeUpdater) {

    final byte nibble = entry.getKey();
    final List<Map.Entry<K, Optional<V>>> updates = entry.getValue();
    final Node<V> child = nodeWrapper.getPendingChildren().get(nibble);

    if (child instanceof BranchNode && updates.size() >= SMALL_GROUP_THRESHOLD) {
      processWithSecondLevelParallelism(
          nodeWrapper, nibble, (BranchNode<V>) child, updates, maybeNodeUpdater);
    } else {
      processNodeUpdates(
          nodeWrapper, nibble, child, Bytes.of(nibble), updates, 1, maybeNodeUpdater);
    }
  }

  private void processWithSecondLevelParallelism(
      final BranchNodeWrapper parentWrapper,
      final byte parentNibble,
      final BranchNode<V> branchNode,
      final List<Map.Entry<K, Optional<V>>> updates,
      final Optional<NodeUpdater> maybeNodeUpdater) {

    final Map<Byte, List<Map.Entry<K, Optional<V>>>> secondLevel =
        updates.stream().collect(Collectors.groupingBy(entry -> getNibble(entry.getKey(), 1)));

    final BranchNodeWrapper branchWrapper = new BranchNodeWrapper(branchNode);

    final List<CompletableFuture<Void>> futures =
        processGroupsBySize(
            secondLevel,
            entry -> processSecondLevelGroup(branchWrapper, parentNibble, entry, maybeNodeUpdater));

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    parentWrapper.setChildren(parentNibble, branchWrapper.applyUpdates());
  }

  private void processSecondLevelGroup(
      final BranchNodeWrapper branchWrapper,
      final byte parentNibble,
      final Map.Entry<Byte, List<Map.Entry<K, Optional<V>>>> entry,
      final Optional<NodeUpdater> maybeNodeUpdater) {

    final byte nibble = entry.getKey();
    final Node<V> child = branchWrapper.getPendingChildren().get(nibble);

    processNodeUpdates(
        branchWrapper,
        nibble,
        child,
        Bytes.of(parentNibble, nibble),
        entry.getValue(),
        2,
        maybeNodeUpdater);
  }

  private void processNodeUpdates(
      final BranchNodeWrapper nodeWrapper,
      final byte nibbleIndex,
      final Node<V> node,
      final Bytes location,
      final List<Map.Entry<K, Optional<V>>> updates,
      final int pathSliceOffset,
      final Optional<NodeUpdater> maybeNodeUpdater) {

    Node<V> tmpNode = node;

    for (final Map.Entry<K, Optional<V>> entry : updates) {
      final Bytes path = bytesToPath(entry.getKey()).slice(pathSliceOffset);
      final PathNodeVisitor<V> visitor =
          entry.getValue().isPresent() ? getPutVisitor(entry.getValue().get()) : getRemoveVisitor();
      tmpNode = tmpNode.accept(visitor, path);
    }

    if (maybeNodeUpdater.isPresent()) {
      tmpNode.accept(location, new CommitVisitor<>(maybeNodeUpdater.get()));
    } else {
      Objects.requireNonNull(tmpNode.getHash());
    }

    nodeWrapper.setChildren(nibbleIndex, tmpNode);
  }

  private List<CompletableFuture<Void>> processGroupsBySize(
      final Map<Byte, List<Map.Entry<K, Optional<V>>>> groupedUpdates,
      final Consumer<Map.Entry<Byte, List<Map.Entry<K, Optional<V>>>>> processor) {

    final Map<Boolean, List<Map.Entry<Byte, List<Map.Entry<K, Optional<V>>>>>> partitioned =
        groupedUpdates.entrySet().stream()
            .collect(Collectors.partitioningBy(e -> e.getValue().size() >= SMALL_GROUP_THRESHOLD));

    final List<CompletableFuture<Void>> futures = new ArrayList<>();

    // Process large groups in parallel
    partitioned
        .get(true)
        .forEach(
            entry ->
                futures.add(CompletableFuture.runAsync(() -> processor.accept(entry), EXECUTOR)));

    // Process small groups together in one async task
    final List<Map.Entry<Byte, List<Map.Entry<K, Optional<V>>>>> smallGroups =
        partitioned.get(false);
    if (!smallGroups.isEmpty()) {
      futures.add(CompletableFuture.runAsync(() -> smallGroups.forEach(processor), EXECUTOR));
    }

    return futures;
  }

  private void processUpdatesSequentially(final Optional<NodeUpdater> maybeNodeUpdater) {
    pendingUpdates.forEach(
        (key, value) -> {
          if (value.isPresent()) {
            super.put(key, value.get());
          } else {
            super.remove(key);
          }
        });
    maybeNodeUpdater.ifPresent(super::commit);
  }

  private void storeAndResetRoot(final NodeUpdater nodeUpdater) {
    final Bytes32 rootHash = root.getHash();
    nodeUpdater.store(Bytes.EMPTY, rootHash, root.getEncodedBytes());
    this.root =
        rootHash.equals(EMPTY_TRIE_NODE_HASH)
            ? NullNode.instance()
            : new StoredNode<>(nodeFactory, Bytes.EMPTY, rootHash);
  }

  private byte getNibble(final K key, final int index) {
    if (key.isEmpty()) {
      return 0;
    }
    return index == 0 ? (byte) ((key.get(0) >> 4) & 0x0F) : (byte) (key.get(0) & 0x0F);
  }

  private Node<V> loadRootNode() {
    this.root =
        this.root.accept(
            new PathNodeVisitor<V>() {
              @Override
              public Node<V> visit(final ExtensionNode<V> extensionNode, final Bytes path) {
                return extensionNode;
              }

              @Override
              public Node<V> visit(final BranchNode<V> branchNode, final Bytes path) {
                return branchNode;
              }

              @Override
              public Node<V> visit(final LeafNode<V> leafNode, final Bytes path) {
                return leafNode;
              }

              @Override
              public Node<V> visit(final NullNode<V> nullNode, final Bytes path) {
                return nullNode;
              }
            },
            Bytes.EMPTY);
    return this.root;
  }

  class BranchNodeWrapper {
    private final BranchNode<V> root;
    private final List<Node<V>> pendingChildren;

    public BranchNodeWrapper(final BranchNode<V> root) {
      this.root = root;
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
