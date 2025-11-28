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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Parallel implementation of StoredMerklePatriciaTrie that processes updates concurrently.
 *
 * <p>This implementation recursively descends to any depth where BranchNodes exist and where
 * updates are sufficient to justify parallelism. It uses a deferred commit strategy to avoid
 * synchronization bottlenecks during parallel processing.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Parallel processing of independent trie branches
 *   <li>Virtual threads for deep descents to maximize throughput
 *   <li>Thread-safe commit cache to prevent concurrent write conflicts
 *   <li>Automatic fallback to sequential processing for small batches
 *   <li>Adaptive executor selection based on recursion depth
 * </ul>
 *
 * <p>Performance considerations:
 *
 * <ul>
 *   <li>Platform threads (2x CPU cores) for top-level parallelism
 *   <li>Virtual threads for deep recursive operations (depth >= 2)
 *   <li>Minimum thresholds prevent overhead on small update sets
 * </ul>
 *
 * @param <K> The type of keys
 * @param <V> The type of values stored by this trie
 */
@SuppressWarnings("rawtypes")
public class ParallelStoredMerklePatriciaTrie<K extends Bytes, V>
    extends StoredMerklePatriciaTrie<K, V> {

  /**
   * Platform thread pool for top-level parallel operations. Sized at 2x CPU cores (minimum 4) for
   * optimal throughput on shallow operations.
   */
  private static final ExecutorService PLATFORM_THREAD_POOL =
      Executors.newFixedThreadPool(
          Math.max(4, Runtime.getRuntime().availableProcessors() * 2),
          r -> {
            Thread t = new Thread(r);
            t.setName("trie-worker-" + t.getId());
            t.setDaemon(true);
            return t;
          });

  /**
   * Virtual thread pool for deep recursive operations. Virtual threads are lightweight and ideal
   * for high-depth tree traversal.
   */
  private static final ExecutorService VIRTUAL_THREAD_POOL =
      Executors.newVirtualThreadPerTaskExecutor();

  /**
   * Depth threshold at which virtual threads are used instead of platform threads. Virtual threads
   * are more efficient for deep recursion due to lower memory overhead.
   */
  private static final int DEPTH_THRESHOLD_FOR_VIRTUAL = 2;

  /**
   * Minimum number of updates required to trigger parallel processing. Below this threshold,
   * sequential processing is more efficient due to overhead.
   */
  private static final int MINIMUM_UPDATES_FOR_PARALLEL_PROCESSING = 10;

  /**
   * Buffer for pending updates that will be batch-processed on commit or getRootHash. Key: The trie
   * key to update Value: Optional.empty() for removals, Optional.of(value) for insertions/updates
   */
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
    applyPendingUpdatesAndOptionallyCommit(Optional.of(nodeUpdater));
  }

  @Override
  public Bytes32 getRootHash() {
    if (pendingUpdates.isEmpty()) {
      return root.getHash();
    }
    applyPendingUpdatesAndOptionallyCommit(Optional.empty());
    return root.getHash();
  }

  /**
   * Processes all pending updates, choosing parallel or sequential strategy based on conditions.
   *
   * <p>Parallel processing is used when:
   *
   * <ul>
   *   <li>Update count >= MIN_UPDATES_FOR_PARALLEL
   *   <li>Root node is a BranchNode (allows distribution across branches)
   * </ul>
   *
   * <p>Otherwise, sequential processing is used for efficiency.
   *
   * @param maybeNodeUpdater Optional updater for persisting nodes; empty for hash-only computation
   */
  private void applyPendingUpdatesAndOptionallyCommit(
      final Optional<NodeUpdater> maybeNodeUpdater) {
    if (pendingUpdates.isEmpty()) {
      return;
    }
    try {
      if (pendingUpdates.size() >= MINIMUM_UPDATES_FOR_PARALLEL_PROCESSING
          && loadAndResolveRootNode() instanceof BranchNode<V>) {
        applyUpdatesInParallel(maybeNodeUpdater);
      } else {
        // fallback to non parallel processing
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
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Failed to process updates", e);
    } finally {
      pendingUpdates.clear();
    }
  }

  /**
   * Processes updates in parallel by distributing them across root-level branches.
   *
   * <p>Algorithm:
   *
   * <ol>
   *   <li>Convert all updates to UpdateEntry objects with nibble paths
   *   <li>Group updates by their first nibble (0-15)
   *   <li>Create a parallel task for each group
   *   <li>Wait for all tasks to complete
   *   <li>Apply accumulated changes to root node
   *   <li>Commit to storage if NodeUpdater is provided
   * </ol>
   *
   * @param maybeNodeUpdater Optional updater for persisting nodes
   * @throws InterruptedException If parallel processing is interrupted
   * @throws ExecutionException If any parallel task fails
   */
  private void applyUpdatesInParallel(final Optional<NodeUpdater> maybeNodeUpdater)
      throws InterruptedException, ExecutionException {

    final NodeCommitCache commitCache = new NodeCommitCache();

    // Convert pending updates to entries with nibble-encoded paths
    final List<UpdateEntry<V>> updateEntries =
        pendingUpdates.entrySet().stream()
            .map(entry -> new UpdateEntry<>(bytesToPath(entry.getKey()), entry.getValue()))
            .toList();

    // Group updates by first nibble for root branch distribution
    final Map<Byte, List<UpdateEntry<V>>> updatesByFirstNibble =
        groupUpdatesByNibbleAtIndex(updateEntries, 0);

    final ThreadSafeBranchNodeWrapper rootWrapper =
        new ThreadSafeBranchNodeWrapper((BranchNode<V>) root, Bytes.EMPTY);

    final List<CompletableFuture<Void>> branchUpdateFutures =
        createParallelBranchUpdateTasks(
            rootWrapper,
            updatesByFirstNibble,
            maybeNodeUpdater.isPresent() ? Optional.of(commitCache) : Optional.empty());

    CompletableFuture.allOf(branchUpdateFutures.toArray(new CompletableFuture[0])).join();

    this.root = rootWrapper.applyUpdates();

    if (maybeNodeUpdater.isPresent()) {
      commitCache.flushTo(maybeNodeUpdater.get());
      persistRootAndReset(maybeNodeUpdater.get());
    }
  }

  /**
   * Groups update entries by nibble value at specified index.
   *
   * @param updates The list of all update entries
   * @param nibbleIndex The index of the nibble to group by (0-63)
   * @return Map of nibble value (0-15) to list of updates
   */
  private Map<Byte, List<UpdateEntry<V>>> groupUpdatesByNibbleAtIndex(
      final List<UpdateEntry<V>> updates, final int nibbleIndex) {
    return updates.stream().collect(Collectors.groupingBy(entry -> entry.getNibble(nibbleIndex)));
  }

  /**
   * Creates parallel tasks for updating each branch of a BranchNode.
   *
   * @param currentNodeWrapper The wrapper for the BranchNode being updated
   * @param updatesByNibble Map of nibble to updates for that branch
   * @param maybeCommitCache Optional cache for deferred commits
   * @return List of CompletableFutures for all branch update tasks
   */
  private List<CompletableFuture<Void>> createParallelBranchUpdateTasks(
      final ThreadSafeBranchNodeWrapper currentNodeWrapper,
      final Map<Byte, List<UpdateEntry<V>>> updatesByNibble,
      final Optional<NodeCommitCache> maybeCommitCache) {

    final int currentDepth = currentNodeWrapper.getDepth() + 1;
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    final ExecutorService executor = selectExecutorForDepth(currentDepth);

    for (Map.Entry<Byte, List<UpdateEntry<V>>> entry : updatesByNibble.entrySet()) {
      final byte childNibble = entry.getKey();
      final Node<V> childNode = currentNodeWrapper.getPendingChildren().get(childNibble);
      final List<UpdateEntry<V>> childUpdates = entry.getValue();

      // Parallelize if enough updates justify the overhead
      if (childUpdates.size() >= MINIMUM_UPDATES_FOR_PARALLEL_PROCESSING
          && childNode instanceof BranchNode<V>) {
        futures.add(
            CompletableFuture.runAsync(
                () ->
                    applyUpdatesToBranchRecursively(
                        currentNodeWrapper,
                        childNibble,
                        (BranchNode<V>) childNode,
                        childUpdates,
                        maybeCommitCache),
                executor));
      } else {
        // Process small batches synchronously to avoid task overhead
        applyUpdatesToNodeSequentially(
            currentNodeWrapper, childNibble, childNode, childUpdates, maybeCommitCache);
      }
    }

    return futures;
  }

  /**
   * Selects the appropriate executor based on current depth. Uses virtual threads for deep
   * recursion to minimize memory overhead.
   *
   * @param currentDepth The current depth in the trie
   * @return The executor to use for parallel tasks at this depth
   */
  private ExecutorService selectExecutorForDepth(final int currentDepth) {
    return currentDepth >= DEPTH_THRESHOLD_FOR_VIRTUAL ? VIRTUAL_THREAD_POOL : PLATFORM_THREAD_POOL;
  }

  /**
   * Recursively processes a group of updates for a specific branch.
   *
   * <p>This method decides whether to:
   *
   * <ul>
   *   <li>Continue parallel descent if the node is a BranchNode with sufficient updates
   *   <li>Process sequentially if the node is a leaf/extension or updates are sparse
   * </ul>
   *
   * @param parentWrapper The wrapper for the parent BranchNode
   * @param nibbleIndex The child index (0-15) in the parent node
   * @param updatedNode Node to update
   * @param updates The list of updates to apply to this subtree
   * @param maybeCommitCache Optional cache for deferred node commits
   */
  private void applyUpdatesToBranchRecursively(
      final ThreadSafeBranchNodeWrapper parentWrapper,
      final byte nibbleIndex,
      final BranchNode<V> updatedNode,
      final List<UpdateEntry<V>> updates,
      final Optional<NodeCommitCache> maybeCommitCache) {

    final ThreadSafeBranchNodeWrapper currentNodeWrapper =
        new ThreadSafeBranchNodeWrapper(
            updatedNode, Bytes.concatenate(parentWrapper.getLocation(), Bytes.of(nibbleIndex)));

    // Group updates by next nibble (all updates continue deeper, none terminate here)
    final Map<Byte, List<UpdateEntry<V>>> updatesByNextNibble =
        groupUpdatesByNibbleAtIndex(updates, currentNodeWrapper.getDepth());

    final List<CompletableFuture<Void>> childUpdateFutures =
        createParallelBranchUpdateTasks(currentNodeWrapper, updatesByNextNibble, maybeCommitCache);

    if (!childUpdateFutures.isEmpty()) {
      CompletableFuture.allOf(childUpdateFutures.toArray(new CompletableFuture[0])).join();
    }

    parentWrapper.setChildren(nibbleIndex, currentNodeWrapper.applyUpdates());
  }

  /**
   * Applies updates to a node sequentially using standard trie operations.
   *
   * <p>Used when:
   *
   * <ul>
   *   <li>Node is LeafNode or ExtensionNode (not a BranchNode)
   *   <li>Update count is below parallel threshold
   * </ul>
   *
   * @param parentWrapper The wrapper containing this node
   * @param childNibble The index in parent (0-15)
   * @param updatedNode Node to update
   * @param updates Updates to apply
   * @param maybeCommitCache Optional cache for deferred commits
   */
  private void applyUpdatesToNodeSequentially(
      final ThreadSafeBranchNodeWrapper parentWrapper,
      final byte childNibble,
      final Node<V> updatedNode,
      final List<UpdateEntry<V>> updates,
      final Optional<NodeCommitCache> maybeCommitCache) {

    Node<V> tmpNode = updatedNode;

    for (final UpdateEntry<V> entry : updates) {
      // Slice path to remove already-processed nibbles
      final Bytes remainingPath = entry.path().slice(parentWrapper.getDepth() + 1);
      final PathNodeVisitor<V> visitor =
          entry.value().isPresent() ? getPutVisitor(entry.value().get()) : getRemoveVisitor();
      tmpNode = tmpNode.accept(visitor, remainingPath);
    }

    if (maybeCommitCache.isPresent()) {
      final Bytes updatedNodeLocation =
          Bytes.concatenate(parentWrapper.getLocation(), Bytes.of(childNibble));
      tmpNode.accept(
          updatedNodeLocation,
          new CommitVisitor<>(
              new NodeUpdater() {
                @Override
                public void store(final Bytes location, final Bytes32 hash, final Bytes value) {
                  maybeCommitCache.get().put(location, hash, value);
                }
              }));
    } else {
      // Ensures the node's hash is computed without persisting to storage.
      Objects.requireNonNull(tmpNode.getHash());
    }

    parentWrapper.setChildren(childNibble, tmpNode);
  }

  private void persistRootAndReset(final NodeUpdater nodeUpdater) {
    final Bytes32 rootHash = root.getHash();
    nodeUpdater.store(Bytes.EMPTY, rootHash, root.getEncodedBytes());
    this.root =
        rootHash.equals(EMPTY_TRIE_NODE_HASH)
            ? NullNode.instance()
            : new StoredNode<>(nodeFactory, Bytes.EMPTY, rootHash);
  }

  private Node<V> loadAndResolveRootNode() {
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

  class ThreadSafeBranchNodeWrapper {
    private final BranchNode<V> root;
    private final List<Node<V>> pendingChildren;
    private final Bytes location;

    public ThreadSafeBranchNodeWrapper(final BranchNode<V> root, final Bytes location) {
      this.root = root;
      this.pendingChildren = Collections.synchronizedList(new ArrayList<>(root.getChildren()));
      this.location = location;
    }

    public List<Node<V>> getPendingChildren() {
      return pendingChildren;
    }

    public Bytes getLocation() {
      return location;
    }

    public int getDepth() {
      return location.size();
    }

    public void setChildren(final byte index, final Node<V> children) {
      this.pendingChildren.set(index, children);
    }

    public Node<V> applyUpdates() {
      return this.root.replaceAllChildren(pendingChildren, true);
    }
  }

  private record UpdateEntry<V>(Bytes path, Optional<V> value) {
    byte getNibble(final int index) {
      if (index >= path.size()) {
        return 0;
      }
      return path.get(index);
    }
  }

  private static class NodeCommitCache {
    private final Map<Bytes, NodeData> cache = new ConcurrentHashMap<>();

    void put(final Bytes location, final Bytes32 hash, final Bytes encodedBytes) {
      cache.put(location, new NodeData(hash, encodedBytes));
    }

    void flushTo(final NodeUpdater nodeUpdater) {
      cache.forEach(
          (location, nodeData) ->
              nodeUpdater.store(location, nodeData.hash, nodeData.encodedBytes));
    }

    private static class NodeData {
      final Bytes32 hash;
      final Bytes encodedBytes;

      NodeData(final Bytes32 hash, final Bytes encodedBytes) {
        this.hash = hash;
        this.encodedBytes = encodedBytes;
      }
    }
  }
}
