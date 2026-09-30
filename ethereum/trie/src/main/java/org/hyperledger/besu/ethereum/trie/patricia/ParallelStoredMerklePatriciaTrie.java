/*
 * Copyright contributors to Besu.
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
import static org.hyperledger.besu.ethereum.trie.patricia.DefaultNodeFactory.NB_CHILD;

import org.hyperledger.besu.ethereum.trie.CommitVisitor;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.PathNodeVisitor;
import org.hyperledger.besu.ethereum.trie.StoredNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A parallel implementation of StoredMerklePatriciaTrie that processes updates in parallel.
 *
 * <p>This implementation batches updates and processes them concurrently when it's possible and
 * there are sufficient updates to warrant parallel processing. It runs in two phases:
 *
 * <ol>
 *   <li><b>Load</b>: the stored nodes on the update paths are resolved level by level. Each level
 *       is sorted by location (the storage key order of path-keyed layouts, so neighbouring reads
 *       share SST files and data blocks), cut into contiguous ranges, and each range is read with
 *       one {@link NodeLoader#getNodes} batch, ranges running in parallel. The number of I/O round
 *       trips is bounded by the trie depth instead of the number of nodes.
 *   <li><b>Update</b>: the trie is updated and hashed by recursively descending it, processing
 *       independent subtrees concurrently. Nodes are already in memory, so this is CPU work.
 * </ol>
 *
 * @param <K> the key type, must extend Bytes
 * @param <V> the value type
 */
@SuppressWarnings({"rawtypes", "ThreadPriorityCheck"})
public class ParallelStoredMerklePatriciaTrie<K extends Bytes, V>
    extends StoredMerklePatriciaTrie<K, V> {

  /**
   * Default pool for tests and callers that do not inject a dedicated pool.
   *
   * <p>Production code should pass separate pools for account and storage tries to avoid deadlock
   * when account workers join storage futures while storage hashing runs in parallel.
   */
  private static final ForkJoinPool DEFAULT_FORK_JOIN_POOL =
      new ForkJoinPool(Runtime.getRuntime().availableProcessors() * 2);

  /** Pending updates accumulated between commits. */
  private final Map<K, PendingUpdate<V>> pendingUpdates = new ConcurrentHashMap<>();

  private final ForkJoinPool forkJoinPool;

  /** Maximum number of nodes read in one batch while loading the update paths. */
  private static final int LOAD_RANGE_SIZE = 128;

  /**
   * Executor running the batched node reads of the load phase, or {@code null} to run them on
   * {@link #forkJoinPool}. Sizing it to the storage device rather than to the CPU lets the I/O
   * parallelism be tuned independently from the hashing parallelism.
   */
  private final Executor loadExecutor;

  /**
   * Stages a deferred operation for the given key. The function is called with the existing value
   * at that key during the next {@link #commit} or {@link #getRootHash} traversal.
   *
   * <p>Unlike {@link #put}, no pre-computed value is required: the trie reads the existing leaf
   * inline during the write traversal, enabling a single-pass read-modify-write.
   *
   * @param key the key to update
   * @param merger function mapping existing value (or empty) to the new value (or empty to remove)
   */
  @Override
  public void putDeferred(final K key, final Function<Optional<V>, Optional<V>> merger) {
    pendingUpdates.put(key, new Merge<>(merger));
  }

  /**
   * Creates a new parallel trie with an empty root.
   *
   * @param nodeLoader the node loader for retrieving stored nodes
   * @param valueSerializer function to serialize values to bytes
   * @param valueDeserializer function to deserialize bytes to values
   */
  public ParallelStoredMerklePatriciaTrie(
      final NodeLoader nodeLoader,
      final Function<V, Bytes> valueSerializer,
      final Function<Bytes, V> valueDeserializer) {
    this(nodeLoader, valueSerializer, valueDeserializer, DEFAULT_FORK_JOIN_POOL);
  }

  public ParallelStoredMerklePatriciaTrie(
      final NodeLoader nodeLoader,
      final Function<V, Bytes> valueSerializer,
      final Function<Bytes, V> valueDeserializer,
      final ForkJoinPool forkJoinPool) {
    super(nodeLoader, valueSerializer, valueDeserializer);
    this.forkJoinPool = forkJoinPool;
    this.loadExecutor = null;
  }

  /**
   * Creates a new parallel trie with a specific root hash and location.
   *
   * @param nodeLoader the node loader for retrieving stored nodes
   * @param rootHash the hash of the root node
   * @param rootLocation the storage location of the root node
   * @param valueSerializer function to serialize values to bytes
   * @param valueDeserializer function to deserialize bytes to values
   */
  public ParallelStoredMerklePatriciaTrie(
      final NodeLoader nodeLoader,
      final Bytes32 rootHash,
      final Bytes rootLocation,
      final Function<V, Bytes> valueSerializer,
      final Function<Bytes, V> valueDeserializer) {
    this(
        nodeLoader,
        rootHash,
        rootLocation,
        valueSerializer,
        valueDeserializer,
        DEFAULT_FORK_JOIN_POOL);
  }

  public ParallelStoredMerklePatriciaTrie(
      final NodeLoader nodeLoader,
      final Bytes32 rootHash,
      final Bytes rootLocation,
      final Function<V, Bytes> valueSerializer,
      final Function<Bytes, V> valueDeserializer,
      final ForkJoinPool forkJoinPool) {
    super(nodeLoader, rootHash, rootLocation, valueSerializer, valueDeserializer);
    this.forkJoinPool = forkJoinPool;
    this.loadExecutor = null;
  }

  /**
   * Creates a new parallel trie with a specific root hash.
   *
   * @param nodeLoader the node loader for retrieving stored nodes
   * @param rootHash the hash of the root node
   * @param valueSerializer function to serialize values to bytes
   * @param valueDeserializer function to deserialize bytes to values
   */
  public ParallelStoredMerklePatriciaTrie(
      final NodeLoader nodeLoader,
      final Bytes32 rootHash,
      final Function<V, Bytes> valueSerializer,
      final Function<Bytes, V> valueDeserializer) {
    this(nodeLoader, rootHash, valueSerializer, valueDeserializer, DEFAULT_FORK_JOIN_POOL);
  }

  public ParallelStoredMerklePatriciaTrie(
      final NodeLoader nodeLoader,
      final Bytes32 rootHash,
      final Function<V, Bytes> valueSerializer,
      final Function<Bytes, V> valueDeserializer,
      final ForkJoinPool forkJoinPool) {
    super(nodeLoader, rootHash, valueSerializer, valueDeserializer);
    this.forkJoinPool = forkJoinPool;
    this.loadExecutor = null;
  }

  /**
   * Creates a new parallel trie whose load phase runs its batched node reads on {@code
   * loadExecutor}.
   *
   * @param nodeLoader the node loader for retrieving stored nodes
   * @param rootHash the hash of the root node
   * @param valueSerializer function to serialize values to bytes
   * @param valueDeserializer function to deserialize bytes to values
   * @param forkJoinPool pool running the parallel update and hashing
   * @param loadExecutor executor running the batched node reads
   */
  public ParallelStoredMerklePatriciaTrie(
      final NodeLoader nodeLoader,
      final Bytes32 rootHash,
      final Function<V, Bytes> valueSerializer,
      final Function<Bytes, V> valueDeserializer,
      final ForkJoinPool forkJoinPool,
      final Executor loadExecutor) {
    super(nodeLoader, rootHash, valueSerializer, valueDeserializer);
    this.forkJoinPool = forkJoinPool;
    this.loadExecutor = loadExecutor;
  }

  /**
   * Creates a new parallel trie with a node factory and root hash.
   *
   * @param nodeFactory the factory for creating nodes
   * @param rootHash the hash of the root node
   */
  public ParallelStoredMerklePatriciaTrie(
      final StoredNodeFactory<V> nodeFactory, final Bytes32 rootHash) {
    this(nodeFactory, rootHash, DEFAULT_FORK_JOIN_POOL);
  }

  public ParallelStoredMerklePatriciaTrie(
      final StoredNodeFactory<V> nodeFactory,
      final Bytes32 rootHash,
      final ForkJoinPool forkJoinPool) {
    super(nodeFactory, rootHash);
    this.forkJoinPool = forkJoinPool;
    this.loadExecutor = null;
  }

  /**
   * Stages a put operation for the given key-value pair. The update is not applied until commit()
   * or getRootHash() is called.
   *
   * @param key the key to insert
   * @param value the value to associate with the key
   */
  @Override
  public void put(final K key, final V value) {
    pendingUpdates.put(key, new Direct<>(Optional.of(value)));
  }

  /**
   * Stages a remove operation for the given key. The update is not applied until commit() or
   * getRootHash() is called.
   *
   * @param key the key to remove
   */
  @Override
  public void remove(final K key) {
    pendingUpdates.put(key, new Direct<>(Optional.empty()));
  }

  /**
   * Commits all pending updates to storage using the provided node updater. Applies updates in
   * parallel when beneficial, then persists nodes to storage.
   *
   * @param nodeUpdater the updater to persist nodes to storage
   */
  @Override
  public void commit(final NodeUpdater nodeUpdater) {
    processPendingUpdates(Optional.of(nodeUpdater));
  }

  /**
   * Computes and returns the root hash after applying all pending updates. This triggers update
   * processing but does not persist nodes to storage.
   *
   * @return the Merkle root hash of the trie
   */
  @Override
  public Bytes32 getRootHash() {
    if (pendingUpdates.isEmpty()) {
      return root.getHash();
    }
    processPendingUpdates(Optional.empty());
    return root.getHash();
  }

  /**
   * Processes all pending updates, applying them to the trie structure. Chooses between parallel
   * and sequential processing based on the root node type.
   *
   * @param maybeNodeUpdater optional node updater for persisting changes
   */
  private void processPendingUpdates(final Optional<NodeUpdater> maybeNodeUpdater) {
    if (pendingUpdates.isEmpty()) {
      return;
    }

    try {
      // Ensure root is fully loaded (not a lazy StoredNode reference)
      this.root = loadNode(root);

      final List<UpdateEntry<V>> entries = new ArrayList<>();
      pendingUpdates.forEach((k, update) -> entries.add(update.toEntry(bytesToPath(k))));

      // Load phase: resolve every stored node on the update paths with batched reads, so the
      // parallel update below only does CPU work.
      loadUpdatePaths(entries);

      final CommitCache commitCache = new CommitCache();
      final boolean shouldCommit = maybeNodeUpdater.isPresent();

      this.root =
          forkJoinPool.invoke(
              ForkJoinTask.adapt(
                  () ->
                      processNode(
                          root,
                          Bytes.EMPTY,
                          0,
                          entries,
                          shouldCommit ? Optional.of(commitCache) : Optional.empty())));

      // Persist all nodes to storage if committing
      if (maybeNodeUpdater.isPresent()) {
        commitCache.flushTo(maybeNodeUpdater.get());
        storeAndResetRoot(maybeNodeUpdater.get());
      }

    } finally {
      pendingUpdates.clear();
    }
  }

  /**
   * Resolves, level by level and with batched reads, every stored node the update will traverse.
   * Best-effort: a node that cannot be loaded here is loaded on demand by the update.
   *
   * @param entries the updates to apply
   */
  private void loadUpdatePaths(final List<UpdateEntry<V>> entries) {
    if (!(nodeFactory instanceof StoredNodeFactory<V> storedNodeFactory)) {
      return;
    }
    List<PathLoad<V>> frontier = new ArrayList<>();
    collectPathLoads(root, Bytes.EMPTY, entries, frontier);
    while (!frontier.isEmpty()) {
      frontier.sort(
          (a, b) ->
              Arrays.compareUnsigned(a.location().toArrayUnsafe(), b.location().toArrayUnsafe()));
      final List<Runnable> ranges = new ArrayList<>();
      for (int from = 0; from < frontier.size(); from += LOAD_RANGE_SIZE) {
        final List<PathLoad<V>> range =
            frontier.subList(from, Math.min(from + LOAD_RANGE_SIZE, frontier.size()));
        ranges.add(() -> loadRange(storedNodeFactory, range));
      }
      runAll(ranges);

      final List<PathLoad<V>> next = new ArrayList<>();
      for (final PathLoad<V> load : frontier) {
        if (load.node().isLoaded() && !load.updates().isEmpty()) {
          collectPathLoads(loadNode(load.node()), load.location(), load.updates(), next);
        }
      }
      frontier = next;
    }
  }

  private void loadRange(
      final StoredNodeFactory<V> storedNodeFactory, final List<PathLoad<V>> range) {
    final List<Bytes> locations = new ArrayList<>(range.size());
    final List<Bytes32> hashes = new ArrayList<>(range.size());
    for (final PathLoad<V> load : range) {
      locations.add(load.location());
      hashes.add(load.node().getHash());
    }
    final List<Optional<Node<V>>> nodes = storedNodeFactory.retrieveAll(locations, hashes);
    for (int i = 0; i < range.size(); i++) {
      final PathLoad<V> load = range.get(i);
      nodes.get(i).ifPresent(node -> load.node().preload(node));
    }
  }

  private void runAll(final List<Runnable> tasks) {
    if (tasks.size() == 1) {
      tasks.getFirst().run();
    } else if (loadExecutor == null) {
      forkJoinPool.invoke(
          ForkJoinTask.adapt(
              () -> ForkJoinTask.invokeAll(tasks.stream().map(ForkJoinTask::adapt).toList())));
    } else {
      CompletableFuture.allOf(
              tasks.stream()
                  .map(task -> CompletableFuture.runAsync(task, loadExecutor))
                  .toArray(CompletableFuture[]::new))
          .join();
    }
  }

  /**
   * Collects the unresolved stored nodes lying on the update paths below {@code node}. Resolved and
   * inline nodes are walked through without I/O.
   */
  private void collectPathLoads(
      final Node<V> node,
      final Bytes location,
      final List<UpdateEntry<V>> updates,
      final List<PathLoad<V>> out) {
    final int depth = location.size();
    switch (node) {
      case BranchNode<V> branch -> {
        final List<List<UpdateEntry<V>>> byNibble =
            new ArrayList<>(Collections.nCopies(NB_CHILD, null));
        boolean onlyRemovals = true;
        for (final UpdateEntry<V> update : updates) {
          if (update.path().size() <= depth) {
            continue;
          }
          final byte nibble = update.path().get(depth);
          if (nibble == CompactEncoding.LEAF_TERMINATOR) {
            continue;
          }
          if (byNibble.get(nibble) == null) {
            byNibble.set(nibble, new ArrayList<>());
          }
          byNibble.get(nibble).add(update);
          onlyRemovals &= update.isRemoval();
        }
        int sibling = -1;
        int siblings = 0;
        for (int nibble = 0; nibble < NB_CHILD; nibble++) {
          final Node<V> child = branch.child((byte) nibble);
          if (byNibble.get(nibble) != null) {
            collectChild(
                child, Bytes.concatenate(location, Bytes.of(nibble)), byNibble.get(nibble), out);
          } else if (!(child instanceof NullNode)) {
            siblings++;
            sibling = nibble;
          }
        }
        // Removals only: the branch may be left with its single untouched child and collapse,
        // which requires loading that child.
        if (onlyRemovals && siblings == 1 && branch.getValue().isEmpty()) {
          collectChild(
              branch.child((byte) sibling),
              Bytes.concatenate(location, Bytes.of(sibling)),
              List.of(),
              out);
        }
      }
      case ExtensionNode<V> extension -> {
        final Bytes extensionPath = extension.getPath();
        final List<UpdateEntry<V>> through = new ArrayList<>(updates.size());
        for (final UpdateEntry<V> update : updates) {
          if (update.path().size() >= depth + extensionPath.size()
              && update.path().slice(depth, extensionPath.size()).equals(extensionPath)) {
            through.add(update);
          }
        }
        if (!through.isEmpty()) {
          collectChild(
              extension.getChild(), Bytes.concatenate(location, extensionPath), through, out);
        }
      }
      default -> {
        // Leaf and null nodes end the path.
      }
    }
  }

  private void collectChild(
      final Node<V> child,
      final Bytes childLocation,
      final List<UpdateEntry<V>> updates,
      final List<PathLoad<V>> out) {
    if (child instanceof StoredNode<V> stored) {
      if (!stored.isLoaded()) {
        out.add(new PathLoad<>(stored, childLocation, updates));
      } else if (!updates.isEmpty()) {
        collectPathLoads(loadNode(stored), childLocation, updates, out);
      }
    } else if (!(child instanceof NullNode) && !updates.isEmpty()) {
      collectPathLoads(child, childLocation, updates, out);
    }
  }

  /**
   * A stored node to resolve during the load phase.
   *
   * @param node the unresolved reference
   * @param location the node location
   * @param updates the updates whose paths go through the node; empty for a node loaded only
   *     because its parent may collapse
   */
  private record PathLoad<V>(StoredNode<V> node, Bytes location, List<UpdateEntry<V>> updates) {}

  /**
   * Processes a node with a list of updates. This is the unified entry point for both root-level
   * and recursive processing. Dispatches based on node type.
   *
   * @param node the node to update
   * @param location the location of the node
   * @param depth the current depth in the trie
   * @param updates the updates to apply
   * @param maybeCommitCache optional commit cache for storing nodes
   * @return the updated node
   */
  private Node<V> processNode(
      final Node<V> node,
      final Bytes location,
      final int depth,
      final List<UpdateEntry<V>> updates,
      final Optional<CommitCache> maybeCommitCache) {

    // Load the node if it's a lazy reference
    final Node<V> loadedNode = loadNode(node);
    // Dispatch based on node type
    return switch (loadedNode) {
      case BranchNode<V> branch ->
          handleBranchNode(branch, location, depth, updates, maybeCommitCache);
      case ExtensionNode<V> ext -> handleExtension(ext, location, depth, updates, maybeCommitCache);
      case LeafNode<V> leaf -> handleLeafNode(leaf, location, depth, updates, maybeCommitCache);
      case NullNode<V> ignored -> handleNullNode(location, depth, updates, maybeCommitCache);
      case null, default ->
          // Unknown node type: fallback to sequential processing
          applyUpdatesSequentially(loadedNode, location, updates, maybeCommitCache);
    };
  }

  /**
   * Handles updates for a branch node by recursively processing its children. This is the key
   * recursion point for parallel processing.
   *
   * @param branchNode the branch node to update
   * @param location the location of the branch node
   * @param depth the current depth in the trie
   * @param updates the updates to apply
   * @param maybeCommitCache optional commit cache for storing nodes
   * @return the updated branch node
   */
  private Node<V> handleBranchNode(
      final BranchNode<V> branchNode,
      final Bytes location,
      final int depth,
      final List<UpdateEntry<V>> updates,
      final Optional<CommitCache> maybeCommitCache) {

    final int pathDepth = location.size();

    // Group updates by next nibble to distribute across branch children
    final Map<Byte, List<UpdateEntry<V>>> groupedUpdates = groupUpdatesByNibble(updates, pathDepth);

    // Wrap the branch to allow concurrent child updates
    final BranchWrapper branchWrapper = new BranchWrapper(branchNode);

    // Partition groups into large (parallel) and small (sequential)
    final Map<Boolean, Map<Byte, List<UpdateEntry<V>>>> partitionedGroups =
        groupedUpdates.entrySet().stream()
            .peek(e -> branchWrapper.loadChild(e.getKey())) // force load lazy nodes
            .collect(
                Collectors.partitioningBy(
                    entry -> entry.getValue().size() > 1 && groupedUpdates.size() > 1,
                    Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    final Map<Byte, List<UpdateEntry<V>>> largeGroups = partitionedGroups.get(true);
    final Map<Byte, List<UpdateEntry<V>>> smallGroups = partitionedGroups.get(false);

    final List<ForkJoinTask<Void>> forkJoinTasks = new ArrayList<>();

    // Submit large groups to thread pool
    if (!largeGroups.isEmpty()) {
      for (final Map.Entry<Byte, List<UpdateEntry<V>>> entry : largeGroups.entrySet()) {
        final byte nibble = entry.getKey();
        final List<UpdateEntry<V>> childUpdates = entry.getValue();
        final Bytes childLocation = Bytes.concatenate(location, Bytes.of(nibble));

        ForkJoinTask<Void> task =
            ForkJoinTask.adapt(
                () -> {
                  final Node<V> currentChild = branchWrapper.getPendingChildren().get(nibble);
                  final Node<V> updatedChild =
                      processNode(
                          currentChild, childLocation, depth, childUpdates, maybeCommitCache);
                  branchWrapper.setChild(nibble, updatedChild);
                  return null;
                });

        task.fork();
        forkJoinTasks.add(task);
      }
    }

    // Process small groups sequentially in current thread
    for (final Map.Entry<Byte, List<UpdateEntry<V>>> entry : smallGroups.entrySet()) {
      final byte nibble = entry.getKey();
      final List<UpdateEntry<V>> childUpdates = entry.getValue();
      final Bytes childLocation = Bytes.concatenate(location, Bytes.of(nibble));

      final Node<V> currentChild = branchWrapper.getPendingChildren().get(nibble);
      final Node<V> updatedChild =
          processNode(currentChild, childLocation, depth, childUpdates, maybeCommitCache);
      branchWrapper.setChild(nibble, updatedChild);
    }

    // Join all tasks
    forkJoinTasks.forEach(ForkJoinTask::join);

    // Apply all child updates
    final Node<V> newBranch = branchWrapper.applyUpdates();
    commitOrHashNode(newBranch, location, maybeCommitCache);

    return newBranch;
  }

  /**
   * Handles updates for an extension node. Attempts to parallelize by temporarily expanding the
   * extension into branches, processing updates, then reconstructing if beneficial.
   *
   * @param extensionNode the extension node to update
   * @param location the location of the extension node
   * @param depth the current depth in the trie
   * @param updates the updates to apply
   * @param maybeCommitCache optional commit cache for storing nodes
   * @return the updated extension or restructured node
   */
  private Node<V> handleExtension(
      final ExtensionNode<V> extensionNode,
      final Bytes location,
      final int depth,
      final List<UpdateEntry<V>> updates,
      final Optional<CommitCache> maybeCommitCache) {

    final Bytes extensionPath = extensionNode.getPath();
    final int pathDepth = location.size();

    // Find where updates diverge
    int divergenceIndex = findDivergencePoint(updates, pathDepth, extensionPath);

    // No divergence: all updates continue past extension
    if (divergenceIndex == extensionPath.size()) {
      final Bytes newLocation = Bytes.concatenate(location, extensionPath);
      final Node<V> newChild =
          processNode(
              extensionNode.getChild(),
              newLocation,
              depth + extensionPath.size(),
              updates,
              maybeCommitCache);

      final Node<V> newExtension = extensionNode.replaceChild(newChild);
      commitOrHashNode(newExtension, location, maybeCommitCache);
      return newExtension;
    }

    // Divergence within extension: only expand if we have multiple updates
    if (updates.size() > 1) {
      return expandExtensionToDivergencePoint(
          extensionNode,
          extensionPath,
          location,
          depth,
          updates,
          maybeCommitCache,
          divergenceIndex);
    }

    // Single update: let visitor handle restructuring (avoids unnecessary expansion)
    return applyUpdatesSequentially(extensionNode, location, updates, maybeCommitCache);
  }

  /**
   * Expands extension into branches up to divergence point. Key: continuation keeps remaining
   * extension, but updates are filtered so continuation won't be re-expanded unnecessarily.
   *
   * @param extensionNode the extension to expand
   * @param extensionPath the path of the extension
   * @param location the current location
   * @param depth the current depth
   * @param updates the updates to apply
   * @param maybeCommitCache optional commit cache
   * @param divergenceIndex the index where updates diverge (already computed)
   * @return the processed and optimized node structure
   */
  private Node<V> expandExtensionToDivergencePoint(
      final ExtensionNode<V> extensionNode,
      final Bytes extensionPath,
      final Bytes location,
      final int depth,
      final List<UpdateEntry<V>> updates,
      final Optional<CommitCache> maybeCommitCache,
      final int divergenceIndex) {

    final Bytes commonPrefix = extensionPath.slice(0, divergenceIndex);
    final byte divergingNibble = extensionPath.get(divergenceIndex);
    final Bytes remainingSuffix = extensionPath.slice(divergenceIndex + 1);

    Node<V> originalChild = loadNode(extensionNode.getChild());

    // Create continuation for remaining suffix
    Node<V> continuation =
        remainingSuffix.isEmpty()
            ? originalChild
            : nodeFactory.createExtension(remainingSuffix, originalChild);

    // Create branch at divergence point
    final List<Node<V>> branchChildren =
        new ArrayList<>(Collections.nCopies(NB_CHILD, NullNode.instance()));
    branchChildren.set(divergingNibble, continuation);
    Node<V> currentNode = nodeFactory.createBranch(branchChildren, Optional.empty());

    // Build branches for common prefix
    for (int i = commonPrefix.size() - 1; i >= 0; i--) {
      final byte nibble = commonPrefix.get(i);
      final List<Node<V>> children =
          new ArrayList<>(Collections.nCopies(NB_CHILD, NullNode.instance()));
      children.set(nibble, currentNode);
      currentNode = nodeFactory.createBranch(children, Optional.empty());
    }

    return processNode(currentNode, location, depth, updates, maybeCommitCache);
  }

  private int findDivergencePoint(
      final List<UpdateEntry<V>> updates, final int baseDepth, final Bytes extensionPath) {

    for (int i = 0; i < extensionPath.size(); i++) {
      final int absolutePosition = baseDepth + i;
      final byte extensionNibble = extensionPath.get(i);

      for (UpdateEntry<V> update : updates) {
        if (update.path.size() <= absolutePosition) {
          return i;
        }
        if (update.getNibble(absolutePosition) != extensionNibble) {
          return i;
        }
      }
    }

    return extensionPath.size();
  }

  /**
   * Handles updates for a leaf node. If there are enough updates that diverge, builds a branch
   * structure incorporating the existing leaf, then processes in parallel.
   *
   * @param leaf the leaf node to update
   * @param location the location of the node
   * @param depth the current depth in the trie
   * @param updates the updates to apply
   * @param maybeCommitCache optional commit cache for storing nodes
   * @return the updated node (may be a branch if expanded)
   */
  private Node<V> handleLeafNode(
      final LeafNode<V> leaf,
      final Bytes location,
      final int depth,
      final List<UpdateEntry<V>> updates,
      final Optional<CommitCache> maybeCommitCache) {
    // Check if parallel processing would be beneficial
    if (updates.size() > 1) {
      // Build a branch incorporating the leaf, then process updates
      final BranchNode<V> branch = buildBranchFromLeaf(leaf);
      return handleBranchNode(branch, location, depth, updates, maybeCommitCache);
    }

    // Sequential processing for small update sets or non-diverging updates
    return applyUpdatesSequentially(leaf, location, updates, maybeCommitCache);
  }

  /**
   * Handles updates for a null node (empty position). If there are enough updates that diverge,
   * builds a branch structure directly, then processes in parallel.
   *
   * @param location the location of the node
   * @param depth the current depth in the trie
   * @param updates the updates to apply
   * @param maybeCommitCache optional commit cache for storing nodes
   * @return the updated node (may be a branch if expanded)
   */
  private Node<V> handleNullNode(
      final Bytes location,
      final int depth,
      final List<UpdateEntry<V>> updates,
      final Optional<CommitCache> maybeCommitCache) {

    // Check if parallel processing would be beneficial
    if (updates.size() > 1) {
      // Build an empty branch, then process updates
      final BranchNode<V> branch = buildEmptyBranch();
      return handleBranchNode(branch, location, depth, updates, maybeCommitCache);
    }

    // Sequential processing for small update sets or non-diverging updates
    return applyUpdatesSequentially(NullNode.instance(), location, updates, maybeCommitCache);
  }

  /**
   * Builds a branch structure from a leaf node, incorporating it into the appropriate child.
   *
   * @param leaf the existing leaf node to incorporate
   * @return a new branch node with the leaf incorporated
   */
  private BranchNode<V> buildBranchFromLeaf(final LeafNode<V> leaf) {
    final List<Node<V>> children = new ArrayList<>(Collections.nCopies(16, NullNode.instance()));
    Optional<V> branchValue = Optional.empty();

    final Bytes leafPath = leaf.getPath();

    if (leafPath.get(0) == CompactEncoding.LEAF_TERMINATOR) {
      // Leaf represents a value at this exact location
      branchValue = leaf.getValue();
    } else {
      // Leaf continues deeper: place it in appropriate child
      final byte leafNibble = leafPath.get(0);
      final Bytes remainingPath = leafPath.slice(1);

      // Create a new leaf with the remaining path
      children.set(
          leafNibble, nodeFactory.createLeaf(remainingPath, leaf.getValue().orElseThrow()));
    }

    return (BranchNode<V>) nodeFactory.createBranch(children, branchValue);
  }

  /**
   * Builds an empty branch structure (16 null children, no value).
   *
   * @return a new empty branch node
   */
  private BranchNode<V> buildEmptyBranch() {
    final List<Node<V>> children = new ArrayList<>(Collections.nCopies(16, NullNode.instance()));
    return (BranchNode<V>) nodeFactory.createBranch(children, Optional.empty());
  }

  /**
   * Groups updates by their nibble at the specified depth.
   *
   * @param updates the updates to group
   * @param depth the depth at which to extract the nibble
   * @return map of nibble -> list of updates
   */
  private Map<Byte, List<UpdateEntry<V>>> groupUpdatesByNibble(
      final List<UpdateEntry<V>> updates, final int depth) {
    return updates.stream().collect(Collectors.groupingBy(entry -> entry.getNibble(depth)));
  }

  /**
   * Commits or hashes a node depending on whether we're committing.
   *
   * @param node the node to commit or hash
   * @param location the storage location of the node
   * @param maybeCommitCache optional commit cache for storing nodes
   */
  private void commitOrHashNode(
      final Node<V> node, final Bytes location, final Optional<CommitCache> maybeCommitCache) {
    if (maybeCommitCache.isPresent()) {
      node.accept(
          location,
          new CommitVisitor<>(
              (loc, hash, value) -> maybeCommitCache.get().store(loc, hash, value)));
    } else {
      Objects.requireNonNull(node.getHash());
    }
  }

  /**
   * Applies updates sequentially using the visitor pattern.
   *
   * @param node the starting node
   * @param location the location of the node
   * @param updates the updates to apply
   * @param maybeCommitCache optional commit cache for storing nodes
   * @return the updated node
   */
  private Node<V> applyUpdatesSequentially(
      final Node<V> node,
      final Bytes location,
      final List<UpdateEntry<V>> updates,
      final Optional<CommitCache> maybeCommitCache) {

    final int pathOffset = location.size();
    Node<V> updatedNode = node;

    for (UpdateEntry<V> entry : updates) {
      final Bytes remainingPath = entry.path.slice(pathOffset);
      final PathNodeVisitor<V> visitor =
          entry.isMerge()
              ? getDeferredPutVisitor(entry.merger())
              : (entry.value.isPresent() ? getPutVisitor(entry.value.get()) : getRemoveVisitor());
      updatedNode = updatedNode.accept(visitor, remainingPath);
    }

    commitOrHashNode(updatedNode, location, maybeCommitCache);
    return updatedNode;
  }

  /**
   * Stores the root node and resets it to a lazy-loaded reference.
   *
   * @param nodeUpdater the updater to persist the root node
   */
  private void storeAndResetRoot(final NodeUpdater nodeUpdater) {
    final Bytes32 rootHash = root.getHash();
    nodeUpdater.store(Bytes.EMPTY, rootHash, root.getEncodedBytes());

    this.root =
        rootHash.equals(EMPTY_TRIE_NODE_HASH)
            ? NullNode.instance()
            : new StoredNode<>(nodeFactory, Bytes.EMPTY, rootHash);
  }

  /**
   * Loads a node if it's a lazy reference (StoredNode), otherwise returns it as-is.
   *
   * @param node the node to load
   * @return the loaded node
   */
  protected Node<V> loadNode(final Node<V> node) {
    if (node instanceof StoredNode) {
      return node.accept(
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
    }
    return node;
  }

  /**
   * A staged update: either a direct put/remove ({@link Direct}) or a read-modify-write deferred
   * operation ({@link Merge}).
   */
  private sealed interface PendingUpdate<V> permits Direct, Merge {

    UpdateEntry<V> toEntry(Bytes path);
  }

  /** A staged put (present value) or remove (empty value). */
  private record Direct<V>(Optional<V> value) implements PendingUpdate<V> {
    @Override
    public UpdateEntry<V> toEntry(final Bytes path) {
      return new UpdateEntry<>(path, value, null);
    }
  }

  /**
   * A staged deferred operation: {@code merger} is called with the existing leaf value during the
   * commit traversal.
   */
  private record Merge<V>(Function<Optional<V>, Optional<V>> merger) implements PendingUpdate<V> {
    @Override
    public UpdateEntry<V> toEntry(final Bytes path) {
      return new UpdateEntry<>(path, Optional.empty(), merger);
    }
  }

  /**
   * Represents a single update operation: a plain put/remove ({@code merger == null}) or a
   * read-modify-write deferred operation ({@code merger != null}).
   *
   * @param path the full nibble path to the key
   * @param value optional value for put/remove entries (ignored for deferred entries)
   * @param merger non-null for deferred entries; called with the existing leaf value during
   *     traversal
   */
  private record UpdateEntry<V>(
      Bytes path, Optional<V> value, Function<Optional<V>, Optional<V>> merger) {
    boolean isMerge() {
      return merger != null;
    }

    boolean isRemoval() {
      return merger == null && value.isEmpty();
    }

    byte getNibble(final int index) {
      return index >= path.size() ? 0 : path.get(index);
    }
  }

  private class BranchWrapper {
    private final BranchNode<V> originalBranch;
    private final List<Node<V>> pendingChildren;

    BranchWrapper(final BranchNode<V> branch) {
      this.originalBranch = branch;
      this.pendingChildren = Collections.synchronizedList(new ArrayList<>(branch.getChildren()));
    }

    void loadChild(final byte index) {
      pendingChildren.set(index, loadNode(pendingChildren.get(index)));
    }

    List<Node<V>> getPendingChildren() {
      return pendingChildren;
    }

    void setChild(final byte index, final Node<V> child) {
      pendingChildren.set(index, child);
    }

    Node<V> applyUpdates() {
      return originalBranch.replaceAllChildren(pendingChildren, true);
    }
  }

  private static class CommitCache {
    private final Map<Bytes, NodeData> cache = new ConcurrentHashMap<>();

    void store(final Bytes location, final Bytes32 hash, final Bytes encodedBytes) {
      cache.put(location, new NodeData(hash, encodedBytes));
    }

    void flushTo(final NodeUpdater nodeUpdater) {
      cache.forEach(
          (location, nodeData) ->
              nodeUpdater.store(location, nodeData.hash, nodeData.encodedBytes));
    }

    private record NodeData(Bytes32 hash, Bytes encodedBytes) {}
  }
}
