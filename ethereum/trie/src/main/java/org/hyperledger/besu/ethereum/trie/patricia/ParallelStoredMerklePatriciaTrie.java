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

import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.PathNodeVisitor;
import org.hyperledger.besu.ethereum.trie.StoredNode;
import org.hyperledger.besu.ethereum.trie.CommitVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Optimized recursive parallel implementation with path-specific optimizations.
 * Uses virtual threads via CompletableFuture.
 *
 * Key optimizations:
 * - Pre-compute all paths once
 * - Use array indexing instead of slice operations
 * - Fast nibble extraction with direct byte array access
 * - Efficient path comparison for extensions
 * - Simple CompletableFuture + virtual threads
 *
 * @param <K> The type of keys
 * @param <V> The type of values stored by this trie
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class ParallelStoredMerklePatriciaTrie<K extends Bytes, V>
        extends StoredMerklePatriciaTrie<K, V> {

    private static final ExecutorService VIRTUAL_EXECUTOR =
            Executors.newVirtualThreadPerTaskExecutor();

    private static final int MIN_UPDATES_FOR_PARALLEL = 4;
    private static final int MIN_CHILDREN_FOR_PARALLEL = 2;

    private final Map<K, Optional<V>> pendingUpdates = new ConcurrentHashMap<>();

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

    /**
     * Optimized update entry with pre-computed path and fast access
     */
    private static class UpdateEntry<V> {
        final byte[] pathBytes; // Direct byte array access for speed
        final Optional<V> value;

        UpdateEntry(final Bytes path, final Optional<V> value) {
            this.pathBytes = path.toArrayUnsafe(); // Zero-copy access
            this.value = value;
        }

        // Fast nibble extraction using array access
        byte getNibble(final int index) {
            if (index >= pathBytes.length) {
                return 0;
            }
            return pathBytes[index];
        }

        // Fast path segment comparison for extensions
        boolean matchesSegment(final Bytes extensionPath, final int startIndex) {
            int extensionLength = extensionPath.size();
            if (startIndex + extensionLength > pathBytes.length) {
                return false;
            }

            byte[] extBytes = extensionPath.toArrayUnsafe();
            for (int i = 0; i < extensionLength; i++) {
                if (pathBytes[startIndex + i] != extBytes[i]) {
                    return false;
                }
            }
            return true;
        }

        // Fast remaining path extraction
        Bytes getRemainingPath(final int depth) {
            if (depth >= pathBytes.length) {
                return Bytes.EMPTY;
            }
            return Bytes.wrap(pathBytes, depth, pathBytes.length - depth);
        }
    }

    /**
         * Result holder for parallel child processing
         */
        private record ChildUpdate(int index, Node<?> node) {
    }

    private void processPendingUpdates(final Optional<NodeUpdater> maybeNodeUpdater) {
        if (pendingUpdates.isEmpty()) {
            return;
        }

        try {
            // Pre-convert all paths once and create optimized entries
            List<UpdateEntry<V>> updateEntries = new ArrayList<>(pendingUpdates.size());
            for (Map.Entry<K, Optional<V>> entry : pendingUpdates.entrySet()) {
                Bytes path = bytesToPath(entry.getKey());
                updateEntries.add(new UpdateEntry<>(path, entry.getValue()));
            }

            // Create commit cache if we're committing
            NodeCommitCache commitCache = maybeNodeUpdater.isPresent()
                    ? new NodeCommitCache()
                    : null;

            // Start recursive processing from root
            this.root = processNodeRecursively(
                    this.root,
                    updateEntries,
                    0,
                    Bytes.EMPTY,
                    commitCache);

            // Flush cache and store root if committing
            if (maybeNodeUpdater.isPresent()) {
                commitCache.flushTo(maybeNodeUpdater.get());
                storeAndResetRoot(maybeNodeUpdater.get());
            }

        } finally {
            pendingUpdates.clear();
        }
    }

    /**
     * Main recursive processing method with optimized UpdateEntry list
     */
    private Node<V> processNodeRecursively(
            final Node<V> node,
            final List<UpdateEntry<V>> updates,
            final int depth,
            final Bytes location,
            final NodeCommitCache commitCache) {

        if (updates.isEmpty()) {
            return node;
        }

        // Single update optimization
        if (updates.size() == 1) {
            return processSingleUpdate(node, updates.get(0), depth, location, commitCache);
        }

        // Handle different node types
        if (node instanceof BranchNode) {
            return processBranchNodeRecursively(
                    (BranchNode<V>) node, updates, depth, location, commitCache);
        }

        if (node instanceof ExtensionNode) {
            return processExtensionNodeRecursively(
                    (ExtensionNode<V>) node, updates, depth, location, commitCache);
        }

        // Leaf or other nodes: sequential processing
        return processSequentialUpdates(node, updates, depth, location, commitCache);
    }

    /**
     * Optimized single update processing
     */
    private Node<V> processSingleUpdate(
            final Node<V> node,
            final UpdateEntry<V> update,
            final int depth,
            final Bytes location,
            final NodeCommitCache commitCache) {

        Bytes remainingPath = update.getRemainingPath(depth);
        PathNodeVisitor<V> visitor = update.value.isPresent()
                ? getPutVisitor(update.value.get())
                : getRemoveVisitor();

        Node<V> result = node.accept(visitor, remainingPath);

        if (commitCache != null) {
            commitNode(result, location, commitCache);
        }

        return result;
    }

    /**
     * Sequential batch processing
     */
    private Node<V> processSequentialUpdates(
            final Node<V> node,
            final List<UpdateEntry<V>> updates,
            final int depth,
            final Bytes location,
            final NodeCommitCache commitCache) {

        Node<V> result = node;
        for (UpdateEntry<V> update : updates) {
            Bytes remainingPath = update.getRemainingPath(depth);
            PathNodeVisitor<V> visitor = update.value.isPresent()
                    ? getPutVisitor(update.value.get())
                    : getRemoveVisitor();
            result = result.accept(visitor, remainingPath);
        }

        if (commitCache != null) {
            commitNode(result, location, commitCache);
        }

        return result;
    }

    /**
     * Optimized ExtensionNode processing with fast path matching
     */
    private Node<V> processExtensionNodeRecursively(
            final ExtensionNode<V> extensionNode,
            final List<UpdateEntry<V>> updates,
            final int depth,
            final Bytes location,
            final NodeCommitCache commitCache) {

        Bytes extensionPath = extensionNode.getPath();
        int extensionPathLength = extensionPath.size();

        // Fast partition: matching vs diverging updates
        // Fast partition with fail-fast: stop as soon as we find a diverging update
        List<UpdateEntry<V>> matchingUpdates = new ArrayList<>();
        boolean hasDivergingUpdates = false;
        for (UpdateEntry<V> update : updates) {
            if (update.matchesSegment(extensionPath, depth)) {
                matchingUpdates.add(update);
            } else {
                // Fail-fast: as soon as we find one diverging update,
                // we know we need to fall back to sequential processing
                hasDivergingUpdates = true;
                break;
            }
        }

        // Case 1: All updates match the extension path
        if (!hasDivergingUpdates) {
            Node<V> child = extensionNode.getChild();
            Bytes childLocation = Bytes.concatenate(location, extensionPath);

            Node<V> updatedChild = processNodeRecursively(
                    child,
                    matchingUpdates,
                    depth + extensionPathLength,
                    childLocation,
                    commitCache);

            Node<V> result = extensionNode.replaceChild(updatedChild);

            if (commitCache != null) {
                commitNode(result, location, commitCache);
            }

            return result;
        }

        // Case 2: Some updates diverge - fall back to sequential
        return processSequentialUpdates(extensionNode, updates, depth, location, commitCache);
    }

    /**
     * Optimized BranchNode processing with fast nibble grouping
     */
    private Node<V> processBranchNodeRecursively(
            final BranchNode<V> branchNode,
            final List<UpdateEntry<V>> updates,
            final int depth,
            final Bytes location,
            final NodeCommitCache commitCache) {

        // Fast grouping by nibble using array of lists
        List<UpdateEntry<V>>[] grouped = new ArrayList[16];

        for (UpdateEntry<V> update : updates) {
            byte nibble = update.getNibble(depth);
            int index = nibble & 0x0F; // Fast modulo 16
            if (grouped[index] == null) {
                grouped[index] = new ArrayList<>();
            }
            grouped[index].add(update);
        }

        // Count non-empty groups
        int nonEmptyGroups = 0;
        for (List<UpdateEntry<V>> group : grouped) {
            if (group != null) {
                nonEmptyGroups++;
            }
        }

        if (nonEmptyGroups == 0) {
            return branchNode;
        }

        boolean shouldParallelize = updates.size() >= MIN_UPDATES_FOR_PARALLEL
                && nonEmptyGroups >= MIN_CHILDREN_FOR_PARALLEL;

        List<Node<V>> children = branchNode.getChildren();
        List<Node<V>> newChildren = new ArrayList<>(children);

        if (shouldParallelize) {
            processChildrenInParallel(
                    children, newChildren, grouped, depth, location, commitCache);
        } else {
            processChildrenSequentially(
                    children, newChildren, grouped, depth, location, commitCache);
        }

        Node<V> result = branchNode.replaceAllChildren(newChildren, true);

        if (commitCache != null) {
            commitNode(result, location, commitCache);
        }

        return result;
    }

    /**
     * Parallel processing using CompletableFuture with virtual threads
     */
    private void processChildrenInParallel(
            final List<Node<V>> children,
            final List<Node<V>> newChildren,
            final List<UpdateEntry<V>>[] grouped,
            final int depth,
            final Bytes location,
            final NodeCommitCache commitCache) {

        List<CompletableFuture<ChildUpdate>> futures = new ArrayList<>();

        // Submit tasks to virtual thread executor
        for (int i = 0; i < 16; i++) {
            if (grouped[i] != null && !grouped[i].isEmpty()) {
                final int index = i;
                final byte nibble = (byte) i;
                final Node<V> child = children.get(index);
                final List<UpdateEntry<V>> childUpdates = grouped[i];
                final Bytes childLocation = Bytes.concatenate(location, Bytes.of(nibble));

                CompletableFuture<ChildUpdate> future = CompletableFuture.supplyAsync(
                        () -> new ChildUpdate(
                                index,
                                processNodeRecursively(child, childUpdates, depth + 1, childLocation, commitCache)
                        ),
                        VIRTUAL_EXECUTOR
                );

                futures.add(future);
            }
        }

        // Wait for all to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Collect results
        for (CompletableFuture<ChildUpdate> future : futures) {
            try {
                ChildUpdate update = future.get();
                newChildren.set(update.index, (Node<V>) update.node);
            } catch (Exception e) {
                throw new RuntimeException("Failed to process child node", e);
            }
        }
    }

    /**
     * Sequential processing for small update sets
     */
    private void processChildrenSequentially(
            final List<Node<V>> children,
            final List<Node<V>> newChildren,
            final List<UpdateEntry<V>>[] grouped,
            final int depth,
            final Bytes location,
            final NodeCommitCache commitCache) {

        for (int i = 0; i < 16; i++) {
            if (grouped[i] != null && !grouped[i].isEmpty()) {
                byte nibble = (byte) i;
                Node<V> child = children.get(i);
                List<UpdateEntry<V>> childUpdates = grouped[i];
                Bytes childLocation = Bytes.concatenate(location, Bytes.of(nibble));

                Node<V> updatedChild = processNodeRecursively(
                        child, childUpdates, depth + 1, childLocation, commitCache);
                newChildren.set(i, updatedChild);
            }
        }
    }

    /**
     * Commit a node to cache
     */
    private void commitNode(final Node<V> node, final Bytes location, final NodeCommitCache commitCache) {
        node.accept(location, new CommitVisitor<>(new NodeUpdater() {
            @Override
            public void store(final Bytes loc, final Bytes32 hash, final Bytes value) {
                commitCache.put(loc, hash, value);
            }
        }));
    }

    private void storeAndResetRoot(final NodeUpdater nodeUpdater) {
        final Bytes32 rootHash = root.getHash();
        nodeUpdater.store(Bytes.EMPTY, rootHash, root.getEncodedBytes());
        this.root =
                rootHash.equals(EMPTY_TRIE_NODE_HASH)
                        ? NullNode.instance()
                        : new StoredNode<>(nodeFactory, Bytes.EMPTY, rootHash);
    }

    /**
     * Thread-safe cache for node commits.
     * Collects all commits from parallel threads and flushes them at once.
     */
    private static class NodeCommitCache {
        private final Map<Bytes, NodeData> cache = new ConcurrentHashMap<>();

        void put(final Bytes location, final Bytes32 hash, final Bytes encodedBytes) {
            cache.put(location, new NodeData(hash, encodedBytes));
        }

        void flushTo(final NodeUpdater nodeUpdater) {
            cache.forEach((location, nodeData) ->
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