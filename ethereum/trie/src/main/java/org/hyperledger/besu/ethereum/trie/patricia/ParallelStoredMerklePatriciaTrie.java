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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A recursive parallel implementation using virtual threads.
 * Recursively descends the trie and spawns virtual threads for each branch.
 *
 * @param <K> The type of keys
 * @param <V> The type of values stored by this trie
 */
public class ParallelStoredMerklePatriciaTrie<K extends Bytes, V>
        extends StoredMerklePatriciaTrie<K, V> {

    private static final ExecutorService VIRTUAL_POOL =
            Executors.newVirtualThreadPerTaskExecutor();

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

    /**
     * Entry point for recursive processing
     */
    private void processPendingUpdates(final Optional<NodeUpdater> maybeNodeUpdater) {
        if (pendingUpdates.isEmpty()) {
            return;
        }

        try {
            // Convert updates to paths
            Map<Bytes, Optional<V>> pathUpdates = pendingUpdates.entrySet().stream()
                    .collect(Collectors.toMap(
                            e -> bytesToPath(e.getKey()),
                            Map.Entry::getValue
                    ));

            // Start recursive processing from root
            this.root = processNodeRecursively(this.root, pathUpdates, 0, Bytes.EMPTY, maybeNodeUpdater);

            // Store root and reset if committing
            if (maybeNodeUpdater.isPresent()) {
                storeAndResetRoot(maybeNodeUpdater.get());
            }

        } finally {
            pendingUpdates.clear();
        }
    }

    /**
     * Recursively process a node and its children in parallel
     *
     * @param node Current node being processed
     * @param updates Map of path -> value updates
     * @param depth Current depth in the path
     * @param location Current location in the trie
     * @param maybeNodeUpdater Optional node updater for committing
     * @return Updated node
     */
    private Node<V> processNodeRecursively(
            final Node<V> node,
            final Map<Bytes, Optional<V>> updates,
            final int depth,
            final Bytes location,
            final Optional<NodeUpdater> maybeNodeUpdater) {

        // Base case: no updates for this node
        if (updates.isEmpty()) {
            return node;
        }

        // If it's a BranchNode, recurse in parallel on children
        if (node instanceof BranchNode) {
            return processBranchNodeRecursively(
                    (BranchNode<V>) node, updates, depth, location, maybeNodeUpdater);
        }

        // For other node types, apply updates sequentially
        Node<V> result = node;
        for (Map.Entry<Bytes, Optional<V>> update : updates.entrySet()) {
            Bytes remainingPath = update.getKey().slice(depth);
            PathNodeVisitor<V> visitor = update.getValue().isPresent()
                    ? getPutVisitor(update.getValue().get())
                    : getRemoveVisitor();
            result = result.accept(visitor, remainingPath);
        }

        // Commit this node if needed
        if (maybeNodeUpdater.isPresent()) {
            result.accept(location, new CommitVisitor<>(maybeNodeUpdater.get()));
        }

        return result;
    }

    /**
     * Process a BranchNode recursively in parallel
     */
    private Node<V> processBranchNodeRecursively(
            final BranchNode<V> branchNode,
            final Map<Bytes, Optional<V>> updates,
            final int depth,
            final Bytes location,
            final Optional<NodeUpdater> maybeNodeUpdater) {

        // Group updates by next nibble (0-15)
        Map<Byte, Map<Bytes, Optional<V>>> grouped = new HashMap<>();

        for (Map.Entry<Bytes, Optional<V>> update : updates.entrySet()) {
            Bytes path = update.getKey();
            if (path.size() > depth) {
                byte nibble = path.get(depth);
                grouped.computeIfAbsent(nibble, k -> new HashMap<>())
                        .put(path, update.getValue());
            }
        }

        // Process each child in parallel using virtual threads
        List<Node<V>> children = branchNode.getChildren();
        List<Node<V>> newChildren = new java.util.ArrayList<>(children);

        for (int i = 0; i < children.size(); i++) {
            final int index = i;
            final byte nibble = (byte) i;
            final Node<V> child = children.get(i);
            final Map<Bytes, Optional<V>> childUpdates = grouped.get(nibble);

            if (childUpdates != null && !childUpdates.isEmpty()) {
                final Bytes childLocation = Bytes.concatenate(location, Bytes.of(nibble));

                // Spawn a virtual thread for each child with updates
                try {
                    Node<V> updatedChild = VIRTUAL_POOL.submit(() ->
                            processNodeRecursively(child, childUpdates, depth + 1, childLocation, maybeNodeUpdater)
                    ).get();
                    newChildren.set(index, updatedChild);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to process child node", e);
                }
            }
        }

        // Return new branch with updated children
        Node<V> result = branchNode.replaceAllChildren(newChildren, true);

        // Commit this branch node if needed
        if (maybeNodeUpdater.isPresent()) {
            result.accept(location, new CommitVisitor<>(maybeNodeUpdater.get()));
        }

        return result;
    }

    private void storeAndResetRoot(final NodeUpdater nodeUpdater) {
        final Bytes32 rootHash = root.getHash();
        nodeUpdater.store(Bytes.EMPTY, rootHash, root.getEncodedBytes());
        this.root =
                rootHash.equals(EMPTY_TRIE_NODE_HASH)
                        ? NullNode.instance()
                        : new StoredNode<>(nodeFactory, Bytes.EMPTY, rootHash);
    }
}