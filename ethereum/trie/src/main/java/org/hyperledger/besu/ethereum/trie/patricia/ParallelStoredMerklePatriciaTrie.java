package org.hyperledger.besu.ethereum.trie.patricia;

import static org.hyperledger.besu.ethereum.trie.CompactEncoding.bytesToPath;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.ethereum.trie.CommitVisitor;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Parallel implementation using virtual threads for efficient trie updates.
 * Descends through ExtensionNodes to find BranchNodes for parallel processing.
 */
public class ParallelStoredMerklePatriciaTrie<K extends Bytes, V>
        extends StoredMerklePatriciaTrie<K, V> {

    private static final ExecutorService VIRTUAL_POOL = Executors.newVirtualThreadPerTaskExecutor();
    private static final int MIN_UPDATES_FOR_PARALLEL = 5;

    private final Map<K, Optional<V>> pendingUpdates = new HashMap<>();

    // Constructeurs
    public ParallelStoredMerklePatriciaTrie(
            final NodeLoader nodeLoader,
            final Function<V, Bytes> valueSerializer,
            final Function<Bytes, V> valueDeserializer) {
        super(nodeLoader, valueSerializer, valueDeserializer);
    }

    public ParallelStoredMerklePatriciaTrie(
            final NodeLoader nodeLoader, final Bytes32 rootHash,
            final Function<V, Bytes> valueSerializer,
            final Function<Bytes, V> valueDeserializer) {
        super(nodeLoader, rootHash, valueSerializer, valueDeserializer);
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
        applyUpdates(Optional.of(nodeUpdater));
    }

    @Override
    public Bytes32 getRootHash() {
        applyUpdates(Optional.empty());
        return root.getHash();
    }

    // Structure pour stocker un update
    private record Update<V>(Bytes path, Optional<V> value) {
        byte nibbleAt(final int index) {
            return index < path.size() ? path.get(index) : 0;
        }
    }

    // Point d'entrée principal
    private void applyUpdates(final Optional<NodeUpdater> nodeUpdater) {
        if (pendingUpdates.isEmpty()) return;

        try {
            List<Update<V>> updates = pendingUpdates.entrySet().stream()
                    .map(e -> new Update<>(bytesToPath(e.getKey()), e.getValue()))
                    .toList();

            if (updates.size() >= MIN_UPDATES_FOR_PARALLEL) {
                applyParallel(updates, nodeUpdater);
            } else {
                applySequential(nodeUpdater);
            }
        } finally {
            pendingUpdates.clear();
        }
    }

    // Application parallèle
    private void applyParallel(final List<Update<V>> updates, final Optional<NodeUpdater> nodeUpdater) {
        CommitCache cache = new CommitCache();

        // Descendre jusqu'à une BranchNode
        NodeInfo<V> nodeInfo = findBranchNode(root, Bytes.EMPTY, 0);

        if (nodeInfo.node instanceof BranchNode<V> branchNode) {
            BranchWrapper wrapper = new BranchWrapper(branchNode);

            // Grouper les updates par nibble
            Map<Byte, List<Update<V>>> grouped = updates.stream()
                    .collect(Collectors.groupingBy(u -> u.nibbleAt(nodeInfo.depth)));

            // Traiter chaque groupe en parallèle
            List<CompletableFuture<Void>> futures = grouped.entrySet().stream()
                    .map(entry -> CompletableFuture.runAsync(() ->
                                    processGroup(wrapper, entry.getKey(), entry.getValue(),
                                            nodeInfo.location, nodeInfo.depth,
                                            nodeUpdater.isPresent() ? Optional.of(cache) : Optional.empty()),
                            VIRTUAL_POOL))
                    .toList();

            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();

            this.root = wrapper.buildNode();

            if (nodeUpdater.isPresent()) {
                cache.commitTo(nodeUpdater.get());
                storeRoot(nodeUpdater.get());
            }
        } else {
            applySequential(nodeUpdater);
        }
    }

    // Trouver la première BranchNode en descendant les Extensions
    private NodeInfo<V> findBranchNode(final Node<V> node, final Bytes location, final int depth) {
        if (node instanceof ExtensionNode<V> ext) {
            Node<V> child = ext.getChild();
            Bytes newLocation = Bytes.concatenate(location, ext.getPath());
            return findBranchNode(child, newLocation, depth + ext.getPath().size());
        }
        return new NodeInfo<V>(node, location, depth);
    }

    private record NodeInfo<V>(Node<V> node, Bytes location, int depth) {}

    // Traiter un groupe d'updates
    private void processGroup(final BranchWrapper wrapper, final byte nibble,
                              final List<Update<V>> updates, final Bytes location,
                              final int depth, final Optional<CommitCache> cache) {
        final Node<V> current = wrapper.getChild(nibble);
        final Bytes childLocation = Bytes.concatenate(location, Bytes.of(nibble));

        NodeInfo<V> info ;
        if (current instanceof ExtensionNode<V> ) {
            info = findBranchNode(current, childLocation, depth + 1);
        } else {
            info = new NodeInfo<>(current, childLocation, depth + 1);
        }
        final Node<V> updated;
        if (updates.size() >= MIN_UPDATES_FOR_PARALLEL && info.node instanceof  BranchNode<V> branchNode) {
            BranchWrapper childWrapper = new BranchWrapper(branchNode);
            processGroupsInParallel(childWrapper, updates, info.location, info.depth, cache);
            updated  = childWrapper.buildNode();
        } else{
            updated = applyUpdatesSequentially(current, updates, depth);

        }

        if (cache.isPresent()) {
            updated.accept(childLocation, new CommitVisitor<>(
                    (loc, hash, bytes) -> cache.get().store(loc, hash, bytes)));
        } else {
            Objects.requireNonNull(updated.getHash());
        }
        wrapper.setChild(nibble, updated);

    }

    private void processGroupsInParallel(final BranchWrapper wrapper, final List<Update<V>> updates,
                                         final Bytes location, final int depth, final Optional<CommitCache> cache) {
        Map<Byte, List<Update<V>>> grouped = updates.stream()
                .collect(Collectors.groupingBy(u -> u.nibbleAt(depth)));

        List<CompletableFuture<Void>> futures = grouped.entrySet().stream()
                .map(entry -> CompletableFuture.runAsync(() ->
                                processGroup(wrapper, entry.getKey(), entry.getValue(), location, depth, cache),
                        VIRTUAL_POOL))
                .toList();

        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
    }

    // Application séquentielle des updates sur un noeud
    private Node<V> applyUpdatesSequentially(final Node<V> node, final List<Update<V>> updates, final int depth) {
        Node<V> current = node;
        for (Update<V> update : updates) {
            Bytes path = update.path.slice(depth);
            PathNodeVisitor<V> visitor = update.value.isPresent()
                    ? getPutVisitor(update.value.get())
                    : getRemoveVisitor();
            current = current.accept(visitor, path);
        }
        return current;
    }

    // Fallback séquentiel
    private void applySequential(final Optional<NodeUpdater> nodeUpdater) {
        pendingUpdates.forEach((key, value) -> {
            if (value.isPresent()) {
                super.put(key, value.get());
            } else {
                super.remove(key);
            }
        });
        nodeUpdater.ifPresent(super::commit);
    }

    private void storeRoot(final NodeUpdater nodeUpdater) {
        final Bytes32 hash = root.getHash();
        nodeUpdater.store(Bytes.EMPTY, hash, root.getEncodedBytes());
        this.root = new StoredNode<>(nodeFactory, Bytes.EMPTY, hash);
    }

    // Wrapper pour BranchNode avec modifications thread-safe
    private class BranchWrapper {
        private final BranchNode<V> original;
        private final List<Node<V>> children;

        BranchWrapper(final BranchNode<V> node) {
            this.original = node;
            this.children = Collections.synchronizedList(new ArrayList<>(node.getChildren()));
        }

        Node<V> getChild(final byte index) {
            return children.get(index);
        }

        void setChild(final byte index, final Node<V> child) {
            children.set(index, child);
        }

        Node<V> buildNode() {
            return original.replaceAllChildren(children, true);
        }
    }

    // Cache pour les commits
    private static class CommitCache {
        private final Map<Bytes, NodeData> cache = new ConcurrentHashMap<>();

        void store(final Bytes location, final Bytes32 hash, final Bytes bytes) {
            cache.put(location, new NodeData(hash, bytes));
        }

        void commitTo(final NodeUpdater updater) {
            cache.forEach((loc, data) -> updater.store(loc, data.hash, data.bytes));
        }

        record NodeData(Bytes32 hash, Bytes bytes) {}
    }
}