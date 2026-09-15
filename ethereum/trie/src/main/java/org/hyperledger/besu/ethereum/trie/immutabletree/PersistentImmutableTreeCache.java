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
package org.hyperledger.besu.ethereum.trie.immutabletree;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Persistent immutable tree cache.
 *
 * <p>Holds full in-memory trees keyed by root ({@link RootKind#STATE} or {@link RootKind#STORAGE}).
 * Updates are copy-on-write: previous roots keep resolving to their historical state. Missing nodes
 * are loaded from disk, cached, and returned. Nodes in active use are locked against pruning.
 */
public final class PersistentImmutableTreeCache {

  private final DiskNodeLoader diskLoader;
  private final long pruneAfterBlocks;
  private final AtomicLong currentBlock = new AtomicLong(0);

  /** Content-addressed in-memory nodes. */
  private final ConcurrentHashMap<Bytes32, CachedNodeRecord> nodes = new ConcurrentHashMap<>();

  /** Live roots indexed by hash + kind. */
  private final ConcurrentHashMap<RootKey, TreeRootHandle> roots = new ConcurrentHashMap<>();

  private volatile TreeRootHandle headState;
  private volatile TreeRootHandle newPayloadState;

  public PersistentImmutableTreeCache(final DiskNodeLoader diskLoader, final long pruneAfterBlocks) {
    this.diskLoader = Objects.requireNonNull(diskLoader);
    if (pruneAfterBlocks < 1) {
      throw new IllegalArgumentException("pruneAfterBlocks must be >= 1");
    }
    this.pruneAfterBlocks = pruneAfterBlocks;
    final TreeRootHandle empty =
        registerRoot(EmptyTreeNode.INSTANCE, RootKind.STATE, TreeRole.HEAD);
    this.headState = empty;
    // Same empty snapshot until head and newPayload diverge through COW updates.
    this.newPayloadState = empty;
  }

  public void advanceBlock(final long blockNumber) {
    currentBlock.set(blockNumber);
  }

  public long currentBlock() {
    return currentBlock.get();
  }

  public TreeRootHandle head() {
    return headState;
  }

  public TreeRootHandle newPayload() {
    return newPayloadState;
  }

  /** Promote a root to head (e.g. after forkchoice). Keeps the previous head as a fork root. */
  public void setHead(final Bytes32 rootHash, final RootKind kind) {
    final TreeRootHandle handle =
        roots.computeIfAbsent(
            new RootKey(rootHash, kind),
            key -> {
              final ImmutableTreeNode node = materializeRoot(rootHash, Bytes.EMPTY);
              return new TreeRootHandle(rootHash, kind, TreeRole.HEAD, node);
            });
    headState = handle;
  }

  /** Bind the in-memory newPayload tree to a specific root. */
  public void setNewPayload(final Bytes32 rootHash, final RootKind kind) {
    final TreeRootHandle handle =
        roots.computeIfAbsent(
            new RootKey(rootHash, kind),
            key -> {
              final ImmutableTreeNode node = materializeRoot(rootHash, Bytes.EMPTY);
              return new TreeRootHandle(rootHash, kind, TreeRole.NEW_PAYLOAD, node);
            });
    newPayloadState = handle;
  }

  /**
   * Registers an existing in-memory root (for example after a COW put on top of head). Parallel
   * forks keep their own handles.
   */
  public TreeRootHandle registerRoot(
      final ImmutableTreeNode rootNode, final RootKind kind, final TreeRole role) {
    final Bytes32 hash = rootNode.hash();
    cacheNode(rootNode);
    final long block = currentBlock.get();
    return roots.compute(
        new RootKey(hash, kind),
        (key, existing) -> {
          if (existing == null) {
            final TreeRootHandle created = new TreeRootHandle(hash, kind, role, rootNode);
            created.touch(block);
            return created;
          }
          existing.replaceRootNode(rootNode);
          existing.touch(block);
          return existing;
        });
  }

  public Optional<TreeRootHandle> findRoot(final Bytes32 rootHash, final RootKind kind) {
    return Optional.ofNullable(roots.get(new RootKey(rootHash, kind)));
  }

  /**
   * Returns the tree for a root. If the root is unknown in memory, attempts to load it from disk
   * and register it as a fork.
   */
  public TreeRootHandle treeForRoot(final Bytes32 rootHash, final RootKind kind) {
    return roots.computeIfAbsent(
        new RootKey(rootHash, kind),
        key -> {
          final ImmutableTreeNode node = materializeRoot(rootHash, Bytes.EMPTY);
          return new TreeRootHandle(rootHash, kind, TreeRole.FORK, node);
        });
  }

  /** Begin a locked traversal over a root. Must be closed to allow pruning. */
  public TreeTraversalLock beginTraversal(final TreeRootHandle root) {
    root.touch(currentBlock.get());
    final TreeTraversalLock lock = new TreeTraversalLock(root);
    final CachedNodeRecord record = touchAndGet(root.rootNode());
    lock.lockNode(record);
    return lock;
  }

  public Optional<Bytes> get(final TreeRootHandle root, final Bytes key) {
    try (TreeTraversalLock lock = beginTraversal(root)) {
      return ImmutableTreeOps.get(root.rootNode(), key, node -> resolve(node, lock));
    }
  }

  /**
   * Copy-on-write put. Returns a new root handle; the previous root remains valid and unchanged.
   * When {@code onTopOfHead} is true, the result is registered as a fork sitting above head.
   */
  public TreeRootHandle put(
      final TreeRootHandle base,
      final Bytes key,
      final Bytes value,
      final boolean onTopOfHead) {
    try (TreeTraversalLock lock = beginTraversal(base)) {
      final ImmutableTreeNode newRoot =
          ImmutableTreeOps.put(base.rootNode(), key, value, node -> resolve(node, lock));
      cacheSubtree(newRoot);
      final TreeRole role =
          onTopOfHead
              ? TreeRole.FORK
              : base.role() == TreeRole.NEW_PAYLOAD ? TreeRole.NEW_PAYLOAD : TreeRole.FORK;
      final TreeRootHandle handle = registerRoot(newRoot, base.kind(), role);
      if (base.role() == TreeRole.NEW_PAYLOAD || role == TreeRole.NEW_PAYLOAD) {
        newPayloadState = handle;
      }
      return handle;
    }
  }

  /**
   * Resolves a node: stored placeholders are loaded from disk, cached, and returned. Locked when a
   * traversal lock is provided.
   */
  public ImmutableTreeNode resolve(final ImmutableTreeNode node, final TreeTraversalLock lock) {
    final ImmutableTreeNode materialized = materialize(node);
    final CachedNodeRecord record = touchAndGet(materialized);
    if (lock != null) {
      lock.lockNode(record);
    }
    return record.node();
  }

  public ImmutableTreeNode resolve(final ImmutableTreeNode node) {
    return resolve(node, null);
  }

  /**
   * Prune nodes not accessed for {@code pruneAfterBlocks} blocks. Skips locked nodes and nodes
   * belonging to trees currently in use.
   *
   * @return number of nodes removed from memory
   */
  public int prune() {
    final long block = currentBlock.get();
    final long threshold = block - pruneAfterBlocks;
    if (threshold < 0) {
      return 0;
    }

    // Do not prune while any registered root is actively traversed.
    for (final TreeRootHandle handle : roots.values()) {
      if (handle.isInUse()) {
        return 0;
      }
    }

    // Drop abandoned fork roots so their trees become unreachable for GC.
    for (final Map.Entry<RootKey, TreeRootHandle> entry : roots.entrySet()) {
      final TreeRootHandle handle = entry.getValue();
      if (handle.isInUse()) {
        continue;
      }
      if (handle == headState || handle == newPayloadState) {
        continue;
      }
      if (handle.role() == TreeRole.HEAD || handle.role() == TreeRole.NEW_PAYLOAD) {
        // Only drop when not the live head/newPayload pointers above.
        if (handle.rootHash().equals(headState.rootHash())
            || handle.rootHash().equals(newPayloadState.rootHash())) {
          continue;
        }
      }
      if (handle.lastAccessBlock() <= threshold) {
        roots.remove(entry.getKey(), handle);
      }
    }

    int removed = 0;
    for (final Map.Entry<Bytes32, CachedNodeRecord> entry : nodes.entrySet()) {
      final CachedNodeRecord record = entry.getValue();
      if (record.isLocked()) {
        continue;
      }
      if (record.lastAccessBlock() > threshold) {
        continue;
      }
      // Keep currently pinned head / newPayload root nodes.
      if (isPinnedRootNode(record.hash())) {
        continue;
      }
      if (nodes.remove(entry.getKey(), record)) {
        removed++;
      }
    }
    return removed;
  }

  public int cachedNodeCount() {
    return nodes.size();
  }

  public int cachedRootCount() {
    return roots.size();
  }

  private boolean isPinnedRootNode(final Bytes32 hash) {
    final TreeRootHandle head = headState;
    final TreeRootHandle payload = newPayloadState;
    return (head != null && head.rootHash().equals(hash))
        || (payload != null && payload.rootHash().equals(hash));
  }

  private CachedNodeRecord touchAndGet(final ImmutableTreeNode node) {
    final long block = currentBlock.get();
    return nodes.compute(
        node.hash(),
        (hash, existing) -> {
          if (existing == null) {
            return new CachedNodeRecord(node, block);
          }
          if (existing.node().isStored() && !node.isStored()) {
            existing.replaceMaterialized(node, block);
          } else {
            existing.touch(block);
          }
          return existing;
        });
  }

  private void cacheNode(final ImmutableTreeNode node) {
    if (node instanceof EmptyTreeNode || node.isStored()) {
      return;
    }
    touchAndGet(node);
  }

  private void cacheSubtree(final ImmutableTreeNode node) {
    cacheNode(node);
    for (final ImmutableTreeNode child : node.children()) {
      cacheSubtree(child);
    }
  }

  private ImmutableTreeNode materialize(final ImmutableTreeNode node) {
    if (!node.isStored()) {
      cacheNode(node);
      return node;
    }
    final StoredTreeNode stored = (StoredTreeNode) node;
    final CachedNodeRecord existing = nodes.get(stored.hash());
    if (existing != null && !existing.node().isStored()) {
      existing.touch(currentBlock.get());
      return existing.node();
    }
    return materializeRoot(stored.hash(), stored.location());
  }

  private ImmutableTreeNode materializeRoot(final Bytes32 hash, final Bytes location) {
    if (hash.equals(TreeCodec.EMPTY_HASH)) {
      return EmptyTreeNode.INSTANCE;
    }
    final CachedNodeRecord cached = nodes.get(hash);
    if (cached != null && !cached.node().isStored()) {
      cached.touch(currentBlock.get());
      return cached.node();
    }
    final Bytes rlp =
        diskLoader
            .load(location, hash)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Missing trie node on disk hash=" + hash + " location=" + location));
    final ImmutableTreeNode decoded = TreeNodeDecoder.decode(location, hash, rlp);
    touchAndGet(decoded);
    return decoded;
  }

  private record RootKey(Bytes32 hash, RootKind kind) {}
}
