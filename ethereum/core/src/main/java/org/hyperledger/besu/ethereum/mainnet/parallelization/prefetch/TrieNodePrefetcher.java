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
package org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.StoredNode;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.patricia.BranchNode;
import org.hyperledger.besu.ethereum.trie.patricia.ExtensionNode;
import org.hyperledger.besu.ethereum.trie.patricia.TrieNodeDecoder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Prefetches into the trie node cache the trie nodes on the paths of the accounts and storage slots
 * of a block access list, so that the state root computation walks them from the cache.
 *
 * <p>The tries are read one level at a time: the nodes of a level are read in batches of one
 * MultiGet each, and their children on the requested paths form the next level. Inlined children
 * are descended into without a read. Storage tries are read along with the account trie: their
 * roots are stored at the account hash, so they are part of the first level, without waiting for
 * the account leaf and its storage root. A node already in the cache is not read again.
 */
final class TrieNodePrefetcher {

  /** Number of children of a branch node. */
  private static final int RADIX = 16;

  /** A key to reach, as a nibble path. */
  private record Target(Bytes path) {}

  /**
   * A node to read: its trie (empty for the account trie), location, hash and the targets below.
   * The hash of a storage trie root is not known (the account has not been read): it is the node
   * stored at that location.
   */
  private record PendingNode(
      Bytes accountHash, Bytes location, Optional<Bytes32> hash, List<Target> targets) {

    boolean isStorageTrieNode() {
      return !accountHash.isEmpty();
    }

    byte[] storageKey() {
      return Bytes.concatenate(accountHash, location).toArrayUnsafe();
    }
  }

  private final BonsaiWorldStateKeyValueStorage storage;
  private final FlatDbCacheManager cache;
  private final Executor fetchExecutor;
  private final int batchSize;
  private final AtomicInteger readNodes = new AtomicInteger();
  private final AtomicInteger cachedNodes = new AtomicInteger();

  private TrieNodePrefetcher(
      final BonsaiWorldStateKeyValueStorage storage,
      final Executor fetchExecutor,
      final int batchSize) {
    this.storage = storage;
    this.cache = storage.getCacheManager();
    this.fetchExecutor = fetchExecutor;
    this.batchSize = batchSize;
  }

  /**
   * Prefetches the trie nodes on the paths of the block access list.
   *
   * @param storage the storage of the world state the access list applies to
   * @param stateRoot the state root of that world state
   * @param blockAccessList the block access list
   * @param fetchExecutor the executor for the reads
   * @param batchSize the number of nodes per MultiGet, all of a level at once if not positive
   * @return a future that completes with a summary once every level is read
   */
  static CompletableFuture<String> prefetch(
      final BonsaiWorldStateKeyValueStorage storage,
      final Hash stateRoot,
      final BlockAccessList blockAccessList,
      final Executor fetchExecutor,
      final int batchSize) {
    final Bytes32 rootHash = Bytes32.wrap(stateRoot.getBytes());
    if (rootHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return CompletableFuture.completedFuture("empty state");
    }
    final TrieNodePrefetcher prefetcher = new TrieNodePrefetcher(storage, fetchExecutor, batchSize);
    return prefetcher
        .prefetchLevel(roots(rootHash, blockAccessList))
        .thenApply(
            unused ->
                prefetcher.readNodes.get()
                    + " trie nodes read, "
                    + prefetcher.cachedNodes.get()
                    + " already cached");
  }

  /** The account trie root, and the storage trie root of every account with slots to reach. */
  private static List<PendingNode> roots(
      final Bytes32 rootHash, final BlockAccessList blockAccessList) {
    final List<PendingNode> roots = new ArrayList<>();
    final List<Target> accounts = new ArrayList<>(blockAccessList.accountChanges().size());
    for (final BlockAccessList.AccountChanges account : blockAccessList.accountChanges()) {
      final Bytes32 accountHash = Bytes32.wrap(account.address().addressHash().getBytes());
      accounts.add(target(accountHash));
      final Set<StorageSlotKey> slotKeys = new LinkedHashSet<>();
      account.storageChanges().forEach(change -> slotKeys.add(change.slot()));
      account.storageReads().forEach(read -> slotKeys.add(read.slot()));
      if (!slotKeys.isEmpty()) {
        final List<Target> slots = new ArrayList<>(slotKeys.size());
        slotKeys.forEach(
            slotKey -> slots.add(target(Bytes32.wrap(slotKey.getSlotHash().getBytes()))));
        roots.add(new PendingNode(accountHash, Bytes.EMPTY, Optional.empty(), slots));
      }
    }
    roots.add(new PendingNode(Bytes.EMPTY, Bytes.EMPTY, Optional.of(rootHash), accounts));
    return roots;
  }

  private static Target target(final Bytes32 key) {
    return new Target(CompactEncoding.bytesToPath(key));
  }

  private CompletableFuture<Void> prefetchLevel(final List<PendingNode> level) {
    if (level.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    final List<List<PendingNode>> batches =
        batchSize > 0 ? Lists.partition(level, batchSize) : List.of(level);
    final List<CompletableFuture<List<PendingNode>>> reads = new ArrayList<>(batches.size());
    for (final List<PendingNode> batch : batches) {
      reads.add(CompletableFuture.supplyAsync(() -> readBatch(batch), fetchExecutor));
    }
    return CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new))
        .thenCompose(
            unused -> {
              final List<PendingNode> nextLevel = new ArrayList<>();
              reads.forEach(read -> nextLevel.addAll(read.join()));
              return prefetchLevel(nextLevel);
            });
  }

  /** Reads the nodes of a batch that are not cached yet and returns their children to read. */
  private List<PendingNode> readBatch(final List<PendingNode> batch) {
    final List<PendingNode> children = new ArrayList<>();
    final List<PendingNode> toRead = new ArrayList<>(batch.size());
    for (final PendingNode pending : batch) {
      final Optional<Bytes> cached =
          pending
              .hash()
              .flatMap(
                  hash ->
                      pending.isStorageTrieNode()
                          ? cache.getStorageTrieNode(hash)
                          : cache.getAccountTrieNode(hash));
      if (cached.isPresent()) {
        cachedNodes.incrementAndGet();
        descend(pending, cached.get(), children);
      } else {
        toRead.add(pending);
      }
    }
    if (toRead.isEmpty()) {
      return children;
    }

    final List<byte[]> keys = new ArrayList<>(toRead.size());
    toRead.forEach(pending -> keys.add(pending.storageKey()));
    final Optional<List<Optional<Bytes>>> maybeNodes = storage.getMultipleTrieNodesByLocation(keys);
    if (maybeNodes.isEmpty()) {
      return children;
    }
    final List<Optional<Bytes>> nodes = maybeNodes.get();
    for (int i = 0; i < toRead.size(); i++) {
      final PendingNode pending = toRead.get(i);
      final Optional<Bytes> node = nodes.get(i);
      if (node.isEmpty()) {
        continue;
      }
      final Bytes32 hash = Bytes32.wrap(Hash.hash(node.get()).getBytes());
      // storage holds the nodes of one state per location: skip a node of another state
      if (pending.hash().isPresent() && !pending.hash().get().equals(hash)) {
        continue;
      }
      readNodes.incrementAndGet();
      if (pending.isStorageTrieNode()) {
        cache.putStorageTrieNode(hash, node.get());
      } else {
        cache.putAccountTrieNode(hash, node.get());
      }
      descend(pending, node.get(), children);
    }
    return children;
  }

  private void descend(final PendingNode pending, final Bytes rlp, final List<PendingNode> next) {
    descend(
        TrieNodeDecoder.decode(pending.location(), rlp),
        pending.accountHash(),
        pending.location(),
        pending.targets(),
        next);
  }

  /** Follows the targets below {@code node}; children referenced by hash go to the next level. */
  private void descend(
      final Node<Bytes> node,
      final Bytes accountHash,
      final Bytes location,
      final List<Target> targets,
      final List<PendingNode> next) {
    if (node instanceof StoredNode<Bytes>) {
      next.add(new PendingNode(accountHash, location, Optional.of(node.getHash()), targets));
    } else if (node instanceof BranchNode<Bytes> branch) {
      descendBranch(branch, accountHash, location, targets, next);
    } else if (node instanceof ExtensionNode<Bytes> extension) {
      final Bytes extensionPath = extension.getPath();
      final List<Target> below = new ArrayList<>(targets.size());
      for (final Target target : targets) {
        if (startsWith(target.path(), location.size(), extensionPath)) {
          below.add(target);
        }
      }
      if (!below.isEmpty()) {
        descend(
            extension.getChild(),
            accountHash,
            Bytes.concatenate(location, extensionPath),
            below,
            next);
      }
    }
  }

  private void descendBranch(
      final BranchNode<Bytes> branch,
      final Bytes accountHash,
      final Bytes location,
      final List<Target> targets,
      final List<PendingNode> next) {
    final int depth = location.size();
    // lists created on demand: deep in the trie, a node is mostly on the path of a single target
    final List<List<Target>> byNibble = new ArrayList<>(Collections.nCopies(RADIX, null));
    for (final Target target : targets) {
      final int nibble = target.path().get(depth);
      // the path terminator (16) only matches a value in the branch itself
      if (nibble < RADIX) {
        if (byNibble.get(nibble) == null) {
          byNibble.set(nibble, new ArrayList<>(1));
        }
        byNibble.get(nibble).add(target);
      }
    }
    for (int nibble = 0; nibble < RADIX; nibble++) {
      if (byNibble.get(nibble) != null) {
        descend(
            branch.child((byte) nibble),
            accountHash,
            Bytes.concatenate(location, Bytes.of(nibble)),
            byNibble.get(nibble),
            next);
      }
    }
  }

  private static boolean startsWith(final Bytes path, final int offset, final Bytes prefix) {
    return path.size() >= offset + prefix.size()
        && path.slice(offset, prefix.size()).equals(prefix);
  }
}
