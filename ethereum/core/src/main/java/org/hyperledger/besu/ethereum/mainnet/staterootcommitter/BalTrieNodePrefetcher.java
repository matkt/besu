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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.trie.patricia.DefaultNodeFactory.NB_CHILD;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.StoredNode;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeStrategy.TrieNodeRequest;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.patricia.BranchNode;
import org.hyperledger.besu.ethereum.trie.patricia.ExtensionNode;
import org.hyperledger.besu.ethereum.trie.patricia.StoredNodeFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads, ahead of the BAL state root computation, every trie node the computation will traverse.
 *
 * <p>The BAL gives the full write set up front, so the paths through the account trie and through
 * each touched storage trie are known before any node is read. Instead of letting the trie update
 * resolve nodes one by one (one synchronous read per node, serial along each path), the walk is
 * level-synchronous across all tries at once:
 *
 * <ol>
 *   <li>the frontier of the current depth is sorted by storage key ({@code location} for the
 *       account trie, {@code accountHash ‖ location} for storage tries), so that neighbouring
 *       requests hit the same SST files and data blocks;
 *   <li>the sorted frontier is cut into contiguous ranges, each loaded with a single MultiGet, the
 *       ranges running in parallel on the I/O executor;
 *   <li>each loaded node is hash-checked, decoded, and the children lying on a write path form the
 *       next frontier.
 * </ol>
 *
 * <p>The number of I/O round trips is therefore bounded by the trie depth rather than by the number
 * of nodes, and the I/O parallelism is set by the number of ranges instead of by the trie
 * computation's CPU pools. Only written keys are followed (reads do not affect the state root). For
 * removals, the sibling of a branch that may collapse is also loaded, since the trie needs it to
 * restructure.
 *
 * <p>This is best-effort: a node that is missing or fails to decode is simply not cached and will
 * be loaded on demand by the trie.
 */
public final class BalTrieNodePrefetcher {

  private static final Logger LOG = LoggerFactory.getLogger(BalTrieNodePrefetcher.class);

  /** Unsigned lexicographic order, matching the RocksDB default comparator. */
  private static final Comparator<Pending> STORAGE_KEY_ORDER =
      (a, b) -> Arrays.compareUnsigned(a.storageKey(), b.storageKey());

  /** Decoder only: children stay as unresolved {@link StoredNode} references. */
  private static final StoredNodeFactory<Bytes> DECODER =
      new StoredNodeFactory<>(
          (location, hash) -> Optional.empty(), Function.identity(), Function.identity());

  /** Loads a batch of trie nodes; results are hash-verified and in request order. */
  @FunctionalInterface
  public interface BatchNodeLoader {
    List<Optional<Bytes>> load(List<TrieNodeRequest> requests);
  }

  /**
   * A trie to walk.
   *
   * @param accountHash owning account for a storage trie, {@code null} for the account trie
   * @param rootHash the trie's root hash in the parent state
   * @param keys the written keys, as leaf paths
   */
  public record TrieWalk(Hash accountHash, Bytes32 rootHash, List<KeyPath> keys) {}

  /**
   * A written key.
   *
   * @param path the key's nibble path, as produced by {@link CompactEncoding#bytesToPath}
   * @param removal whether the key is deleted
   */
  public record KeyPath(Bytes path, boolean removal) {

    static KeyPath of(final Bytes key, final boolean removal) {
      return new KeyPath(CompactEncoding.bytesToPath(key), removal);
    }
  }

  /**
   * Outcome of a prefetch.
   *
   * @param nodes loaded nodes, keyed by hash
   * @param rounds number of level-synchronous rounds
   * @param requests number of node reads issued
   */
  public record Result(Map<Bytes32, Bytes> nodes, int rounds, int requests) {

    static Result empty() {
      return new Result(Map.of(), 0, 0);
    }
  }

  /** A node to load, with the written keys whose paths go through it. */
  private record Pending(
      Hash accountHash, Bytes location, Bytes32 hash, List<KeyPath> keys, byte[] storageKey) {

    static Pending of(
        final Hash accountHash,
        final Bytes location,
        final Bytes32 hash,
        final List<KeyPath> keys) {
      final byte[] storageKey =
          accountHash == null
              ? location.toArrayUnsafe()
              : Bytes.concatenate(accountHash.getBytes(), location).toArrayUnsafe();
      return new Pending(accountHash, location, hash, keys, storageKey);
    }

    TrieNodeRequest request() {
      return accountHash == null
          ? TrieNodeRequest.account(location, hash)
          : TrieNodeRequest.storage(accountHash, location, hash);
    }
  }

  private BalTrieNodePrefetcher() {}

  /**
   * Prefetches the account and storage trie nodes the BAL state root computation will traverse.
   *
   * @param worldState the parent world state the computation runs on
   * @param accountLookup the block access list
   * @param batchSize maximum number of nodes per MultiGet; a non-positive value loads each level in
   *     a single MultiGet
   * @param ioExecutor executor running the MultiGets
   * @return the loaded nodes
   */
  public static Result prefetch(
      final BonsaiWorldState worldState,
      final BlockAccessListAccountLookup accountLookup,
      final int batchSize,
      final Executor ioExecutor) {
    final BonsaiWorldStateKeyValueStorage storage = worldState.getWorldStateStorage();
    final List<TrieWalk> walks = new ArrayList<>();

    final List<KeyPath> accountKeys = new ArrayList<>();
    final List<BlockAccessList.AccountChanges> withStorageChanges = new ArrayList<>();
    for (final BlockAccessList.AccountChanges changes : accountLookup.accountChanges()) {
      if (changes.hasAnyChange()) {
        // Account deletions (accounts becoming empty) are not known up front and are rare; their
        // collapse siblings are left to on-demand loading.
        accountKeys.add(KeyPath.of(changes.address().addressHash().getBytes(), false));
      }
      if (!changes.storageChanges().isEmpty()) {
        withStorageChanges.add(changes);
      }
    }
    if (!accountKeys.isEmpty()) {
      walks.add(
          new TrieWalk(
              null, Bytes32.wrap(worldState.getWorldStateRootHash().getBytes()), accountKeys));
    }

    // Storage roots come from the flat account values: one MultiGet, and the account trie walk
    // does not have to finish before the storage trie walks start.
    if (!withStorageChanges.isEmpty()) {
      final List<byte[]> accountFlatKeys = new ArrayList<>(withStorageChanges.size());
      for (final BlockAccessList.AccountChanges changes : withStorageChanges) {
        accountFlatKeys.add(changes.address().addressHash().getBytes().toArrayUnsafe());
      }
      final List<Optional<Bytes>> accounts =
          storage.getMultipleFlat(ACCOUNT_INFO_STATE, accountFlatKeys);
      for (int i = 0; i < withStorageChanges.size(); i++) {
        final Optional<Bytes> maybeAccount = accounts.get(i);
        if (maybeAccount.isEmpty()) {
          continue;
        }
        final Hash storageRoot =
            PmtStateTrieAccountValue.readFrom(RLP.input(maybeAccount.get())).getStorageRoot();
        if (Hash.EMPTY_TRIE_HASH.equals(storageRoot)) {
          continue;
        }
        final BlockAccessList.AccountChanges changes = withStorageChanges.get(i);
        final List<KeyPath> slotKeys = new ArrayList<>(changes.storageChanges().size());
        for (final BlockAccessList.SlotChanges slotChanges : changes.storageChanges()) {
          final UInt256 newValue = slotChanges.changes().getLast().newValue();
          slotKeys.add(
              KeyPath.of(
                  slotChanges.slot().getSlotHash().getBytes(),
                  newValue == null || newValue.isZero()));
        }
        walks.add(
            new TrieWalk(
                changes.address().addressHash(), Bytes32.wrap(storageRoot.getBytes()), slotKeys));
      }
    }

    return walk(walks, storage::getTrieNodes, batchSize, ioExecutor);
  }

  /**
   * Walks the given tries level by level, loading each level with range-partitioned MultiGets.
   *
   * @param walks the tries and written keys to follow
   * @param loader batch node loader
   * @param batchSize maximum number of nodes per MultiGet; non-positive means one per level
   * @param ioExecutor executor running the MultiGets
   * @return the loaded nodes
   */
  public static Result walk(
      final List<TrieWalk> walks,
      final BatchNodeLoader loader,
      final int batchSize,
      final Executor ioExecutor) {
    if (walks.isEmpty()) {
      return Result.empty();
    }
    final Map<Bytes32, Bytes> nodes = new ConcurrentHashMap<>();
    List<Pending> frontier = new ArrayList<>(walks.size());
    for (final TrieWalk walk : walks) {
      frontier.add(Pending.of(walk.accountHash(), Bytes.EMPTY, walk.rootHash(), walk.keys()));
    }

    int rounds = 0;
    int requests = 0;
    while (!frontier.isEmpty() && !Thread.currentThread().isInterrupted()) {
      rounds++;
      requests += frontier.size();
      frontier.sort(STORAGE_KEY_ORDER);
      final int rangeSize = batchSize > 0 ? batchSize : frontier.size();
      if (frontier.size() <= rangeSize) {
        frontier = loadRange(frontier, loader, nodes);
        continue;
      }
      final List<CompletableFuture<List<Pending>>> ranges = new ArrayList<>();
      for (int from = 0; from < frontier.size(); from += rangeSize) {
        final List<Pending> range =
            frontier.subList(from, Math.min(from + rangeSize, frontier.size()));
        ranges.add(
            CompletableFuture.supplyAsync(() -> loadRange(range, loader, nodes), ioExecutor));
      }
      final List<Pending> next = new ArrayList<>();
      for (final CompletableFuture<List<Pending>> range : ranges) {
        next.addAll(range.join());
      }
      frontier = next;
    }
    return new Result(nodes, rounds, requests);
  }

  /** Loads one contiguous range of the frontier and returns the children to load next. */
  private static List<Pending> loadRange(
      final List<Pending> range, final BatchNodeLoader loader, final Map<Bytes32, Bytes> nodes) {
    final List<TrieNodeRequest> requests = new ArrayList<>(range.size());
    for (final Pending pending : range) {
      requests.add(pending.request());
    }
    final List<Optional<Bytes>> loaded = loader.load(requests);
    final List<Pending> next = new ArrayList<>();
    for (int i = 0; i < range.size(); i++) {
      final Optional<Bytes> maybeNode = loaded.get(i);
      if (maybeNode.isEmpty()) {
        continue;
      }
      final Pending pending = range.get(i);
      nodes.put(pending.hash(), maybeNode.get());
      if (pending.keys().isEmpty()) {
        // Fetch-only node (sibling of a collapsing branch): no need to descend.
        continue;
      }
      try {
        final Node<Bytes> node = DECODER.decode(pending.location(), maybeNode.get());
        follow(pending.accountHash(), node, pending.location(), pending.keys(), next);
      } catch (final RuntimeException e) {
        LOG.trace("Unable to decode prefetched trie node {}", pending.hash(), e);
      }
    }
    return next;
  }

  /**
   * Collects the children of {@code node} lying on a written path. Inline children are embedded in
   * their parent and are followed without I/O.
   */
  private static void follow(
      final Hash accountHash,
      final Node<Bytes> node,
      final Bytes location,
      final List<KeyPath> keys,
      final List<Pending> next) {
    final int depth = location.size();
    if (node instanceof BranchNode<Bytes> branch) {
      final List<List<KeyPath>> byNibble = new ArrayList<>(Collections.nCopies(NB_CHILD, null));
      boolean onlyRemovals = true;
      for (final KeyPath key : keys) {
        final byte nibble = key.path().get(depth);
        if (nibble == CompactEncoding.LEAF_TERMINATOR) {
          continue;
        }
        if (byNibble.get(nibble) == null) {
          byNibble.set(nibble, new ArrayList<>());
        }
        byNibble.get(nibble).add(key);
        onlyRemovals &= key.removal();
      }
      for (int nibble = 0; nibble < NB_CHILD; nibble++) {
        if (byNibble.get(nibble) != null) {
          followChild(
              accountHash,
              branch.child((byte) nibble),
              Bytes.concatenate(location, Bytes.of(nibble)),
              byNibble.get(nibble),
              next);
        }
      }
      if (onlyRemovals && branch.getValue().isEmpty()) {
        addCollapseSibling(accountHash, branch, location, byNibble, next);
      }
    } else if (node instanceof ExtensionNode<Bytes> extension) {
      final Bytes extensionPath = extension.getPath();
      final List<KeyPath> through = new ArrayList<>(keys.size());
      for (final KeyPath key : keys) {
        if (key.path().size() >= depth + extensionPath.size()
            && key.path().slice(depth, extensionPath.size()).equals(extensionPath)) {
          through.add(key);
        }
      }
      if (!through.isEmpty()) {
        followChild(
            accountHash,
            extension.getChild(),
            Bytes.concatenate(location, extensionPath),
            through,
            next);
      }
    }
    // Leaf and null nodes end the path.
  }

  private static void followChild(
      final Hash accountHash,
      final Node<Bytes> child,
      final Bytes childLocation,
      final List<KeyPath> keys,
      final List<Pending> next) {
    if (child instanceof NullNode) {
      return;
    }
    if (child instanceof StoredNode) {
      next.add(Pending.of(accountHash, childLocation, child.getHash(), keys));
    } else {
      follow(accountHash, child, childLocation, keys, next);
    }
  }

  /**
   * When every written key under a branch is a removal, the branch may be left with a single child
   * and collapse into its parent; the trie then needs that remaining child to restructure. This
   * applies when exactly one existing child is off the written paths.
   */
  private static void addCollapseSibling(
      final Hash accountHash,
      final BranchNode<Bytes> branch,
      final Bytes location,
      final List<List<KeyPath>> byNibble,
      final List<Pending> next) {
    int sibling = -1;
    int siblings = 0;
    for (int nibble = 0; nibble < NB_CHILD; nibble++) {
      if (byNibble.get(nibble) == null && !(branch.child((byte) nibble) instanceof NullNode)) {
        siblings++;
        sibling = nibble;
      }
    }
    if (siblings == 1) {
      final Node<Bytes> child = branch.child((byte) sibling);
      if (child instanceof StoredNode) {
        next.add(
            Pending.of(
                accountHash,
                Bytes.concatenate(location, Bytes.of(sibling)),
                child.getHash(),
                List.of()));
      }
    }
  }

  /** Trie node loader serving prefetched nodes first, falling back to storage. */
  public static final class PrefetchedMerkleTrieLoader extends NoOpBonsaiCachedMerkleTrieLoader {

    private final Map<Bytes32, Bytes> nodes;

    public PrefetchedMerkleTrieLoader(final Map<Bytes32, Bytes> nodes) {
      this.nodes = nodes;
    }

    @Override
    public Optional<Bytes> getAccountStateTrieNode(
        final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
        final Bytes location,
        final Bytes32 nodeHash) {
      final Bytes node = nodes.get(nodeHash);
      return node != null
          ? Optional.of(node)
          : super.getAccountStateTrieNode(worldStateKeyValueStorage, location, nodeHash);
    }

    @Override
    public Optional<Bytes> getAccountStorageTrieNode(
        final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
        final Hash accountHash,
        final Bytes location,
        final Bytes32 nodeHash) {
      final Bytes node = nodes.get(nodeHash);
      return node != null
          ? Optional.of(node)
          : super.getAccountStorageTrieNode(
              worldStateKeyValueStorage, accountHash, location, nodeHash);
    }
  }
}
