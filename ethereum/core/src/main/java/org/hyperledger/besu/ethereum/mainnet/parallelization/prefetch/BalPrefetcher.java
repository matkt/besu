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
package org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mechanism for prefetching world state data based on Block Access List (BAL).
 *
 * <p>This class handles prefetching of flat account and storage data plus {@link
 * KeyValueSegmentIdentifier#TRIE_BRANCH_STORAGE} trie-node keys (path prefixes) to warm the cache
 * before transaction execution, improving parallel processing performance.
 */
@SuppressWarnings("rawtypes")
public class BalPrefetcher {

  private static final Logger LOG = LoggerFactory.getLogger(BalPrefetcher.class);

  /**
   * Multiget chunk size when {@link #batchSize} is unset (0); matches RocksDB-style batch reads.
   */
  private static final int DEFAULT_MULTIGET_PREFETCH_BATCH_SIZE = 64;

  private final boolean isSortingEnabled;
  private final int batchSize;

  /**
   * Creates a new prefetch mechanism.
   *
   * @param isSortingEnabled whether to sort keys before prefetching (may improve DB locality)
   * @param batchSize multiget chunk size for each storage/account fetch; 0 or negative defaults to
   *     64
   */
  public BalPrefetcher(final boolean isSortingEnabled, final int batchSize) {
    this.isSortingEnabled = isSortingEnabled;
    this.batchSize = batchSize;
  }

  /**
   * Prefetches world state reads asynchronously on {@code prefetchCoordinatorExecutor}. Individual
   * multiget batches run on {@code fetchBatchExecutor}.
   */
  public CompletableFuture<Void> prefetch(
      final BonsaiWorldState worldState,
      final BlockAccessList blockAccessList,
      final Executor prefetchCoordinatorExecutor,
      final Executor fetchBatchExecutor) {
    return CompletableFuture.runAsync(
        () -> prefetchBlocking(worldState, blockAccessList, fetchBatchExecutor),
        prefetchCoordinatorExecutor);
  }

  /**
   * Same as {@link #prefetch(BonsaiWorldState, BlockAccessList, Executor, Executor)} with a single
   * executor for coordination and fetch batches.
   */
  public CompletableFuture<Void> prefetch(
      final BonsaiWorldState worldState,
      final BlockAccessList blockAccessList,
      final Executor prefetchExecutor) {
    return prefetch(worldState, blockAccessList, prefetchExecutor, prefetchExecutor);
  }

  private void prefetchBlocking(
      final BonsaiWorldState worldState,
      final BlockAccessList blockAccessList,
      final Executor fetchBatchExecutor) {

    try {
      worldState.disableCacheMerkleTrieLoader();

      // Collect and optionally sort account changes
      final List<BlockAccessList.AccountChanges> accounts =
          isSortingEnabled
              ? blockAccessList.accountChanges().stream()
                  .sorted(Comparator.comparing(ac -> ac.address().addressHash()))
                  .toList()
              : new ArrayList<>(blockAccessList.accountChanges());

      // Collect all keys to prefetch
      final PrefetchPlan plan = collectKeys(accounts);
      final List<byte[]> trieBranchKeys = collectTrieBranchKeys(accounts);

      LOG.debug(
          "Prefetch: collected {} account keys and {} storage keys ({} accounts with storage), {} trie branch keys",
          plan.accountKeys().size(),
          plan.totalStorageKeys(),
          plan.storageKeysPerAccount().size(),
          trieBranchKeys.size());

      // Multiget in chunks of 64: accounts first, then per-address storage batches, then trie
      // branch prefixes, then BAL state root computation runs in {@link BalStateRootCalculator}
      // after this returns.
      fetchKeys(worldState, plan, trieBranchKeys, fetchBatchExecutor);

      LOG.info(
          "Prefetch completed: {} accounts + {} storage slots + {} trie branch keys in multiget batches of {}",
          plan.accountKeys().size(),
          plan.totalStorageKeys(),
          trieBranchKeys.size(),
          multigetBatchSize());

    } catch (final Exception e) {
      LOG.error("Error during prefetch", e);
      throw e;
    }
  }

  /**
   * Collect all account keys and, for each address, the list of storage keys (for multiget batching
   * per account).
   */
  private PrefetchPlan collectKeys(final List<BlockAccessList.AccountChanges> accounts) {
    final List<byte[]> accountKeys = new ArrayList<>();
    final List<List<byte[]>> storageKeysPerAccount = new ArrayList<>();

    for (final BlockAccessList.AccountChanges accountChanges : accounts) {
      final Address address = accountChanges.address();
      accountKeys.add(address.addressHash().getBytes().toArrayUnsafe());

      // Collect unique storage slots
      final Set<StorageSlotKey> uniqueSlots = new HashSet<>();
      accountChanges.storageChanges().forEach(sc -> uniqueSlots.add(sc.slot()));
      accountChanges.storageReads().forEach(sr -> uniqueSlots.add(sr.slot()));

      // Optionally sort storage slots
      final List<StorageSlotKey> slots =
          isSortingEnabled
              ? uniqueSlots.stream()
                  .sorted(Comparator.comparing(StorageSlotKey::getSlotHash))
                  .toList()
              : new ArrayList<>(uniqueSlots);

      final List<byte[]> storageKeysForAccount = new ArrayList<>(slots.size());
      for (var slot : slots) {
        storageKeysForAccount.add(
            Bytes.concatenate(address.addressHash().getBytes(), slot.getSlotHash().getBytes())
                .toArrayUnsafe());
      }
      if (!storageKeysForAccount.isEmpty()) {
        storageKeysPerAccount.add(storageKeysForAccount);
      }
    }

    return new PrefetchPlan(accountKeys, storageKeysPerAccount);
  }

  /**
   * Builds {@link KeyValueSegmentIdentifier#TRIE_BRANCH_STORAGE} keys: every prefix of the account
   * trie nibble-path (world trie, keyed by location only) for accounts with balance/nonce/code
   * changes, and every prefix of {@code accountHash || storagePath} for each slot in {@link
   * BlockAccessList.AccountChanges#storageChanges()} only.
   */
  private List<byte[]> collectTrieBranchKeys(
      final List<BlockAccessList.AccountChanges> accounts) {
    final Set<Bytes> unique = new HashSet<>();
    final int maxNibbles = 6;

    for (final BlockAccessList.AccountChanges ac : accounts) {
      if (hasAccountFieldChanges(ac)) {
        addTriePathPrefixes(
            unique, nibblePathWithoutLeafTerminator(ac.address().addressHash()), maxNibbles, null);
      }
      if (!ac.storageChanges().isEmpty()) {
        final Hash accountHash = ac.address().addressHash();
        final Set<StorageSlotKey> changedSlots = new HashSet<>();
        ac.storageChanges().forEach(sc -> changedSlots.add(sc.slot()));
        for (final StorageSlotKey slot : changedSlots) {
          addTriePathPrefixes(
              unique,
              nibblePathWithoutLeafTerminator(slot.getSlotHash()),
              maxNibbles,
              accountHash.getBytes());
        }
      }
    }

    final List<Bytes> sorted =
        unique.stream()
            .sorted(Comparator.comparing(Bytes::toArrayUnsafe, Arrays::compareUnsigned))
            .toList();
    return sorted.stream().map(Bytes::toArrayUnsafe).toList();
  }

  private static boolean hasAccountFieldChanges(final BlockAccessList.AccountChanges ac) {
    return !ac.balanceChanges().isEmpty()
        || !ac.nonceChanges().isEmpty()
        || !ac.codeChanges().isEmpty();
  }

  /**
   * Nibble path for trie traversal (branch locations are prefixes of this path), without the leaf
   * terminator byte.
   */
  private static Bytes nibblePathWithoutLeafTerminator(final Hash key) {
    final Bytes path = CompactEncoding.bytesToPath(key.getBytes());
    return path.slice(0, path.size() - 1);
  }

  /**
   * Adds {@code prefix} for every prefix length 0..maxNibbles inclusive. If {@code prefixBytes32}
   * is non-null (account hash), keys are {@code accountHash || prefix}.
   */
  private static void addTriePathPrefixes(
      final Set<Bytes> sink,
      final Bytes nibblePath,
      final int maxNibbles,
      final Bytes prefixBytes32) {
    final int cap = Math.min(nibblePath.size(), maxNibbles);
    for (int len = 0; len <= cap; len++) {
      final Bytes tail = nibblePath.slice(0, len);
      sink.add(
          prefixBytes32 == null ? tail : Bytes.concatenate(prefixBytes32, tail));
    }
  }

  /**
   * Fetches account data in multiget batches, then for each BAL address fetches that account's
   * storage keys in multiget batches, then trie branch keys sorted for RocksDB locality, before
   * the caller continues to state root computation.
   */
  private void fetchKeys(
      final BonsaiWorldState worldState,
      final PrefetchPlan plan,
      final List<byte[]> trieBranchKeys,
      final Executor fetchExecutor) {

    final int multigetBatch = multigetBatchSize();

    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    futures.addAll(
        fetchSegmentKeys(
            worldState,
            ACCOUNT_INFO_STATE,
            plan.accountKeys(),
            "account",
            multigetBatch,
            fetchExecutor));

    for (final List<byte[]> storageKeysOneAccount : plan.storageKeysPerAccount()) {
      futures.addAll(
          fetchSegmentKeys(
              worldState,
              ACCOUNT_STORAGE_STORAGE,
              storageKeysOneAccount,
              "storage",
              multigetBatch,
              fetchExecutor));
    }

    futures.addAll(
        fetchSegmentKeys(
            worldState,
            TRIE_BRANCH_STORAGE,
            trieBranchKeys,
            "trie_branch",
            multigetBatch,
            fetchExecutor));

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
  }

  /**
   * Fetch keys for a specific segment, with optional batching.
   *
   * @param worldState the world state
   * @param segment the segment identifier
   * @param keys the keys to fetch
   * @param segmentName human-readable segment name for logging
   * @param multigetBatchSize chunk size for each {@code getMultipleKeys} (multiget) call
   * @param fetchExecutor the executor for fetch operations
   * @return list of futures for all batch operations
   */
  private List<CompletableFuture<Void>> fetchSegmentKeys(
      final BonsaiWorldState worldState,
      final SegmentIdentifier segment,
      final List<byte[]> keys,
      final String segmentName,
      final int multigetBatchSize,
      final Executor fetchExecutor) {

    final List<CompletableFuture<Void>> futures = new ArrayList<>();

    if (keys.isEmpty()) {
      return futures;
    }

    final int batchCount = calculateBatchCount(keys.size(), multigetBatchSize);
    for (int i = 0; i < batchCount; i++) {
      final List<byte[]> batch = getBatch(keys, i, multigetBatchSize);
      final int batchNumber = i;

      futures.add(
          CompletableFuture.runAsync(
              () -> {
                final List<Optional<byte[]>> multipleKeys =
                    worldState.getWorldStateStorage().getMultipleKeys(segment, batch);
                LOG.trace(
                    "Prefetch: fetched {} batch {}/{} ({} keys)",
                    segmentName,
                    batchNumber + 1,
                    batchCount,
                    multipleKeys.size());
              },
              fetchExecutor));
    }

    LOG.debug(
        "Prefetch: fetched {} {} keys in {} multiget batches",
        keys.size(),
        segmentName,
        batchCount);

    return futures;
  }

  /** Effective multiget chunk size: explicit config, or 64 when unset. */
  private int multigetBatchSize() {
    return batchSize > 0 ? batchSize : DEFAULT_MULTIGET_PREFETCH_BATCH_SIZE;
  }

  private static int calculateBatchCount(final int totalKeys, final int multigetBatchSize) {
    return (int) Math.ceil((double) totalKeys / multigetBatchSize);
  }

  private static List<byte[]> getBatch(
      final List<byte[]> keys, final int batchIndex, final int multigetBatchSize) {
    final int start = batchIndex * multigetBatchSize;
    final int end = Math.min(start + multigetBatchSize, keys.size());
    return keys.subList(start, end);
  }

  private record PrefetchPlan(List<byte[]> accountKeys, List<List<byte[]>> storageKeysPerAccount) {
    int totalStorageKeys() {
      int n = 0;
      for (List<byte[]> keys : storageKeysPerAccount) {
        n += keys.size();
      }
      return n;
    }
  }
}
