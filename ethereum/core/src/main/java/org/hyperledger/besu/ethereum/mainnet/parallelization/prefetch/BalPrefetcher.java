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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.evm.Code;
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
 * <p>Pipeline: (1) account + storage keys on the IO executor, (2) flat contract bytecode load on
 * the IO executor, (3) jump-dest analysis into the analyzed-code cache on the CPU executor. {@link
 * BonsaiWorldStateKeyValueStorage#getCacheManager()} is always non-null ({@code NO_OP} when
 * cross-block caching is disabled).
 */
@SuppressWarnings("rawtypes")
public class BalPrefetcher {

  private static final Logger LOG = LoggerFactory.getLogger(BalPrefetcher.class);

  private static final Comparator<byte[]> STORAGE_KEY_COMPARATOR = Arrays::compareUnsigned;
  private final boolean isSortingEnabled;
  private final int batchSize;

  /**
   * Creates a new prefetch mechanism.
   *
   * @param isSortingEnabled whether to sort keys before prefetching (may improve DB locality)
   * @param batchSize the batch size for prefetch operations (0 or negative = no batching, fetch all
   *     at once)
   */
  public BalPrefetcher(final boolean isSortingEnabled, final int batchSize) {
    this.isSortingEnabled = isSortingEnabled;
    this.batchSize = batchSize;
  }

  /**
   * Prefetch world state data based on the block access list.
   *
   * <p>Stages: collect keys → fetch accounts/slots (IO) → load flat code (IO) → jump-dest analyze
   * (CPU).
   *
   * @param worldState the world state to prefetch data into
   * @param blockAccessList the block access list containing read operations
   * @param ioExecutor executor for RocksDB / flat-db reads (accounts, slots, code bytes)
   * @param cpuExecutor executor for jump-dest analysis into the analyzed-code cache
   * @return a completable future that completes when prefetching is done
   */
  public CompletableFuture<Void> prefetch(
      final BonsaiWorldState worldState,
      final BlockAccessList blockAccessList,
      final Executor ioExecutor,
      final Executor cpuExecutor) {

    return CompletableFuture.supplyAsync(
            () -> {
              worldState.disableCacheMerkleTrieLoader();
              // Always NO_OP (never null) when cross-block cache is disabled.
              final FlatDbCacheManager cacheManager =
                  worldState.getWorldStateStorage().getCacheManager();
              cacheManager.expandCachesForBlock();
              LOG.info(
                  "Prefetch code: cacheSizeBefore={}", cacheManager.getCodeCacheSize());

              // Collect and optionally sort account changes
              final List<BlockAccessList.AccountChanges> accounts =
                  isSortingEnabled
                      ? blockAccessList.accountChanges().stream()
                          .sorted(Comparator.comparing(ac -> ac.address().addressHash().getBytes()))
                          .toList()
                      : new ArrayList<>(blockAccessList.accountChanges());

              // Collect all keys to prefetch
              final PrefetchKeys keys = collectKeys(accounts);

              LOG.debug(
                  "Prefetch: collected {} account keys and {} storage keys",
                  keys.accountKeys.size(),
                  keys.storageKeys.size());

              return keys;
            },
            ioExecutor)
        .thenCompose(keys -> fetchAccountAndStorageAsync(worldState, keys, ioExecutor))
        .thenCompose(keys -> loadCodeBytesAsync(worldState, keys, ioExecutor))
        .thenCompose(pending -> analyzeJumpDestAsync(worldState, pending, cpuExecutor))
        .whenComplete(
            (result, ex) -> {
              if (ex != null) {
                LOG.error("Error during prefetch", ex);
              } else {
                LOG.info("Prefetch completed (accounts/slots → code IO → jump-dest CPU)");
              }
            });
  }

  /** Collect all account and storage keys from the block access list. */
  private PrefetchKeys collectKeys(final List<BlockAccessList.AccountChanges> accounts) {
    final List<byte[]> accountKeys = new ArrayList<>(accounts.size());
    final List<byte[]> storageKeys = new ArrayList<>();
    final List<Hash> accountHashes = new ArrayList<>(accounts.size());
    for (final BlockAccessList.AccountChanges accountChanges : accounts) {
      final Address address = accountChanges.address();
      final Hash addressHash = address.addressHash();
      final byte[] addressHashBytes = addressHash.getBytes().toArrayUnsafe();
      accountKeys.add(addressHashBytes);
      accountHashes.add(addressHash);
      final List<BlockAccessList.SlotChanges> storageChanges = accountChanges.storageChanges();
      final List<BlockAccessList.SlotRead> storageReads = accountChanges.storageReads();
      final int rawSlotCount = storageChanges.size() + storageReads.size();
      if (rawSlotCount == 0) {
        continue;
      }
      // Deduplicate storage slots by hash without streams/lambdas (plain iterator loops).
      final Set<StorageSlotKey> uniqueSlots = HashSet.newHashSet(rawSlotCount);
      for (final BlockAccessList.SlotChanges storageChange : storageChanges) {
        uniqueSlots.add(storageChange.slot());
      }
      for (final BlockAccessList.SlotRead storageRead : storageReads) {
        uniqueSlots.add(storageRead.slot());
      }
      final int rangeStart = storageKeys.size();
      for (final StorageSlotKey slot : uniqueSlots) {
        final byte[] slotHash = slot.getSlotHash().getBytes().toArrayUnsafe();
        final byte[] storageKey = new byte[addressHashBytes.length + slotHash.length];
        System.arraycopy(addressHashBytes, 0, storageKey, 0, addressHashBytes.length);
        System.arraycopy(slotHash, 0, storageKey, addressHashBytes.length, slotHash.length);
        storageKeys.add(storageKey);
      }
      if (isSortingEnabled) {
        storageKeys.subList(rangeStart, storageKeys.size()).sort(STORAGE_KEY_COMPARATOR);
      }
    }
    return new PrefetchKeys(
        accountKeys, storageKeys, accountHashes, new ArrayList<>(accounts.size()));
  }

  /**
   * Fetch accounts and storage slots on the IO executor. Account RLPs are retained for the code
   * stage.
   *
   * @return the same {@link PrefetchKeys} once account/storage IO is done
   */
  private CompletableFuture<PrefetchKeys> fetchAccountAndStorageAsync(
      final BonsaiWorldState worldState, final PrefetchKeys keys, final Executor ioExecutor) {

    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    futures.add(fetchAccountKeysAsync(worldState, keys, ioExecutor));

    if (!keys.storageKeys.isEmpty()) {
      futures.addAll(
          fetchSegmentKeys(
              worldState, ACCOUNT_STORAGE_STORAGE, keys.storageKeys, "storage", ioExecutor));
    }

    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
        .thenApply(
            ignored -> {
              LOG.debug(
                  "Prefetch: accounts/slots done ({} accounts, {} slots{})",
                  keys.accountKeys.size(),
                  keys.storageKeys.size(),
                  shouldBatch() ? ", batches of " + batchSize : "");
              return keys;
            });
  }

  /**
   * Prefetch account info keys and retain returned RLPs aligned with {@link
   * PrefetchKeys#accountHashes}.
   */
  private CompletableFuture<Void> fetchAccountKeysAsync(
      final BonsaiWorldState worldState, final PrefetchKeys keys, final Executor ioExecutor) {
    if (keys.accountKeys.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    if (!shouldBatch()) {
      return CompletableFuture.runAsync(
          () -> {
            final List<Optional<Bytes>> rlps =
                prefetchKeys(worldState, ACCOUNT_INFO_STATE, keys.accountKeys);
            keys.accountRlps.addAll(rlps);
            LOG.debug("Prefetch: fetched {} account keys in single batch", keys.accountKeys.size());
          },
          ioExecutor);
    }

    final int size = keys.accountKeys.size();
    @SuppressWarnings("unchecked")
    final Optional<Bytes>[] rlps = (Optional<Bytes>[]) new Optional<?>[size];
    final int batchCount = calculateBatchCount(size);
    final List<CompletableFuture<Void>> batchFutures = new ArrayList<>(batchCount);
    for (int i = 0; i < batchCount; i++) {
      final int batchIndex = i;
      final List<byte[]> batch = getBatch(keys.accountKeys, batchIndex);
      final int start = batchIndex * batchSize;
      batchFutures.add(
          CompletableFuture.runAsync(
              () -> {
                final List<Optional<Bytes>> batchRlps =
                    prefetchKeys(worldState, ACCOUNT_INFO_STATE, batch);
                for (int j = 0; j < batchRlps.size(); j++) {
                  rlps[start + j] = batchRlps.get(j);
                }
                LOG.trace(
                    "Prefetch: fetched account batch {}/{} ({} keys)",
                    batchIndex + 1,
                    batchCount,
                    batch.size());
              },
              ioExecutor));
    }

    LOG.debug("Prefetch: fetched {} account keys in {} batches", size, batchCount);
    return CompletableFuture.allOf(batchFutures.toArray(CompletableFuture[]::new))
        .thenRun(
            () -> {
              for (final Optional<Bytes> rlp : rlps) {
                keys.accountRlps.add(rlp != null ? rlp : Optional.empty());
              }
            });
  }

  /**
   * IO stage: parse code hashes from retained account RLPs and batch-load flat {@code CODE_STORAGE}
   * bytes. Does not run jump-dest analysis.
   */
  private CompletableFuture<List<PendingCode>> loadCodeBytesAsync(
      final BonsaiWorldState worldState, final PrefetchKeys keys, final Executor ioExecutor) {
    return CompletableFuture.supplyAsync(
        () -> {
          final BonsaiWorldStateKeyValueStorage storage = worldState.getWorldStateStorage();
          final FlatDbCacheManager cacheManager = storage.getCacheManager();
          final boolean codeByHash = storage.getFlatDbStrategy().isCodeByCodeHash();

          final List<Hash> codeHashes = new ArrayList<>();
          final List<Hash> accountHashesForCode = new ArrayList<>();
          final List<byte[]> flatKeys = new ArrayList<>();
          int contracts = 0;
          int alreadyCached = 0;

          for (int i = 0; i < keys.accountHashes.size(); i++) {
            final Hash accountHash = keys.accountHashes.get(i);
            final Optional<Bytes> accountRlp = keys.accountRlps.get(i);
            if (accountRlp.isEmpty()) {
              continue;
            }
            final Hash codeHash;
            try {
              codeHash =
                  PmtStateTrieAccountValue.readFrom(RLP.input(accountRlp.get())).getCodeHash();
            } catch (final RuntimeException e) {
              LOG.trace("Prefetch: skipping unparseable account {}", accountHash, e);
              continue;
            }
            if (codeHash.equals(Hash.EMPTY)) {
              continue;
            }
            contracts++;
            // Skip if already warmed in analyzed-code cache (NO_OP returns null → proceed).
            if (cacheManager.getIfPresent(codeHash) != null) {
              alreadyCached++;
              continue;
            }
            codeHashes.add(codeHash);
            accountHashesForCode.add(accountHash);
            flatKeys.add((codeByHash ? codeHash : accountHash).getBytes().toArrayUnsafe());
          }

          LOG.info(
              "Prefetch code: contracts={}, alreadyCached={}, toLoad={}, cacheSize={}",
              contracts,
              alreadyCached,
              flatKeys.size(),
              cacheManager.getCodeCacheSize());

          if (flatKeys.isEmpty()) {
            LOG.debug("Prefetch: no contract code bytes to load");
            return List.of();
          }

          final List<Optional<Bytes>> flats = prefetchKeys(worldState, CODE_STORAGE, flatKeys);
          final List<PendingCode> pending = new ArrayList<>(flats.size());
          for (int i = 0; i < flats.size(); i++) {
            final Optional<Bytes> flat = flats.get(i);
            final Hash codeHash = codeHashes.get(i);
            final Hash accountHash = accountHashesForCode.get(i);
            if (flat.isPresent()
                && !flat.get().isEmpty()
                && (codeByHash || Hash.hash(flat.get()).equals(codeHash))) {
              pending.add(new PendingCode(codeHash, flat.get()));
            } else if (!codeByHash) {
              // Account-hash strategy miss / filter mismatch: fall back to getCode (IO + analyze).
              storage.getCode(codeHash, accountHash);
            }
          }
          LOG.debug("Prefetch: loaded {} flat code entries (IO)", pending.size());
          return pending;
        },
        ioExecutor);
  }

  /**
   * CPU stage: build {@link Code}, run jump-dest analysis, put into the shared analyzed-code cache.
   */
  private CompletableFuture<Void> analyzeJumpDestAsync(
      final BonsaiWorldState worldState,
      final List<PendingCode> pending,
      final Executor cpuExecutor) {
    if (pending.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return CompletableFuture.runAsync(
        () -> {
          final FlatDbCacheManager cacheManager =
              worldState.getWorldStateStorage().getCacheManager();
          int addedToCache = 0;
          int skippedAlreadyCached = 0;
          long jumpDestNs = 0L;
          for (final PendingCode entry : pending) {
            if (cacheManager.getIfPresent(entry.codeHash()) != null) {
              skippedAlreadyCached++;
              continue;
            }
            final Code code = new Code(entry.bytecode(), entry.codeHash());
            final long t0 = System.nanoTime();
            code.ensureJumpDestAnalyzed();
            jumpDestNs += System.nanoTime() - t0;
            cacheManager.put(entry.codeHash(), code);
            addedToCache++;
          }
          LOG.info(
              "Prefetch code: addedToCache={} skippedAlreadyCached={} jumpDestUs={} thread={}",
              addedToCache,
              skippedAlreadyCached,
              jumpDestNs / 1_000,
              Thread.currentThread().getName());
          LOG.debug("Prefetch: jump-dest analyzed {} contract code entries (CPU)", addedToCache);
        },
        cpuExecutor);
  }

  /**
   * Fetch keys for a specific segment, with optional batching.
   *
   * @param worldState the world state
   * @param segment the segment identifier
   * @param keys the keys to fetch
   * @param segmentName human-readable segment name for logging
   * @param ioExecutor the executor for IO fetch operations
   * @return list of futures for all batch operations
   */
  private List<CompletableFuture<Void>> fetchSegmentKeys(
      final BonsaiWorldState worldState,
      final SegmentIdentifier segment,
      final List<byte[]> keys,
      final String segmentName,
      final Executor ioExecutor) {

    final List<CompletableFuture<Void>> futures = new ArrayList<>();

    if (!shouldBatch()) {
      futures.add(
          CompletableFuture.runAsync(
              () -> {
                prefetchKeys(worldState, segment, keys);
                LOG.debug("Prefetch: fetched {} {} keys in single batch", keys.size(), segmentName);
              },
              ioExecutor));
    } else {
      final int batchCount = calculateBatchCount(keys.size());
      for (int i = 0; i < batchCount; i++) {
        final List<byte[]> batch = getBatch(keys, i);
        final int batchNumber = i;

        futures.add(
            CompletableFuture.runAsync(
                () -> {
                  prefetchKeys(worldState, segment, batch);
                  LOG.trace(
                      "Prefetch: fetched {} batch {}/{} ({} keys)",
                      segmentName,
                      batchNumber + 1,
                      batchCount,
                      batch.size());
                },
                ioExecutor));
      }

      LOG.debug("Prefetch: fetched {} {} keys in {} batches", keys.size(), segmentName, batchCount);
    }

    return futures;
  }

  private List<Optional<Bytes>> prefetchKeys(
      final BonsaiWorldState worldState, final SegmentIdentifier segment, final List<byte[]> keys) {
    // Go through BonsaiWorldStateKeyValueStorage so reads populate VersionedFlatDbCacheManager.
    return worldState.getWorldStateStorage().getMultipleKeys(segment, keys);
  }

  private boolean shouldBatch() {
    return batchSize > 0;
  }

  private int calculateBatchCount(final int totalKeys) {
    return (int) Math.ceil((double) totalKeys / batchSize);
  }

  private List<byte[]> getBatch(final List<byte[]> keys, final int batchIndex) {
    final int start = batchIndex * batchSize;
    final int end = Math.min(start + batchSize, keys.size());
    return keys.subList(start, end);
  }

  /** Flat bytecode loaded on the IO stage, awaiting jump-dest analysis on the CPU stage. */
  private record PendingCode(Hash codeHash, Bytes bytecode) {}

  /**
   * Container for collected prefetch keys. {@code accountRlps} is filled by account {@code
   * getMultipleKeys} and is aligned with {@code accountKeys}/{@code accountHashes}.
   */
  private record PrefetchKeys(
      List<byte[]> accountKeys,
      List<byte[]> storageKeys,
      List<Hash> accountHashes,
      List<Optional<Bytes>> accountRlps) {}
}
