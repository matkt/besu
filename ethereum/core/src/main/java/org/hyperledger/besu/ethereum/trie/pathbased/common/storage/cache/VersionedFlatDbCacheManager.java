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
package org.hyperledger.besu.ethereum.trie.pathbased.common.storage.cache;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration.PathBasedUnstable.DEFAULT_BONSAI_CROSS_BLOCK_CACHE_TRIE_NODE_MAX_ACCOUNTS;
import static org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration.PathBasedUnstable.DEFAULT_BONSAI_CROSS_BLOCK_CACHE_TRIE_NODE_WEIGHT;

import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Versioned cache implementation using Caffeine.
 *
 * <p>Trie nodes follow geth pathdb's clean-node cache: path-keyed (account location, or {@code
 * accountHash || location} for storage), write-through on commit, populate-on-miss only for the
 * head epoch, and eviction by RLP byte weight rather than entry count. Account-trie nodes are split
 * by nibble depth (shallow {@code 0–4} vs deep {@code 5+}). Storage-trie nodes are grouped by
 * account with a max number of accounts so a scan of cold contracts cannot evict a hot one.
 * Versioning is the Besu analogue of pathdb's disk-layer-only clean cache plus layered diffs.
 */
public class VersionedFlatDbCacheManager implements FlatDbCacheManager, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(VersionedFlatDbCacheManager.class);

  /** Default threshold of pending tasks before triggering automatic maintenance. */
  private static final int DEFAULT_DRAIN_THRESHOLD = 1000;

  /** Upper bound for Caffeine {@code initialCapacity} (must fit in a positive int). */
  private static final long MAX_INITIAL_CAPACITY = Integer.MAX_VALUE;

  /** Account-hash prefix length on storage-trie keys ({@code accountHash || location}). */
  private static final int HASHED_ADDRESS_SIZE = 32;

  /** Inclusive max nibble depth for the shallow account-trie bucket (root through depth 4). */
  private static final int SHALLOW_TRIE_MAX_DEPTH = 4;

  /** Max storage-trie nodes retained per cached account. */
  private static final long TRIE_NODES_PER_ACCOUNT = 64L;

  private final AtomicLong globalVersion = new AtomicLong(0);
  private final Cache<CacheKey, VersionedValue> accountCache;
  private final Cache<CacheKey, VersionedValue> storageCache;
  private final Cache<CacheKey, VersionedValue> trieNodeShallowCache;
  private final Cache<CacheKey, VersionedValue> trieNodeDeepCache;
  private final Cache<CacheKey, Cache<CacheKey, VersionedValue>> trieStorageNodesByAccount;
  private final ThresholdDrainExecutor drainExecutor;
  private final ExecutorService maintenanceWorker;
  private final AtomicBoolean maintenanceScheduled = new AtomicBoolean(false);

  private final Counter cacheRequestCounter;
  private final Counter cacheHitCounter;
  private final Counter cacheMissCounter;
  private final Counter cacheInsertCounter;
  private final Counter cacheRemovalCounter;

  /**
   * Creates a new VersionedFlatDbCacheManager with the default drain threshold and default trie
   * node cache weight.
   *
   * @param accountCacheSize maximum number of entries in the account cache
   * @param storageCacheSize maximum number of entries in the storage cache
   * @param metricsSystem the metrics system for instrumentation
   */
  public VersionedFlatDbCacheManager(
      final long accountCacheSize, final long storageCacheSize, final MetricsSystem metricsSystem) {
    this(
        accountCacheSize,
        storageCacheSize,
        DEFAULT_BONSAI_CROSS_BLOCK_CACHE_TRIE_NODE_WEIGHT,
        DEFAULT_BONSAI_CROSS_BLOCK_CACHE_TRIE_NODE_MAX_ACCOUNTS,
        metricsSystem,
        DEFAULT_DRAIN_THRESHOLD);
  }

  /**
   * Creates a new VersionedFlatDbCacheManager with a custom drain threshold and default trie node
   * cache weight.
   *
   * @param accountCacheSize maximum number of entries in the account cache
   * @param storageCacheSize maximum number of entries in the storage cache
   * @param metricsSystem the metrics system for instrumentation
   * @param drainThreshold number of pending maintenance tasks before automatic drain is triggered
   */
  public VersionedFlatDbCacheManager(
      final long accountCacheSize,
      final long storageCacheSize,
      final MetricsSystem metricsSystem,
      final int drainThreshold) {
    this(
        accountCacheSize,
        storageCacheSize,
        DEFAULT_BONSAI_CROSS_BLOCK_CACHE_TRIE_NODE_WEIGHT,
        DEFAULT_BONSAI_CROSS_BLOCK_CACHE_TRIE_NODE_MAX_ACCOUNTS,
        metricsSystem,
        drainThreshold);
  }

  /**
   * Creates a new VersionedFlatDbCacheManager including a weight-bounded trie node cache.
   *
   * @param accountCacheSize maximum number of entries in the account cache
   * @param storageCacheSize maximum number of entries in the storage cache
   * @param trieNodeCacheWeight maximum weight in bytes of cached trie node RLP (geth pathdb {@code
   *     TrieCleanSize} analogue)
   * @param metricsSystem the metrics system for instrumentation
   */
  public VersionedFlatDbCacheManager(
      final long accountCacheSize,
      final long storageCacheSize,
      final long trieNodeCacheWeight,
      final MetricsSystem metricsSystem) {
    this(
        accountCacheSize,
        storageCacheSize,
        trieNodeCacheWeight,
        DEFAULT_BONSAI_CROSS_BLOCK_CACHE_TRIE_NODE_MAX_ACCOUNTS,
        metricsSystem,
        DEFAULT_DRAIN_THRESHOLD);
  }

  /**
   * Creates a new VersionedFlatDbCacheManager including trie-node caches.
   *
   * @param accountCacheSize maximum number of entries in the account cache
   * @param storageCacheSize maximum number of entries in the storage cache
   * @param trieNodeCacheWeight maximum weight in bytes of account-trie node RLP
   * @param trieNodeMaxAccounts maximum accounts whose storage-trie nodes are retained
   * @param metricsSystem the metrics system for instrumentation
   */
  public VersionedFlatDbCacheManager(
      final long accountCacheSize,
      final long storageCacheSize,
      final long trieNodeCacheWeight,
      final long trieNodeMaxAccounts,
      final MetricsSystem metricsSystem) {
    this(
        accountCacheSize,
        storageCacheSize,
        trieNodeCacheWeight,
        trieNodeMaxAccounts,
        metricsSystem,
        DEFAULT_DRAIN_THRESHOLD);
  }

  /**
   * Creates a new VersionedFlatDbCacheManager with a custom drain threshold.
   *
   * @param accountCacheSize maximum number of entries in the account cache
   * @param storageCacheSize maximum number of entries in the storage cache
   * @param trieNodeCacheWeight maximum weight in bytes of cached trie node RLP
   * @param metricsSystem the metrics system for instrumentation
   * @param drainThreshold number of pending maintenance tasks before automatic drain is triggered
   */
  public VersionedFlatDbCacheManager(
      final long accountCacheSize,
      final long storageCacheSize,
      final long trieNodeCacheWeight,
      final MetricsSystem metricsSystem,
      final int drainThreshold) {
    this(
        accountCacheSize,
        storageCacheSize,
        trieNodeCacheWeight,
        DEFAULT_BONSAI_CROSS_BLOCK_CACHE_TRIE_NODE_MAX_ACCOUNTS,
        metricsSystem,
        drainThreshold);
  }

  /**
   * Creates a new VersionedFlatDbCacheManager with a custom drain threshold.
   *
   * @param accountCacheSize maximum number of entries in the account cache
   * @param storageCacheSize maximum number of entries in the storage cache
   * @param trieNodeCacheWeight maximum weight in bytes of account-trie node RLP
   * @param trieNodeMaxAccounts maximum accounts whose storage-trie nodes are retained
   * @param metricsSystem the metrics system for instrumentation
   * @param drainThreshold number of pending maintenance tasks before automatic drain is triggered
   */
  public VersionedFlatDbCacheManager(
      final long accountCacheSize,
      final long storageCacheSize,
      final long trieNodeCacheWeight,
      final long trieNodeMaxAccounts,
      final MetricsSystem metricsSystem,
      final int drainThreshold) {

    requirePositiveCacheMaxSize("accountCacheSize", accountCacheSize);
    requirePositiveCacheMaxSize("storageCacheSize", storageCacheSize);
    requirePositiveCacheMaxSize("trieNodeCacheWeight", trieNodeCacheWeight);
    requirePositiveCacheMaxSize("trieNodeMaxAccounts", trieNodeMaxAccounts);

    this.maintenanceWorker =
        Executors.newSingleThreadExecutor(
            r -> {
              final Thread t = new Thread(r, "cache-maintenance");
              t.setDaemon(true);
              return t;
            });

    this.drainExecutor = new ThresholdDrainExecutor(drainThreshold, this::scheduleAsyncMaintenance);

    this.accountCache = createCache(accountCacheSize);
    this.storageCache = createCache(storageCacheSize);
    final long shallowWeight = Math.max(1L, trieNodeCacheWeight / 4);
    final long deepWeight = Math.max(1L, trieNodeCacheWeight - shallowWeight);
    this.trieNodeShallowCache = createWeightedCache(shallowWeight);
    this.trieNodeDeepCache = createWeightedCache(deepWeight);
    this.trieStorageNodesByAccount =
        Caffeine.newBuilder()
            .initialCapacity(initialCapacityFor(trieNodeMaxAccounts))
            .maximumSize(trieNodeMaxAccounts)
            .executor(drainExecutor)
            .build();

    this.cacheRequestCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_cache_requests_total",
            "Total number of cache requests");

    this.cacheHitCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN, "bonsai_cache_hits_total", "Total number of cache hits");

    this.cacheMissCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_cache_misses_total",
            "Total number of cache misses");

    this.cacheInsertCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_cache_inserts_total",
            "Total number of cache insertions");

    this.cacheRemovalCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_cache_removals_total",
            "Total number of cache removals");

    LOG.info(
        "Cache maintenance will trigger asynchronously after {} pending tasks; trie node cache"
            + " shallow={} bytes (account depth 0-{}), deep={} bytes, storage by account max={}",
        drainThreshold,
        shallowWeight,
        SHALLOW_TRIE_MAX_DEPTH,
        deepWeight,
        trieNodeMaxAccounts);
  }

  private Cache<CacheKey, VersionedValue> createCache(final long maxSize) {
    return Caffeine.newBuilder()
        .initialCapacity(initialCapacityFor(maxSize))
        .maximumSize(maxSize)
        .executor(drainExecutor)
        .build();
  }

  /**
   * Weight-bounded cache for path-keyed trie nodes. Geth pathdb's {@code fastcache} evicts by
   * serialized bytes; Caffeine {@code maximumWeight} is the on-heap equivalent so a 17-child branch
   * does not count the same as a one-byte leaf.
   */
  private Cache<CacheKey, VersionedValue> createWeightedCache(final long maxWeightBytes) {
    return Caffeine.newBuilder()
        .initialCapacity(initialCapacityFor(Math.max(1L, maxWeightBytes / 256)))
        .maximumWeight(maxWeightBytes)
        .weigher(
            (Weigher<CacheKey, VersionedValue>)
                (key, value) -> {
                  long weight = key.size();
                  final Bytes cached = value.getValue();
                  if (cached != null) {
                    weight += cached.size();
                  }
                  return (int) Math.min(Integer.MAX_VALUE, Math.max(1L, weight));
                })
        .executor(drainExecutor)
        .build();
  }

  /**
   * Initial capacity ~10% of {@code maxSize}, at least 1, never exceeding {@link
   * Integer#MAX_VALUE}.
   */
  private static int initialCapacityFor(final long maxSize) {
    final long tenth = maxSize / 10;
    final long capped = Math.min(MAX_INITIAL_CAPACITY, tenth);
    return (int) Math.max(1L, capped);
  }

  private static void requirePositiveCacheMaxSize(final String name, final long maxSize) {
    if (maxSize <= 0) {
      throw new IllegalArgumentException(name + " must be positive, got " + maxSize);
    }
  }

  private Cache<CacheKey, VersionedValue> cacheFor(
      final SegmentIdentifier segment, final Bytes key) {
    if (segment == ACCOUNT_INFO_STATE) {
      return accountCache;
    }
    if (segment == ACCOUNT_STORAGE_STORAGE) {
      return storageCache;
    }
    if (segment == TRIE_BRANCH_STORAGE) {
      if (key.size() >= HASHED_ADDRESS_SIZE) {
        return storageTrieNodeCacheFor(key);
      }
      return isShallowTrieKey(key) ? trieNodeShallowCache : trieNodeDeepCache;
    }
    return null;
  }

  private Cache<CacheKey, VersionedValue> storageTrieNodeCacheFor(final Bytes key) {
    return trieStorageNodesByAccount.get(
        CacheKey.of(key.slice(0, HASHED_ADDRESS_SIZE)),
        ignored -> createCache(TRIE_NODES_PER_ACCOUNT));
  }

  /**
   * Nibble depth of an account-trie location (one byte per nibble). Storage keys are not measured
   * here; they are routed per account.
   */
  static int trieNibbleDepth(final Bytes key) {
    return key.size();
  }

  static boolean isShallowTrieKey(final Bytes key) {
    return key.size() < HASHED_ADDRESS_SIZE && key.size() <= SHALLOW_TRIE_MAX_DEPTH;
  }

  /**
   * Schedules an async maintenance if one is not already scheduled. Uses an AtomicBoolean to
   * prevent flooding the maintenance worker with redundant tasks.
   */
  @Override
  public void scheduleAsyncMaintenance() {
    if (maintenanceScheduled.compareAndSet(false, true)) {
      try {
        maintenanceWorker.execute(
            () -> {
              try {
                doMaintenance();
              } finally {
                maintenanceScheduled.set(false);
              }
            });
      } catch (final Exception e) {
        maintenanceScheduled.set(false);
        LOG.warn("Failed to schedule async cache maintenance", e);
      }
    }
  }

  /** Performs the actual maintenance work: drains pending tasks and runs Caffeine's cleanUp. */
  private void doMaintenance() {
    try {
      final int drained = drainExecutor.drain();
      accountCache.cleanUp();
      storageCache.cleanUp();
      trieNodeShallowCache.cleanUp();
      trieNodeDeepCache.cleanUp();
      trieStorageNodesByAccount.cleanUp();
      if (drained > 0) {
        LOG.trace("Cache maintenance drained {} tasks", drained);
      }
    } catch (final Exception e) {
      LOG.warn("Error during cache maintenance", e);
    }
  }

  /**
   * Shuts down the maintenance worker. Should be called when the cache manager is no longer needed.
   */
  @Override
  public void close() {
    LOG.info("Shutting down cache maintenance worker");
    maintenanceWorker.shutdown();
    try {
      if (!maintenanceWorker.awaitTermination(5, TimeUnit.SECONDS)) {
        maintenanceWorker.shutdownNow();
      }
    } catch (final InterruptedException e) {
      maintenanceWorker.shutdownNow();
      Thread.currentThread().interrupt();
    }
    // Final synchronous drain to process any remaining tasks
    doMaintenance();
  }

  @Override
  public long getCurrentVersion() {
    return globalVersion.get();
  }

  @Override
  public long incrementAndGetVersion() {
    return globalVersion.incrementAndGet();
  }

  @Override
  public void clear(final SegmentIdentifier segment) {
    if (segment == TRIE_BRANCH_STORAGE) {
      trieNodeShallowCache.invalidateAll();
      trieNodeDeepCache.invalidateAll();
      trieStorageNodesByAccount.invalidateAll();
      return;
    }
    final Cache<CacheKey, VersionedValue> cache = cacheFor(segment, Bytes.EMPTY);
    if (cache != null) {
      cache.invalidateAll();
    }
  }

  @Override
  public Optional<Bytes> getFromCacheOrStorage(
      final SegmentIdentifier segment,
      final Bytes key,
      final long version,
      final Supplier<Optional<Bytes>> storageGetter) {

    final Cache<CacheKey, VersionedValue> cache = cacheFor(segment, key);

    cacheRequestCounter.inc();

    if (cache == null) {
      cacheMissCounter.inc();
      return storageGetter.get();
    }

    final CacheKey cacheKey = CacheKey.of(key);
    final VersionedValue versionedValue = cache.getIfPresent(cacheKey);

    if (versionedValue != null && versionedValue.version <= version) {
      cacheHitCounter.inc();
      return versionedValue.isRemoval ? Optional.empty() : Optional.of(versionedValue.getValue());
    }

    cacheMissCounter.inc();
    final Optional<Bytes> result = storageGetter.get();

    if (version == globalVersion.get() && shouldInsertReadMiss(segment, result)) {
      cacheInsertCounter.inc();
      final Bytes valueToCache = result.orElse(null);
      final boolean isRemoval = result.isEmpty();

      cache
          .asMap()
          .compute(
              cacheKey,
              (k, existingValue) -> {
                if (existingValue == null || existingValue.version < version) {
                  return new VersionedValue(valueToCache, version, isRemoval);
                }
                return existingValue;
              });
    }

    return result;
  }

  @Override
  public List<Optional<Bytes>> getMultipleFromCacheOrStorage(
      final SegmentIdentifier segment,
      final List<Bytes> keys,
      final long version,
      final Function<List<Bytes>, List<Optional<Bytes>>> batchFetcher) {

    if (keys.isEmpty()) {
      return List.of();
    }
    if (segment != ACCOUNT_INFO_STATE
        && segment != ACCOUNT_STORAGE_STORAGE
        && segment != TRIE_BRANCH_STORAGE) {
      keys.forEach(k -> cacheMissCounter.inc());
      return batchFetcher.apply(keys);
    }

    final List<Optional<Bytes>> results = new ArrayList<>(keys.size());
    final List<Bytes> keysToFetch = new ArrayList<>();
    final List<Integer> indicesToFetch = new ArrayList<>();

    for (int i = 0; i < keys.size(); i++) {
      final Bytes key = keys.get(i);
      cacheRequestCounter.inc();

      final Cache<CacheKey, VersionedValue> cache = cacheFor(segment, key);
      final CacheKey cacheKey = CacheKey.of(key);
      final VersionedValue versionedValue = cache == null ? null : cache.getIfPresent(cacheKey);

      if (versionedValue != null && versionedValue.version <= version) {
        cacheHitCounter.inc();
        results.add(
            versionedValue.isRemoval ? Optional.empty() : Optional.of(versionedValue.getValue()));
      } else {
        cacheMissCounter.inc();
        results.add(null);
        keysToFetch.add(key);
        indicesToFetch.add(i);
      }
    }

    if (!keysToFetch.isEmpty()) {
      final List<Optional<Bytes>> fetchedValues = batchFetcher.apply(keysToFetch);
      final boolean shouldUpdateCache = version == globalVersion.get();

      for (int i = 0; i < fetchedValues.size(); i++) {
        final Optional<Bytes> fetchedValue = fetchedValues.get(i);
        final int resultIndex = indicesToFetch.get(i);
        final Bytes key = keysToFetch.get(i);

        results.set(resultIndex, fetchedValue);

        if (shouldUpdateCache && shouldInsertReadMiss(segment, fetchedValue)) {
          cacheInsertCounter.inc();
          final CacheKey cacheKey = CacheKey.of(key);
          final Bytes valueToCache = fetchedValue.orElse(null);
          final boolean isRemoval = fetchedValue.isEmpty();
          final Cache<CacheKey, VersionedValue> cache = cacheFor(segment, key);

          cache
              .asMap()
              .compute(
                  cacheKey,
                  (k, existingValue) -> {
                    if (existingValue == null || existingValue.version < version) {
                      return new VersionedValue(valueToCache, version, isRemoval);
                    }
                    return existingValue;
                  });
        }
      }
    }

    return results;
  }

  @Override
  public void putInCache(
      final SegmentIdentifier segment, final Bytes key, final Bytes value, final long version) {
    final Cache<CacheKey, VersionedValue> cache = cacheFor(segment, key);
    if (cache != null) {
      final CacheKey cacheKey = CacheKey.of(key);
      cache
          .asMap()
          .compute(
              cacheKey,
              (k, existingValue) -> {
                if (existingValue == null || existingValue.version < version) {
                  cacheInsertCounter.inc();
                  return new VersionedValue(value, version, false);
                }
                return existingValue;
              });
    }
  }

  @Override
  public void removeFromCache(
      final SegmentIdentifier segment, final Bytes key, final long version) {
    final Cache<CacheKey, VersionedValue> cache = cacheFor(segment, key);
    if (cache != null) {
      final CacheKey cacheKey = CacheKey.of(key);
      cache
          .asMap()
          .compute(
              cacheKey,
              (k, existingValue) -> {
                if (existingValue == null || existingValue.version < version) {
                  cacheRemovalCounter.inc();
                  // Geth pathdb `fastcache.Del`: drop the path instead of storing a tombstone so
                  // missing trie nodes do not consume the byte-weight budget.
                  if (segment == TRIE_BRANCH_STORAGE) {
                    return null;
                  }
                  return new VersionedValue(null, version, true);
                }
                return existingValue;
              });
    }
  }

  /**
   * Geth pathdb only inserts present trie blobs into the clean node cache ({@code len(blob) > 0}).
   * Account/storage still cache absences (geth state cache does the same).
   */
  private static boolean shouldInsertReadMiss(
      final SegmentIdentifier segment, final Optional<Bytes> result) {
    return segment != TRIE_BRANCH_STORAGE || result.filter(b -> !b.isEmpty()).isPresent();
  }

  @Override
  public long getCacheSize(final SegmentIdentifier segment) {
    if (segment == TRIE_BRANCH_STORAGE) {
      long size = trieNodeShallowCache.estimatedSize() + trieNodeDeepCache.estimatedSize();
      for (final Cache<CacheKey, VersionedValue> perAccount :
          trieStorageNodesByAccount.asMap().values()) {
        size += perAccount.estimatedSize();
      }
      return size;
    }
    final Cache<CacheKey, VersionedValue> cache = cacheFor(segment, Bytes.EMPTY);
    return cache != null ? cache.estimatedSize() : 0;
  }

  @Override
  public boolean isCached(final SegmentIdentifier segment, final Bytes key) {
    final Cache<CacheKey, VersionedValue> cache = cacheFor(segment, key);
    return cache != null && cache.getIfPresent(CacheKey.of(key)) != null;
  }

  @Override
  public Optional<VersionedValue> getCachedValue(final SegmentIdentifier segment, final Bytes key) {
    final Cache<CacheKey, VersionedValue> cache = cacheFor(segment, key);
    return cache != null
        ? Optional.ofNullable(cache.getIfPresent(CacheKey.of(key)))
        : Optional.empty();
  }

  /**
   * An executor that queues maintenance tasks instead of running them immediately. This prevents
   * Caffeine's scheduleDrainBuffers from impacting read/write performance. When the number of
   * pending tasks exceeds a configurable threshold, an async maintenance is submitted.
   */
  private static class ThresholdDrainExecutor implements java.util.concurrent.Executor {
    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pendingCount = new AtomicInteger(0);
    private final int drainThreshold;
    private final Runnable onThresholdReached;

    ThresholdDrainExecutor(final int drainThreshold, final Runnable onThresholdReached) {
      this.drainThreshold = drainThreshold;
      this.onThresholdReached = onThresholdReached;
    }

    @Override
    public void execute(final Runnable command) {
      tasks.add(command);
      if (pendingCount.incrementAndGet() >= drainThreshold) {
        onThresholdReached.run();
      }
    }

    /**
     * Execute all pending maintenance tasks.
     *
     * @return the number of tasks that were drained
     */
    public int drain() {
      int drained = 0;
      Runnable task;
      while ((task = tasks.poll()) != null) {
        task.run();
        drained++;
      }
      pendingCount.addAndGet(-drained);
      return drained;
    }
  }
}
