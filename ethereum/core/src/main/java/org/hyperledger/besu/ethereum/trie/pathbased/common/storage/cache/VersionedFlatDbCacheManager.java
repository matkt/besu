/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Versioned cache implementation using Caffeine.
 *
 * Trie node focus:
 * - visible logs
 * - hit/miss by shallow/deep/account and finer depth buckets
 * - per-account weighted cache
 * - stricter admission for cold trie nodes
 * - shallower nodes favored more than deeper ones
 */
public class VersionedFlatDbCacheManager implements FlatDbCacheManager, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(VersionedFlatDbCacheManager.class);

  private static final int DEFAULT_DRAIN_THRESHOLD = 1000;
  private static final long MAX_INITIAL_CAPACITY = Integer.MAX_VALUE;
  private static final int HASHED_ADDRESS_SIZE = 32;
  private static final int SHALLOW_TRIE_MAX_DEPTH = 4;

  /** More aggressive shallow budget because shallow nodes are hotter on your workload. */
  private static final int SHALLOW_WEIGHT_PERCENT = 40;

  /** Admission size cutoff for trie nodes. */
  private static final int ADMISSION_MAX_NODE_SIZE_BYTES = 16 * 1024;

  /** Extra stricter cutoff for deep / account nodes. */
  private static final int ADMISSION_MAX_DEEP_NODE_SIZE_BYTES = 8 * 1024;

  /** Node-size buckets for logs. */
  private static final int SMALL_NODE_LIMIT = 64;
  private static final int MEDIUM_NODE_LIMIT = 256;
  private static final int LARGE_NODE_LIMIT = 1024;
  private static final int HUGE_NODE_LIMIT = 4096;

  /** Per-account cache weight budget. */
  private static final long PER_ACCOUNT_TRIE_WEIGHT_BYTES = 64 * 1024L;

  private final AtomicLong globalVersion = new AtomicLong(0);

  private final Cache<CacheKey, VersionedValue> accountCache;
  private final Cache<CacheKey, VersionedValue> storageCache;
  private final Cache<CacheKey, VersionedValue> trieNodeShallowCache;
  private final Cache<CacheKey, VersionedValue> trieNodeDeepCache;
  private final Cache<CacheKey, Cache<CacheKey, VersionedValue>> trieStorageNodesByAccount;
  private final ConcurrentMap<CacheKey, AtomicLong> trieStorageWeightByAccount = new ConcurrentHashMap<>();
  private final ConcurrentMap<CacheKey, AccountTrieStats> accountTrieStats = new ConcurrentHashMap<>();

  private final ThresholdDrainExecutor drainExecutor;
  private final ExecutorService maintenanceWorker;
  private final AtomicBoolean maintenanceScheduled = new AtomicBoolean(false);

  private final ScheduledExecutorService trieStatsLogger;

  private final Counter cacheRequestCounter;
  private final Counter cacheHitCounter;
  private final Counter cacheMissCounter;
  private final Counter cacheInsertCounter;
  private final Counter cacheRemovalCounter;

  private final AtomicLong trieRequestsTotal = new AtomicLong();
  private final AtomicLong trieHitsTotal = new AtomicLong();
  private final AtomicLong trieMissesTotal = new AtomicLong();

  private final AtomicLong trieHitsShallow = new AtomicLong();
  private final AtomicLong trieHitsDeep = new AtomicLong();
  private final AtomicLong trieHitsAccount = new AtomicLong();

  private final AtomicLong trieMissesShallow = new AtomicLong();
  private final AtomicLong trieMissesDeep = new AtomicLong();
  private final AtomicLong trieMissesAccount = new AtomicLong();

  /** Finer depth buckets. */
  private final AtomicLong depth0to2Hits = new AtomicLong();
  private final AtomicLong depth0to2Misses = new AtomicLong();
  private final AtomicLong depth3to4Hits = new AtomicLong();
  private final AtomicLong depth3to4Misses = new AtomicLong();
  private final AtomicLong depth5to8Hits = new AtomicLong();
  private final AtomicLong depth5to8Misses = new AtomicLong();
  private final AtomicLong depth9PlusHits = new AtomicLong();
  private final AtomicLong depth9PlusMisses = new AtomicLong();

  private final AtomicLong trieAdmitted = new AtomicLong();
  private final AtomicLong trieRejected = new AtomicLong();

  private final AtomicLong trieBytesInserted = new AtomicLong();
  private final AtomicLong trieEntriesInserted = new AtomicLong();

  private final AtomicLong trieSmallNodes = new AtomicLong();
  private final AtomicLong trieMediumNodes = new AtomicLong();
  private final AtomicLong trieLargeNodes = new AtomicLong();
  private final AtomicLong trieHugeNodes = new AtomicLong();

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

    LOG.warn(">>> VersionedFlatDbCacheManager constructor called <<<");

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

    final long shallowWeight = Math.max(1L, trieNodeCacheWeight * SHALLOW_WEIGHT_PERCENT / 100);
    final long deepWeight = Math.max(1L, trieNodeCacheWeight - shallowWeight);

    this.trieNodeShallowCache = createWeightedCache(shallowWeight);
    this.trieNodeDeepCache = createWeightedCache(deepWeight);

    this.trieStorageNodesByAccount =
            Caffeine.newBuilder()
                    .initialCapacity(initialCapacityFor(trieNodeMaxAccounts))
                    .maximumWeight(trieNodeMaxAccounts)
                    .weigher(
                            (Weigher<CacheKey, Cache<CacheKey, VersionedValue>>)
                                    (key, value) -> {
                                      final AtomicLong w = trieStorageWeightByAccount.get(key);
                                      return (int) Math.min(Integer.MAX_VALUE, Math.max(1L, w == null ? 1L : w.get()));
                                    })
                    .executor(drainExecutor)
                    .build();

    this.cacheRequestCounter =
            metricsSystem.createCounter(
                    BesuMetricCategory.BLOCKCHAIN, "bonsai_cache_requests_total", "Total number of cache requests");
    this.cacheHitCounter =
            metricsSystem.createCounter(
                    BesuMetricCategory.BLOCKCHAIN, "bonsai_cache_hits_total", "Total number of cache hits");
    this.cacheMissCounter =
            metricsSystem.createCounter(
                    BesuMetricCategory.BLOCKCHAIN, "bonsai_cache_misses_total", "Total number of cache misses");
    this.cacheInsertCounter =
            metricsSystem.createCounter(
                    BesuMetricCategory.BLOCKCHAIN, "bonsai_cache_inserts_total", "Total number of cache insertions");
    this.cacheRemovalCounter =
            metricsSystem.createCounter(
                    BesuMetricCategory.BLOCKCHAIN, "bonsai_cache_removals_total", "Total number of cache removals");

    final ScheduledThreadPoolExecutor loggerExecutor =
            new ScheduledThreadPoolExecutor(
                    1,
                    r -> {
                      final Thread t = new Thread(r, "trie-stats-logger");
                      t.setDaemon(true);
                      return t;
                    });
    loggerExecutor.setRemoveOnCancelPolicy(true);
    this.trieStatsLogger = loggerExecutor;

    LOG.warn(">>> creating trie stats logger <<<");
    logTrieNodeStats();
    this.trieStatsLogger.scheduleAtFixedRate(
            () -> {
              LOG.warn(">>> trie stats logger tick <<<");
              logTrieNodeStats();
            },
            5,
            5,
            TimeUnit.SECONDS);

    LOG.info(
            "Trie cache configured: shallow={} bytes, deep={} bytes, per-account={} bytes",
            shallowWeight,
            deepWeight,
            PER_ACCOUNT_TRIE_WEIGHT_BYTES);
  }

  private Cache<CacheKey, VersionedValue> createCache(final long maxSize) {
    return Caffeine.newBuilder()
            .initialCapacity(initialCapacityFor(maxSize))
            .maximumSize(maxSize)
            .executor(drainExecutor)
            .build();
  }

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

  private Cache<CacheKey, VersionedValue> cacheFor(final SegmentIdentifier segment, final Bytes key) {
    if (segment == ACCOUNT_INFO_STATE) return accountCache;
    if (segment == ACCOUNT_STORAGE_STORAGE) return storageCache;
    if (segment == TRIE_BRANCH_STORAGE) {
      if (key.size() >= HASHED_ADDRESS_SIZE) {
        return storageTrieNodeCacheFor(key);
      }
      return isShallowTrieKey(key) ? trieNodeShallowCache : trieNodeDeepCache;
    }
    return null;
  }

  private Cache<CacheKey, VersionedValue> storageTrieNodeCacheFor(final Bytes key) {
    final CacheKey accountKey = CacheKey.of(key.slice(0, HASHED_ADDRESS_SIZE));
    return trieStorageNodesByAccount.get(accountKey, ignored -> createWeightedPerAccountCache(accountKey));
  }

  private Cache<CacheKey, VersionedValue> createWeightedPerAccountCache(final CacheKey accountKey) {
    trieStorageWeightByAccount.putIfAbsent(accountKey, new AtomicLong(0));
    return Caffeine.newBuilder()
            .initialCapacity(16)
            .maximumWeight(PER_ACCOUNT_TRIE_WEIGHT_BYTES)
            .weigher(
                    (Weigher<CacheKey, VersionedValue>)
                            (key, value) -> {
                              long weight = key.size();
                              final Bytes cached = value.getValue();
                              if (cached != null) {
                                weight += cached.size();
                              }
                              final AtomicLong accWeight = trieStorageWeightByAccount.get(accountKey);
                              if (accWeight != null) {
                                accWeight.set(weight);
                              }
                              return (int) Math.min(Integer.MAX_VALUE, Math.max(1L, weight));
                            })
            .executor(drainExecutor)
            .build();
  }

  static int trieNibbleDepth(final Bytes key) {
    return key.size();
  }

  static boolean isShallowTrieKey(final Bytes key) {
    return key.size() < HASHED_ADDRESS_SIZE && key.size() <= SHALLOW_TRIE_MAX_DEPTH;
  }

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

  @Override
  public void close() {
    LOG.warn(">>> close() called, dumping trie stats <<<");
    logTrieNodeStats();
    LOG.info("Shutting down cache maintenance worker and trie stats logger");

    trieStatsLogger.shutdown();
    maintenanceWorker.shutdown();

    try {
      if (!trieStatsLogger.awaitTermination(5, TimeUnit.SECONDS)) {
        trieStatsLogger.shutdownNow();
      }
      if (!maintenanceWorker.awaitTermination(5, TimeUnit.SECONDS)) {
        maintenanceWorker.shutdownNow();
      }
    } catch (final InterruptedException e) {
      trieStatsLogger.shutdownNow();
      maintenanceWorker.shutdownNow();
      Thread.currentThread().interrupt();
    }

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
      trieStorageWeightByAccount.clear();
      accountTrieStats.clear();
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
      if (segment == TRIE_BRANCH_STORAGE) {
        recordTrieHit(key);
      }
      return versionedValue.isRemoval ? Optional.empty() : Optional.of(versionedValue.getValue());
    }

    cacheMissCounter.inc();
    if (segment == TRIE_BRANCH_STORAGE) {
      recordTrieMiss(key);
    }

    final Optional<Bytes> result = storageGetter.get();
    if (version == globalVersion.get() && shouldInsertReadMiss(segment, result)) {
      final Bytes valueToCache = result.orElse(null);
      if (segment != TRIE_BRANCH_STORAGE || admitTrieNode(key, valueToCache)) {
        cacheInsertCounter.inc();
        if (segment == TRIE_BRANCH_STORAGE && valueToCache != null) {
          trieBytesInserted.addAndGet(valueToCache.size());
          trieEntriesInserted.incrementAndGet();
          recordNodeSizeBucket(valueToCache.size());
        }
        final boolean isRemoval = result.isEmpty();
        cache.asMap()
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
        results.add(versionedValue.isRemoval ? Optional.empty() : Optional.of(versionedValue.getValue()));
        if (segment == TRIE_BRANCH_STORAGE) {
          recordTrieHit(key);
        }
      } else {
        cacheMissCounter.inc();
        if (segment == TRIE_BRANCH_STORAGE) {
          recordTrieMiss(key);
        }
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
          final Bytes valueToCache = fetchedValue.orElse(null);
          if (segment != TRIE_BRANCH_STORAGE || admitTrieNode(key, valueToCache)) {
            cacheInsertCounter.inc();
            if (segment == TRIE_BRANCH_STORAGE && valueToCache != null) {
              trieBytesInserted.addAndGet(valueToCache.size());
              trieEntriesInserted.incrementAndGet();
              recordNodeSizeBucket(valueToCache.size());
            }
            final boolean isRemoval = fetchedValue.isEmpty();
            final Cache<CacheKey, VersionedValue> cache = cacheFor(segment, key);
            final CacheKey cacheKey = CacheKey.of(key);
            cache.asMap()
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
    }

    return results;
  }

  @Override
  public void putInCache(
          final SegmentIdentifier segment, final Bytes key, final Bytes value, final long version) {
    final Cache<CacheKey, VersionedValue> cache = cacheFor(segment, key);
    if (cache != null) {
      final CacheKey cacheKey = CacheKey.of(key);
      cache.asMap()
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
  public void removeFromCache(final SegmentIdentifier segment, final Bytes key, final long version) {
    final Cache<CacheKey, VersionedValue> cache = cacheFor(segment, key);
    if (cache != null) {
      final CacheKey cacheKey = CacheKey.of(key);
      cache.asMap()
              .compute(
                      cacheKey,
                      (k, existingValue) -> {
                        if (existingValue == null || existingValue.version < version) {
                          cacheRemovalCounter.inc();
                          if (segment == TRIE_BRANCH_STORAGE) {
                            return null;
                          }
                          return new VersionedValue(null, version, true);
                        }
                        return existingValue;
                      });
    }
  }

  private static boolean shouldInsertReadMiss(
          final SegmentIdentifier segment, final Optional<Bytes> result) {
    return segment != TRIE_BRANCH_STORAGE || result.filter(b -> !b.isEmpty()).isPresent();
  }

  @Override
  public long getCacheSize(final SegmentIdentifier segment) {
    if (segment == TRIE_BRANCH_STORAGE) {
      long size = trieNodeShallowCache.estimatedSize() + trieNodeDeepCache.estimatedSize();
      for (final Cache<CacheKey, VersionedValue> perAccount : trieStorageNodesByAccount.asMap().values()) {
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
    return cache != null ? Optional.ofNullable(cache.getIfPresent(CacheKey.of(key))) : Optional.empty();
  }

  private void recordTrieHit(final Bytes key) {
    trieRequestsTotal.incrementAndGet();
    trieHitsTotal.incrementAndGet();
    final int depth = trieNibbleDepth(key);
    recordDepthHit(depth);

    if (key.size() >= HASHED_ADDRESS_SIZE) {
      trieHitsAccount.incrementAndGet();
    } else if (isShallowTrieKey(key)) {
      trieHitsShallow.incrementAndGet();
    } else {
      trieHitsDeep.incrementAndGet();
    }
  }

  private void recordTrieMiss(final Bytes key) {
    trieRequestsTotal.incrementAndGet();
    trieMissesTotal.incrementAndGet();
    final int depth = trieNibbleDepth(key);
    recordDepthMiss(depth);

    if (key.size() >= HASHED_ADDRESS_SIZE) {
      trieMissesAccount.incrementAndGet();
    } else if (isShallowTrieKey(key)) {
      trieMissesShallow.incrementAndGet();
    } else {
      trieMissesDeep.incrementAndGet();
    }
  }

  private void recordDepthHit(final int depth) {
    if (depth <= 2) {
      depth0to2Hits.incrementAndGet();
    } else if (depth <= 4) {
      depth3to4Hits.incrementAndGet();
    } else if (depth <= 8) {
      depth5to8Hits.incrementAndGet();
    } else {
      depth9PlusHits.incrementAndGet();
    }
  }

  private void recordDepthMiss(final int depth) {
    if (depth <= 2) {
      depth0to2Misses.incrementAndGet();
    } else if (depth <= 4) {
      depth3to4Misses.incrementAndGet();
    } else if (depth <= 8) {
      depth5to8Misses.incrementAndGet();
    } else {
      depth9PlusMisses.incrementAndGet();
    }
  }

  private boolean admitTrieNode(final Bytes key, final Bytes value) {
    if (value == null || value.isEmpty()) {
      trieRejected.incrementAndGet();
      return false;
    }

    final int size = value.size();

    if (size > ADMISSION_MAX_NODE_SIZE_BYTES) {
      trieRejected.incrementAndGet();
      return false;
    }

    final boolean isAccountTrieNode = key.size() >= HASHED_ADDRESS_SIZE;
    final boolean isShallow = isShallowTrieKey(key);

    // Shallow nodes are always admitted if not too large.
    if (isShallow) {
      trieAdmitted.incrementAndGet();
      return true;
    }

    // Deep and account nodes are stricter.
    if (isAccountTrieNode && size > ADMISSION_MAX_DEEP_NODE_SIZE_BYTES) {
      trieRejected.incrementAndGet();
      return false;
    }

    if (isAccountTrieNode) {
      final CacheKey accountKey = CacheKey.of(key.slice(0, HASHED_ADDRESS_SIZE));
      final AccountTrieStats stats =
              accountTrieStats.computeIfAbsent(accountKey, k -> new AccountTrieStats());
      final boolean accepted = stats.shouldAdmit(size);
      if (accepted) {
        trieAdmitted.incrementAndGet();
      } else {
        trieRejected.incrementAndGet();
      }
      return accepted;
    }

    // Non-shallow, non-account trie nodes: admit only if acceptable size.
    if (size > ADMISSION_MAX_DEEP_NODE_SIZE_BYTES) {
      trieRejected.incrementAndGet();
      return false;
    }

    trieAdmitted.incrementAndGet();
    return true;
  }

  private void recordNodeSizeBucket(final int size) {
    if (size <= SMALL_NODE_LIMIT) {
      trieSmallNodes.incrementAndGet();
    } else if (size <= MEDIUM_NODE_LIMIT) {
      trieMediumNodes.incrementAndGet();
    } else if (size <= LARGE_NODE_LIMIT) {
      trieLargeNodes.incrementAndGet();
    } else {
      trieHugeNodes.incrementAndGet();
    }
  }

  private void logTrieNodeStats() {
    try {
      final long requests = trieRequestsTotal.get();
      final long hits = trieHitsTotal.get();
      final long misses = trieMissesTotal.get();

      final long shallowHits = trieHitsShallow.get();
      final long deepHits = trieHitsDeep.get();
      final long accountHits = trieHitsAccount.get();

      final long shallowMisses = trieMissesShallow.get();
      final long deepMisses = trieMissesDeep.get();
      final long accountMisses = trieMissesAccount.get();

      final long admitted = trieAdmitted.get();
      final long rejected = trieRejected.get();

      final long insertedBytes = trieBytesInserted.get();
      final long insertedEntries = trieEntriesInserted.get();

      final long small = trieSmallNodes.get();
      final long medium = trieMediumNodes.get();
      final long large = trieLargeNodes.get();
      final long huge = trieHugeNodes.get();

      final double hitRate = requests == 0 ? 0.0 : (100.0 * hits / requests);
      final double shallowHitRate =
              (shallowHits + shallowMisses) == 0 ? 0.0 : (100.0 * shallowHits / (shallowHits + shallowMisses));
      final double deepHitRate =
              (deepHits + deepMisses) == 0 ? 0.0 : (100.0 * deepHits / (deepHits + deepMisses));
      final double accountHitRate =
              (accountHits + accountMisses) == 0 ? 0.0 : (100.0 * accountHits / (accountHits + accountMisses));
      final double avgNodeBytes = insertedEntries == 0 ? 0.0 : (double) insertedBytes / insertedEntries;

      final long d0h = depth0to2Hits.get();
      final long d0m = depth0to2Misses.get();
      final long d1h = depth3to4Hits.get();
      final long d1m = depth3to4Misses.get();
      final long d2h = depth5to8Hits.get();
      final long d2m = depth5to8Misses.get();
      final long d3h = depth9PlusHits.get();
      final long d3m = depth9PlusMisses.get();

      final double depth0HitRate = (d0h + d0m) == 0 ? 0.0 : (100.0 * d0h / (d0h + d0m));
      final double depth1HitRate = (d1h + d1m) == 0 ? 0.0 : (100.0 * d1h / (d1h + d1m));
      final double depth2HitRate = (d2h + d2m) == 0 ? 0.0 : (100.0 * d2h / (d2h + d2m));
      final double depth3HitRate = (d3h + d3m) == 0 ? 0.0 : (100.0 * d3h / (d3h + d3m));

      LOG.warn(
              "[TRIE CACHE STATS] req={} hits={} misses={} hitRate={}%; shallowHits={} shallowMisses={} shallowHitRate={}%; deepHits={} deepMisses={} deepHitRate={}%; accountHits={} accountMisses={} accountHitRate={}%; depthHitRates: d0to2={}%, d3to4={}%, d5to8={}%, d9plus={}%; admitted={} rejected={} avgNodeBytes={} insertedBytes={} entries={}; sizeBuckets: small={} medium={} large={} huge={}",
              requests,
              hits,
              misses,
              String.format("%.2f", hitRate),
              shallowHits,
              shallowMisses,
              String.format("%.2f", shallowHitRate),
              deepHits,
              deepMisses,
              String.format("%.2f", deepHitRate),
              accountHits,
              accountMisses,
              String.format("%.2f", accountHitRate),
              String.format("%.2f", depth0HitRate),
              String.format("%.2f", depth1HitRate),
              String.format("%.2f", depth2HitRate),
              String.format("%.2f", depth3HitRate),
              admitted,
              rejected,
              String.format("%.2f", avgNodeBytes),
              insertedBytes,
              insertedEntries,
              small,
              medium,
              large,
              huge);
    } catch (final Exception e) {
      LOG.warn("Failed to log trie cache stats", e);
    }
  }

  private static class AccountTrieStats {
    private final AtomicLong requests = new AtomicLong();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong bytesSeen = new AtomicLong();

    boolean shouldAdmit(final int size) {
      final long r = requests.incrementAndGet();
      if (size > ADMISSION_MAX_DEEP_NODE_SIZE_BYTES) {
        return false;
      }
      if (r <= 8) {
        bytesSeen.addAndGet(size);
        return true;
      }
      final long h = hits.get();
      final long seen = bytesSeen.addAndGet(size);
      final long avgSize = seen / Math.max(1L, r);
      final long estimatedHitRate = (h * 100) / Math.max(1L, r);

      if (avgSize > 4096) {
        return estimatedHitRate >= 20;
      }
      return estimatedHitRate >= 5 || r <= 32;
    }

    @SuppressWarnings("unused")
    void recordHit() {
      hits.incrementAndGet();
    }
  }

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