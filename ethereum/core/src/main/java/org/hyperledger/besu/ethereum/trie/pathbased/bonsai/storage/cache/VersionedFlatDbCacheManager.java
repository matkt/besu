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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;

import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Versioned cache implementation using {@link BlockLruCache}: LRU at block granularity, evicted
 * once per committed block on a background thread.
 */
public class VersionedFlatDbCacheManager implements FlatDbCacheManager, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(VersionedFlatDbCacheManager.class);

  /** Upper bound for {@code initialCapacity} (must fit in a positive int). */
  private static final long MAX_INITIAL_CAPACITY = Integer.MAX_VALUE;

  private final AtomicLong globalVersion = new AtomicLong(0);

  /** Nested commit-bypass count; when readers ignore the cache entirely. */
  private final AtomicInteger commitCacheBypassCount = new AtomicInteger(0);

  private final BlockLruCache<VersionedValue> accountCache;
  private final BlockLruCache<VersionedValue> storageCache;
  private final ExecutorService maintenanceWorker;
  private final AtomicBoolean maintenanceScheduled = new AtomicBoolean(false);

  /** Set by every request, so one arriving while a pass runs gets its own pass. */
  private final AtomicBoolean maintenanceRequested = new AtomicBoolean(false);

  private final Counter cacheRequestCounter;
  private final Counter cacheHitCounter;
  private final Counter cacheMissCounter;
  private final Counter cacheInsertCounter;
  private final Counter cacheRemovalCounter;

  /**
   * Creates a new VersionedFlatDbCacheManager.
   *
   * @param accountCacheSize maximum number of entries in the account cache
   * @param storageCacheSize maximum number of entries in the storage cache
   * @param metricsSystem the metrics system for instrumentation
   */
  public VersionedFlatDbCacheManager(
      final long accountCacheSize, final long storageCacheSize, final MetricsSystem metricsSystem) {

    requirePositiveCacheMaxSize("accountCacheSize", accountCacheSize);
    requirePositiveCacheMaxSize("storageCacheSize", storageCacheSize);

    this.maintenanceWorker =
        Executors.newSingleThreadExecutor(
            r -> {
              final Thread t = new Thread(r, "cache-maintenance");
              t.setDaemon(true);
              return t;
            });

    this.accountCache = createCache(accountCacheSize);
    this.storageCache = createCache(storageCacheSize);

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
  }

  private <V> BlockLruCache<V> createCache(final long maxSize) {
    return new BlockLruCache<>(maxSize, initialCapacityFor(maxSize), globalVersion::get);
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

  private BlockLruCache<VersionedValue> cacheForSegment(final SegmentIdentifier segment) {
    if (segment == ACCOUNT_INFO_STATE) {
      return accountCache;
    }
    if (segment == ACCOUNT_STORAGE_STORAGE) {
      return storageCache;
    }
    return null;
  }

  /**
   * Schedules an async maintenance if one is not already scheduled. Uses an AtomicBoolean to
   * prevent flooding the maintenance worker with redundant tasks; a request made while a pass runs
   * triggers another pass.
   */
  @Override
  public void scheduleAsyncMaintenance() {
    maintenanceRequested.set(true);
    if (maintenanceScheduled.compareAndSet(false, true)) {
      try {
        maintenanceWorker.execute(this::runRequestedMaintenance);
      } catch (final Exception e) {
        maintenanceScheduled.set(false);
        LOG.warn("Failed to schedule async cache maintenance", e);
      }
    }
  }

  private void runRequestedMaintenance() {
    try {
      while (maintenanceRequested.getAndSet(false)) {
        doMaintenance();
      }
    } finally {
      maintenanceScheduled.set(false);
    }
    // a request may have landed between the last check and clearing the flag
    if (maintenanceRequested.get()) {
      scheduleAsyncMaintenance();
    }
  }

  /** Performs the actual maintenance work: evicts every cache down to its bound. */
  private void doMaintenance() {
    try {
      final int evicted = accountCache.evict() + storageCache.evict();
      if (evicted > 0) {
        LOG.trace("Cache maintenance evicted {} entries", evicted);
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
  public void beginCommitCacheBypass() {
    commitCacheBypassCount.incrementAndGet();
  }

  @Override
  public void endCommitCacheBypass() {
    final int remaining = commitCacheBypassCount.decrementAndGet();
    if (remaining < 0) {
      commitCacheBypassCount.set(0);
      LOG.warn("endCommitCacheBypass() called without a matching begin");
    }
  }

  @Override
  public boolean isCommitCacheBypassActive() {
    return commitCacheBypassCount.get() > 0;
  }

  @Override
  public void clear(final SegmentIdentifier segment) {
    final BlockLruCache<VersionedValue> cache = cacheForSegment(segment);
    if (cache != null) {
      cache.clear();
    }
  }

  @Override
  public Optional<Bytes> getFromCacheOrStorage(
      final SegmentIdentifier segment,
      final Bytes key,
      final long version,
      final Supplier<Optional<Bytes>> storageGetter) {

    cacheRequestCounter.inc();

    // During head commit publish, ignore the cache completely: no stale hits, no miss inserts.
    if (isCommitCacheBypassActive()) {
      cacheMissCounter.inc();
      return storageGetter.get();
    }

    final BlockLruCache<VersionedValue> cache = cacheForSegment(segment);

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

    if (version == globalVersion.get()) {
      cacheInsertCounter.inc();
      final Bytes valueToCache = result.orElse(null);
      final boolean isRemoval = result.isEmpty();

      cache.compute(
          cacheKey,
          existingValue -> {
            if (existingValue == null || existingValue.version < version) {
              return new VersionedValue(valueToCache, version, isRemoval);
            }
            return existingValue;
          });
      requestMaintenanceIfOverflowing(cache);
    }

    return result;
  }

  @Override
  public List<Optional<Bytes>> getMultipleFromCacheOrStorage(
      final SegmentIdentifier segment,
      final List<Bytes> keys,
      final long version,
      final Function<List<Bytes>, List<Optional<Bytes>>> batchFetcher) {

    if (isCommitCacheBypassActive()) {
      keys.forEach(
          k -> {
            cacheRequestCounter.inc();
            cacheMissCounter.inc();
          });
      final List<Optional<Bytes>> fetched = batchFetcher.apply(keys);
      if (fetched.size() != keys.size()) {
        return Collections.nCopies(keys.size(), null);
      }
      return fetched;
    }

    final BlockLruCache<VersionedValue> cache = cacheForSegment(segment);

    if (cache == null) {
      keys.forEach(k -> cacheMissCounter.inc());
      return batchFetcher.apply(keys);
    }

    final List<Optional<Bytes>> results = new ArrayList<>(keys.size());
    final List<Bytes> keysToFetch = new ArrayList<>();
    final List<Integer> indicesToFetch = new ArrayList<>();

    for (int i = 0; i < keys.size(); i++) {
      final Bytes key = keys.get(i);
      cacheRequestCounter.inc();

      final CacheKey cacheKey = CacheKey.of(key);
      final VersionedValue versionedValue = cache.getIfPresent(cacheKey);

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
      if (fetchedValues.size() != keysToFetch.size()) {
        return results;
      }

      final boolean shouldUpdateCache = version == globalVersion.get();

      for (int i = 0; i < fetchedValues.size(); i++) {
        final Optional<Bytes> fetchedValue = fetchedValues.get(i);
        final int resultIndex = indicesToFetch.get(i);
        final Bytes key = keysToFetch.get(i);

        results.set(resultIndex, fetchedValue);

        if (shouldUpdateCache && fetchedValue != null) {
          cacheInsertCounter.inc();
          final CacheKey cacheKey = CacheKey.of(key);
          final Bytes valueToCache = fetchedValue.orElse(null);
          final boolean isRemoval = fetchedValue.isEmpty();

          cache.compute(
              cacheKey,
              existingValue -> {
                if (existingValue == null || existingValue.version < version) {
                  return new VersionedValue(valueToCache, version, isRemoval);
                }
                return existingValue;
              });
        }
      }
      requestMaintenanceIfOverflowing(cache);
    }

    return results;
  }

  @Override
  public void putInCache(
      final SegmentIdentifier segment, final Bytes key, final Bytes value, final long version) {
    final BlockLruCache<VersionedValue> cache = cacheForSegment(segment);
    if (cache != null) {
      final CacheKey cacheKey = CacheKey.of(key);
      cache.compute(
          cacheKey,
          existingValue -> {
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
    final BlockLruCache<VersionedValue> cache = cacheForSegment(segment);
    if (cache != null) {
      final CacheKey cacheKey = CacheKey.of(key);
      cache.compute(
          cacheKey,
          existingValue -> {
            if (existingValue == null || existingValue.version < version) {
              cacheRemovalCounter.inc();
              return new VersionedValue(null, version, true);
            }
            return existingValue;
          });
    }
  }

  /** Normally eviction waits for the end of the block; a large block or RPC load can't wait. */
  private void requestMaintenanceIfOverflowing(final BlockLruCache<?> cache) {
    if (cache.isOverflowing()) {
      scheduleAsyncMaintenance();
    }
  }

  @Override
  public long getCacheSize(final SegmentIdentifier segment) {
    final BlockLruCache<VersionedValue> cache = cacheForSegment(segment);
    return cache != null ? cache.size() : 0;
  }

  @Override
  public boolean isCached(final SegmentIdentifier segment, final Bytes key) {
    final BlockLruCache<VersionedValue> cache = cacheForSegment(segment);
    return cache != null && cache.peek(CacheKey.of(key)) != null;
  }

  @Override
  public Optional<VersionedValue> getCachedValue(final SegmentIdentifier segment, final Bytes key) {
    final BlockLruCache<VersionedValue> cache = cacheForSegment(segment);
    return cache != null ? Optional.ofNullable(cache.peek(CacheKey.of(key))) : Optional.empty();
  }
}
