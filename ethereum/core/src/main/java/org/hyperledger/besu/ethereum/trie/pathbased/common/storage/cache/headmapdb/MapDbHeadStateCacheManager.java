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
package org.hyperledger.besu.ethereum.trie.pathbased.common.storage.cache.headmapdb;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.io.Closeable;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Central manager for the single active head MapDB cache and its read-only snapshot.
 *
 * <p>Eviction policy: entries whose last access or write block is more than {@link
 * #EVICTION_BLOCK_THRESHOLD} blocks behind the current head block are removed from the active head
 * MapDB cache only.
 */
public final class MapDbHeadStateCacheManager implements Closeable {

  public static final int EVICTION_BLOCK_THRESHOLD = 128;
  public static final int STARTUP_TRIE_LOG_WINDOW = 512;

  private static final Logger LOG = LoggerFactory.getLogger(MapDbHeadStateCacheManager.class);
  private static final Bytes RESOLVED_FROM_MAPDB_META = Bytes.of(1);
  private static final Bytes RESOLVED_FROM_KV_META = Bytes.of(2);
  private static final Bytes REMOVAL_SENTINEL = Bytes.EMPTY;

  private final ActiveMapDbHeadCache activeCache = new ActiveMapDbHeadCache();
  private final Object snapshotLock = new Object();
  private volatile MapDbHeadSnapshot headSnapshot;
  private volatile long headBlockNumber;
  private volatile Hash headBlockHash = Hash.ZERO;

  private final Counter mapDbHitCounter;
  private final Counter mapDbMissCounter;
  private final Counter kvFallbackCounter;
  private final Counter writeBackCounter;

  public MapDbHeadStateCacheManager(final MetricsSystem metricsSystem) {
    this.mapDbHitCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_head_mapdb_hits_total",
            "Head MapDB cache hits");
    this.mapDbMissCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_head_mapdb_misses_total",
            "Head MapDB cache misses");
    this.kvFallbackCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_head_mapdb_kv_fallback_total",
            "Head reads resolved from key-value storage");
    this.writeBackCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "bonsai_head_mapdb_write_back_total",
            "Values read from key-value storage and inserted into active head MapDB");
    replaceHeadSnapshot();
  }

  public long getHeadBlockNumber() {
    return headBlockNumber;
  }

  public Hash getHeadBlockHash() {
    return headBlockHash;
  }

  /**
   * FCU head switch ordering:
   *
   * <ol>
   *   <li>Prepare new head MapDB snapshot from the active cache
   *   <li>Validate snapshot readability
   *   <li>Atomically replace head snapshot association
   *   <li>Delete previous snapshot
   * </ol>
   */
  public void performForkChoiceUpdate(final Hash newHeadBlockHash, final long newHeadBlockNumber) {
    Objects.requireNonNull(newHeadBlockHash, "newHeadBlockHash");
    synchronized (snapshotLock) {
      activeCache.commit();
      final MapDbHeadSnapshot prepared = new MapDbHeadSnapshot(activeCache);
      validateSnapshot(prepared);
      final MapDbHeadSnapshot previous = headSnapshot;
      headSnapshot = prepared;
      headBlockHash = newHeadBlockHash;
      headBlockNumber = newHeadBlockNumber;
      if (previous != null) {
        previous.close();
      }
    }
    evictStaleEntries();
    LOG.atDebug()
        .setMessage("FCU head MapDB snapshot swapped for block {} ({})")
        .addArgument(newHeadBlockNumber)
        .addArgument(newHeadBlockHash)
        .log();
  }

  /** Snapshot captured for frozen newPayload states; does not permit active-cache mutation. */
  public MapDbHeadSnapshot captureSnapshotForFrozenState() {
    synchronized (snapshotLock) {
      activeCache.commit();
      return new MapDbHeadSnapshot(activeCache);
    }
  }

  public void preloadEntry(
      final HeadMapDbCacheCategory category, final Bytes logicalKey, final Bytes value) {
    final Bytes encoded = category.encodeKey(logicalKey);
    activeCache.putData(encoded, value);
    activeCache.touchAccessBlock(encoded, headBlockNumber);
    recordResolution(category, logicalKey, ResolutionSource.RESOLVED_FROM_MAPDB);
  }

  public Optional<Bytes> readFlatAccount(
      final HeadStateCacheAccessPolicy policy,
      final Bytes accountKey,
      final Supplier<Optional<Bytes>> keyValueStorageGetter) {
    return readWithFlatPropagation(
        HeadMapDbCacheCategory.ACCOUNT_FLAT,
        accountKey,
        null,
        policy,
        keyValueStorageGetter);
  }

  public Optional<Bytes> readFlatStorage(
      final HeadStateCacheAccessPolicy policy,
      final Bytes storageKey,
      final Bytes accountKey,
      final Supplier<Optional<Bytes>> keyValueStorageGetter) {
    return readWithFlatPropagation(
        HeadMapDbCacheCategory.STORAGE_FLAT,
        storageKey,
        accountKey,
        policy,
        keyValueStorageGetter);
  }

  public Optional<Bytes> readTrieNode(
      final HeadMapDbCacheCategory trieCategory,
      final Bytes trieKey,
      final Optional<ResolutionSource> parentResolution,
      final HeadStateCacheAccessPolicy policy,
      final Supplier<Optional<Bytes>> keyValueStorageGetter) {
    if (policy == HeadStateCacheAccessPolicy.KEY_VALUE_STORAGE_ONLY) {
      return keyValueStorageGetter.get();
    }
    if (parentResolution.orElse(ResolutionSource.RESOLVED_FROM_MAPDB)
        == ResolutionSource.RESOLVED_FROM_KEY_VALUE_STORAGE) {
      return readKeyValueOnly(trieCategory, trieKey, keyValueStorageGetter);
    }
    return readHeadPath(trieCategory, trieKey, policy, keyValueStorageGetter, true);
  }

  public Optional<Bytes> readCode(
      final HeadStateCacheAccessPolicy policy,
      final Bytes codeKey,
      final Supplier<Optional<Bytes>> keyValueStorageGetter) {
    if (policy == HeadStateCacheAccessPolicy.KEY_VALUE_STORAGE_ONLY) {
      return keyValueStorageGetter.get();
    }
    return readHeadPath(
        HeadMapDbCacheCategory.CODE, codeKey, policy, keyValueStorageGetter, false);
  }

  public void writeHeadEntry(
      final HeadMapDbCacheCategory category, final Bytes logicalKey, final Bytes value) {
    final Bytes encoded = category.encodeKey(logicalKey);
    if (value == null || value.isEmpty()) {
      activeCache.putData(encoded, REMOVAL_SENTINEL);
    } else {
      activeCache.putData(encoded, value);
    }
    activeCache.touchAccessBlock(encoded, headBlockNumber);
    recordResolution(category, logicalKey, ResolutionSource.RESOLVED_FROM_MAPDB);
  }

  public void removeHeadEntry(final HeadMapDbCacheCategory category, final Bytes logicalKey) {
    final Bytes encoded = category.encodeKey(logicalKey);
    activeCache.removeData(encoded);
    activeCache.removeMeta(ActiveMapDbHeadCache.accessMetaKey(encoded));
    activeCache.removeMeta(resolutionKey(category, logicalKey));
    activeCache.commit();
  }

  /**
   * Evicts head-cache entries not accessed or written within the last {@link
   * #EVICTION_BLOCK_THRESHOLD} blocks (last-access-time policy).
   */
  public int evictStaleEntries() {
    final long threshold = headBlockNumber - EVICTION_BLOCK_THRESHOLD;
    if (threshold < 0) {
      return 0;
    }
    int removed = 0;
    for (final Bytes encodedKey : activeCache.dataKeys()) {
      final long last =
          activeCache.lastAccessBlock(encodedKey).orElse(Long.MIN_VALUE);
      if (last <= threshold) {
        activeCache.removeData(encodedKey);
        activeCache.removeMeta(ActiveMapDbHeadCache.accessMetaKey(encodedKey));
        removed++;
      }
    }
    if (removed > 0) {
      activeCache.commit();
      replaceHeadSnapshot();
    }
    return removed;
  }

  public Optional<ResolutionSource> getTrieNodeResolution(final Bytes trieLogicalKey) {
    return readResolutionMeta(HeadMapDbCacheCategory.TRIE_NODE_RESOLUTION, trieLogicalKey);
  }

  public Optional<ResolutionSource> getFlatAccountResolution(final Bytes accountKey) {
    return readResolutionMeta(HeadMapDbCacheCategory.FLAT_ACCOUNT_RESOLUTION, accountKey);
  }

  public static HeadMapDbCacheCategory trieCategoryForSegment(final SegmentIdentifier segment) {
    if (segment == TRIE_BRANCH_STORAGE) {
      return HeadMapDbCacheCategory.ACCOUNT_TRIE;
    }
    return HeadMapDbCacheCategory.STORAGE_TRIE;
  }

  public static Bytes trieLogicalKey(final Bytes location, final Bytes nodeHash) {
    return Bytes.concatenate(location == null ? Bytes.EMPTY : location, nodeHash);
  }

  @Override
  public void close() {
    synchronized (snapshotLock) {
      if (headSnapshot != null) {
        headSnapshot.close();
        headSnapshot = null;
      }
    }
    activeCache.close();
  }

  private Optional<Bytes> readWithFlatPropagation(
      final HeadMapDbCacheCategory category,
      final Bytes logicalKey,
      final Bytes accountKeyForStorage,
      final HeadStateCacheAccessPolicy policy,
      final Supplier<Optional<Bytes>> keyValueStorageGetter) {
    if (policy == HeadStateCacheAccessPolicy.KEY_VALUE_STORAGE_ONLY) {
      return keyValueStorageGetter.get();
    }
    if (accountKeyForStorage != null) {
      final Optional<ResolutionSource> accountResolution =
          getFlatAccountResolution(accountKeyForStorage);
      if (accountResolution.orElse(ResolutionSource.RESOLVED_FROM_MAPDB)
          == ResolutionSource.RESOLVED_FROM_KEY_VALUE_STORAGE) {
        return readKeyValueOnly(category, logicalKey, keyValueStorageGetter);
      }
    }
    final boolean trackFlatResolution =
        category == HeadMapDbCacheCategory.ACCOUNT_FLAT
            || category == HeadMapDbCacheCategory.STORAGE_FLAT;
    return readHeadPath(category, logicalKey, policy, keyValueStorageGetter, trackFlatResolution);
  }

  private Optional<Bytes> readHeadPath(
      final HeadMapDbCacheCategory category,
      final Bytes logicalKey,
      final HeadStateCacheAccessPolicy policy,
      final Supplier<Optional<Bytes>> keyValueStorageGetter,
      final boolean recordFlatResolution) {
    final Bytes encoded = category.encodeKey(logicalKey);
    final Optional<Bytes> fromMapDb = readFromSnapshotOrActive(encoded, policy);
    if (fromMapDb.isPresent()) {
      mapDbHitCounter.inc();
      activeCache.touchAccessBlock(encoded, headBlockNumber);
      if (recordFlatResolution
          && (category == HeadMapDbCacheCategory.ACCOUNT_FLAT
              || category == HeadMapDbCacheCategory.STORAGE_FLAT)) {
        recordResolution(category, logicalKey, ResolutionSource.RESOLVED_FROM_MAPDB);
      }
      if (fromMapDb.get().equals(REMOVAL_SENTINEL)) {
        return Optional.empty();
      }
      return fromMapDb;
    }
    mapDbMissCounter.inc();
    kvFallbackCounter.inc();
    final Optional<Bytes> fromKv = keyValueStorageGetter.get();
    if (fromKv.isPresent()) {
      if (policy == HeadStateCacheAccessPolicy.CANONICAL_HEAD) {
        activeCache.putData(encoded, fromKv.get());
        activeCache.touchAccessBlock(encoded, headBlockNumber);
        writeBackCounter.inc();
        recordResolution(category, logicalKey, ResolutionSource.RESOLVED_FROM_KEY_VALUE_STORAGE);
      }
      return fromKv;
    }
    if (policy == HeadStateCacheAccessPolicy.CANONICAL_HEAD) {
      recordResolution(category, logicalKey, ResolutionSource.RESOLVED_FROM_KEY_VALUE_STORAGE);
    }
    return Optional.empty();
  }

  private Optional<Bytes> readKeyValueOnly(
      final HeadMapDbCacheCategory category,
      final Bytes logicalKey,
      final Supplier<Optional<Bytes>> keyValueStorageGetter) {
    kvFallbackCounter.inc();
    final Optional<Bytes> fromKv = keyValueStorageGetter.get();
    if (fromKv.isPresent()) {
      recordResolution(category, logicalKey, ResolutionSource.RESOLVED_FROM_KEY_VALUE_STORAGE);
    }
    return fromKv;
  }

  private Optional<Bytes> readFromSnapshotOrActive(
      final Bytes encodedKey, final HeadStateCacheAccessPolicy policy) {
    if (policy == HeadStateCacheAccessPolicy.FROZEN_SNAPSHOT) {
      final MapDbHeadSnapshot snap = headSnapshot;
      if (snap == null || snap.isClosed()) {
        return Optional.empty();
      }
      return snap.get(encodedKey);
    }
    if (policy == HeadStateCacheAccessPolicy.CANONICAL_HEAD) {
      final Optional<Bytes> active = activeCache.getData(encodedKey);
      if (active.isPresent()) {
        return active;
      }
      final MapDbHeadSnapshot snap = headSnapshot;
      if (snap != null && !snap.isClosed()) {
        return snap.get(encodedKey);
      }
    }
    return Optional.empty();
  }

  private void recordResolution(
      final HeadMapDbCacheCategory category, final Bytes logicalKey, final ResolutionSource source) {
    if (category == HeadMapDbCacheCategory.ACCOUNT_FLAT) {
      putResolution(HeadMapDbCacheCategory.FLAT_ACCOUNT_RESOLUTION, logicalKey, source);
    } else if (category == HeadMapDbCacheCategory.ACCOUNT_TRIE
        || category == HeadMapDbCacheCategory.STORAGE_TRIE) {
      putResolution(HeadMapDbCacheCategory.TRIE_NODE_RESOLUTION, logicalKey, source);
    }
  }

  private void putResolution(
      final HeadMapDbCacheCategory resolutionCategory,
      final Bytes logicalKey,
      final ResolutionSource source) {
    activeCache.putMeta(
        resolutionKey(resolutionCategory, logicalKey),
        source == ResolutionSource.RESOLVED_FROM_MAPDB
            ? RESOLVED_FROM_MAPDB_META
            : RESOLVED_FROM_KV_META);
  }

  private Optional<ResolutionSource> readResolutionMeta(
      final HeadMapDbCacheCategory resolutionCategory, final Bytes logicalKey) {
    return activeCache
        .getMeta(resolutionKey(resolutionCategory, logicalKey))
        .map(
            meta -> {
              if (meta.equals(RESOLVED_FROM_MAPDB_META)) {
                return ResolutionSource.RESOLVED_FROM_MAPDB;
              }
              return ResolutionSource.RESOLVED_FROM_KEY_VALUE_STORAGE;
            });
  }

  private static Bytes resolutionKey(
      final HeadMapDbCacheCategory resolutionCategory, final Bytes logicalKey) {
    return resolutionCategory.encodeKey(logicalKey);
  }

  private void replaceHeadSnapshot() {
    synchronized (snapshotLock) {
      activeCache.commit();
      final MapDbHeadSnapshot next = new MapDbHeadSnapshot(activeCache);
      validateSnapshot(next);
      final MapDbHeadSnapshot previous = headSnapshot;
      headSnapshot = next;
      if (previous != null) {
        previous.close();
      }
    }
  }

  private static void validateSnapshot(final MapDbHeadSnapshot snapshot) {
    if (snapshot.isClosed()) {
      throw new IllegalStateException("Prepared head MapDB snapshot is already closed");
    }
  }
}
