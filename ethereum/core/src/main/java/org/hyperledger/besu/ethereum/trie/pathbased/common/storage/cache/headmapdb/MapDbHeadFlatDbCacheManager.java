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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;

/**
 * {@link FlatDbCacheManager} implementation backed by the head MapDB cache manager. Non-head states
 * must install {@link HeadStateCacheAccessPolicy#KEY_VALUE_STORAGE_ONLY} on the owning storage
 * before reads occur.
 */
public final class MapDbHeadFlatDbCacheManager implements FlatDbCacheManager, Closeable {

  private final MapDbHeadStateCacheManager headCacheManager;
  private final Supplier<HeadStateCacheAccessPolicy> accessPolicySupplier;
  private final AtomicHeadCacheVersion cacheVersion = new AtomicHeadCacheVersion();

  public MapDbHeadFlatDbCacheManager(
      final MapDbHeadStateCacheManager headCacheManager,
      final Supplier<HeadStateCacheAccessPolicy> accessPolicySupplier) {
    this.headCacheManager = headCacheManager;
    this.accessPolicySupplier = accessPolicySupplier;
  }

  @Override
  public long getCurrentVersion() {
    return cacheVersion.get();
  }

  @Override
  public long incrementAndGetVersion() {
    return cacheVersion.incrementAndGet();
  }

  @Override
  public void clear(final SegmentIdentifier segment) {
    // Head MapDB eviction is block-threshold based; explicit clear is a no-op at this layer.
  }

  @Override
  public Optional<Bytes> getFromCacheOrStorage(
      final SegmentIdentifier segment,
      final Bytes key,
      final long version,
      final Supplier<Optional<Bytes>> storageGetter) {
    final HeadStateCacheAccessPolicy policy = accessPolicySupplier.get();
    if (segment == ACCOUNT_INFO_STATE) {
      return headCacheManager.readFlatAccount(policy, key, storageGetter);
    }
    if (segment == ACCOUNT_STORAGE_STORAGE) {
      final Bytes accountKey = accountPrefixFromStorageKey(key);
      return headCacheManager.readFlatStorage(policy, key, accountKey, storageGetter);
    }
    return storageGetter.get();
  }

  @Override
  public List<Optional<Bytes>> getMultipleFromCacheOrStorage(
      final SegmentIdentifier segment,
      final List<Bytes> keys,
      final long version,
      final Function<List<Bytes>, List<Optional<Bytes>>> batchFetcher) {
    final List<Optional<Bytes>> results = new ArrayList<>(keys.size());
    for (final Bytes key : keys) {
      results.add(getFromCacheOrStorage(segment, key, version, () -> Optional.empty()));
    }
    return results;
  }

  @Override
  public void putInCache(
      final SegmentIdentifier segment, final Bytes key, final Bytes value, final long version) {
    if (accessPolicySupplier.get() != HeadStateCacheAccessPolicy.CANONICAL_HEAD) {
      return;
    }
    if (segment == ACCOUNT_INFO_STATE) {
      headCacheManager.writeHeadEntry(HeadMapDbCacheCategory.ACCOUNT_FLAT, key, value);
    } else if (segment == ACCOUNT_STORAGE_STORAGE) {
      headCacheManager.writeHeadEntry(HeadMapDbCacheCategory.STORAGE_FLAT, key, value);
    }
  }

  @Override
  public void removeFromCache(
      final SegmentIdentifier segment, final Bytes key, final long version) {
    if (accessPolicySupplier.get() != HeadStateCacheAccessPolicy.CANONICAL_HEAD) {
      return;
    }
    if (segment == ACCOUNT_INFO_STATE) {
      headCacheManager.removeHeadEntry(HeadMapDbCacheCategory.ACCOUNT_FLAT, key);
    } else if (segment == ACCOUNT_STORAGE_STORAGE) {
      headCacheManager.removeHeadEntry(HeadMapDbCacheCategory.STORAGE_FLAT, key);
    }
  }

  @Override
  public long getCacheSize(final SegmentIdentifier segment) {
    return 0;
  }

  @Override
  public boolean isCached(final SegmentIdentifier segment, final Bytes key) {
    return false;
  }

  @Override
  public Optional<VersionedValue> getCachedValue(final SegmentIdentifier segment, final Bytes key) {
    return Optional.empty();
  }

  @Override
  public void close() {
    headCacheManager.close();
  }

  public MapDbHeadStateCacheManager getHeadCacheManager() {
    return headCacheManager;
  }

  private static Bytes accountPrefixFromStorageKey(final Bytes storageKey) {
    if (storageKey.size() < 32) {
      return storageKey;
    }
    return storageKey.slice(0, 32);
  }

  /** Local version counter for {@link FlatDbCacheManager} contract compatibility. */
  private static final class AtomicHeadCacheVersion {
    private long version;

    long get() {
      return version;
    }

    long incrementAndGet() {
      return ++version;
    }
  }
}
