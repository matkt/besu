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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.internal.CodeCache;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;

/**
 * Flat-db cache manager for versioned account/storage entries and content-addressed analyzed
 * contract {@link Code}. Implements {@link CodeCache} so EVM account tracking can use the same KV
 * analyzed-code cache (not a separate layer). The no-op implementation bypasses caching.
 */
public interface FlatDbCacheManager extends CodeCache {

  FlatDbCacheManager NO_OP_CACHE = new FlatDbCacheManager() {};

  /**
   * Steady-state maximum entries for versioned account and storage caches after {@link
   * #scheduleAsyncMaintenance()}.
   */
  long CACHE_STEADY_SIZE = 256L;

  /**
   * Steady-state maximum entries for the analyzed-code cache after maintenance (~production
   * BonsaiCodeCache occupancy). Peak during a block remains the configured code peak (default
   * 100_000).
   */
  long CODE_CACHE_STEADY_SIZE = 25_000L;

  default long getCurrentVersion() {
    return 0;
  }

  default long incrementAndGetVersion() {
    return 0;
  }

  default void clear(final SegmentIdentifier segment) {
    // No-op
  }

  default void scheduleAsyncMaintenance() {
    // No-op
  }

  /**
   * Raises account, storage, and analyzed-code cache maxima to their configured peaks so a block
   * (and BAL prefetch) can retain all warmed entries until maintenance shrinks them again.
   */
  default void expandCachesForBlock() {
    // No-op
  }

  /**
   * @deprecated use {@link #expandCachesForBlock()}
   */
  @Deprecated
  default void expandCodeCacheForBlock() {
    expandCachesForBlock();
  }

  /** Estimated number of analyzed {@link Code} entries in the code cache. */
  default long getCodeCacheSize() {
    return 0;
  }

  @Override
  default Code getIfPresent(final Hash codeHash) {
    return null;
  }

  @Override
  default void put(final Hash codeHash, final Code code) {
    // No-op
  }

  /**
   * Code load path: hit by {@code codeHash}, else load flat bytes, {@code put} the {@link Code}
   * instance (without forcing jump-dest analysis — that is done by BAL prefetch or lazily on
   * JUMPDEST checks), and return.
   */
  default Optional<Code> getCodeFromCacheOrStorage(
      final Hash codeHash, final Supplier<Optional<Bytes>> flatCodeLoader) {
    if (codeHash.equals(Hash.EMPTY)) {
      return Optional.of(Code.EMPTY_CODE);
    }
    final Code cached = getIfPresent(codeHash);
    if (cached != null) {
      return Optional.of(cached);
    }
    final Optional<Bytes> flat = flatCodeLoader.get();
    if (flat.isEmpty()) {
      return Optional.empty();
    }
    if (flat.get().isEmpty()) {
      return Optional.of(Code.EMPTY_CODE);
    }
    final Code code = new Code(flat.get(), codeHash);
    put(codeHash, code);
    return Optional.of(code);
  }

  /**
   * Drops an analyzed {@link Code} entry. Used when flat code is removed (e.g. account-hash code
   * strategy). Analyzed code is otherwise content-addressed and immutable for a given hash.
   */
  default void invalidateCode(final Hash codeHash) {
    // No-op
  }

  default Optional<Bytes> getFromCacheOrStorage(
      final SegmentIdentifier segment,
      final Bytes key,
      final long version,
      final Supplier<Optional<Bytes>> storageGetter) {
    // Always bypass cache and go directly to storage
    return storageGetter.get();
  }

  default List<Optional<Bytes>> getMultipleFromCacheOrStorage(
      final SegmentIdentifier segment,
      final List<Bytes> keys,
      final long version,
      final Function<List<Bytes>, List<Optional<Bytes>>> batchFetcher) {
    // Always bypass cache and go directly to storage
    return batchFetcher.apply(keys);
  }

  default void putInCache(
      final SegmentIdentifier segment, final Bytes key, final Bytes value, final long version) {
    // No-op
  }

  default void removeFromCache(
      final SegmentIdentifier segment, final Bytes key, final long version) {
    // No-op
  }

  default long getCacheSize(final SegmentIdentifier segment) {
    return 0;
  }

  default boolean isCached(final SegmentIdentifier segment, final Bytes key) {
    return false;
  }

  default Optional<VersionedValue> getCachedValue(
      final SegmentIdentifier segment, final Bytes key) {
    return Optional.empty();
  }

  /** Value wrapper with version and removal flag. */
  final class VersionedValue {
    final Bytes value;
    final long version;
    final boolean isRemoval;

    VersionedValue(final Bytes value, final long version, final boolean isRemoval) {
      this.value = value;
      this.version = version;
      this.isRemoval = isRemoval;
    }

    public Bytes getValue() {
      return value;
    }

    public long getVersion() {
      return version;
    }

    public boolean isRemoval() {
      return isRemoval;
    }
  }
}

/**
 * Holds bytes by reference and a precomputed hash; callers must not mutate the source array after
 * handing it off.
 */
final class CacheKey {
  private final byte[] data;
  private final int hashCode;

  static CacheKey of(final Bytes bytes) {
    return new CacheKey(bytes.toArrayUnsafe());
  }

  private CacheKey(final byte[] data) {
    this.data = data;
    this.hashCode = Arrays.hashCode(data);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof CacheKey that)) return false;
    return Arrays.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }
}
