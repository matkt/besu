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

import java.io.Closeable;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

/**
 * Mutable MapDB instance backing the active canonical head cache. This is the only MapDB instance
 * that accepts writes.
 */
public final class ActiveMapDbHeadCache implements Closeable {

  static final String DATA_MAP = "head-cache-data";
  static final String META_MAP = "head-cache-meta";

  private final DB db;
  private final HTreeMap<byte[], byte[]> dataMap;
  private final HTreeMap<byte[], byte[]> metaMap;
  private volatile boolean closed;

  ActiveMapDbHeadCache() {
    this.db =
        DBMaker.memoryDB().concurrencyScale(16).make();
    this.dataMap =
        db.hashMap(DATA_MAP, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY).createOrOpen();
    this.metaMap =
        db.hashMap(META_MAP, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY).createOrOpen();
  }

  DB db() {
    return db;
  }

  HTreeMap<byte[], byte[]> dataMap() {
    return dataMap;
  }

  Optional<Bytes> getData(final Bytes encodedKey) {
    if (closed) {
      return Optional.empty();
    }
    final byte[] raw = dataMap.get(encodedKey.toArrayUnsafe());
    return raw == null ? Optional.empty() : Optional.of(Bytes.wrap(raw));
  }

  void putData(final Bytes encodedKey, final Bytes value) {
    if (closed) {
      throw new IllegalStateException("Active head MapDB cache is closed");
    }
    dataMap.put(encodedKey.toArrayUnsafe(), value.toArrayUnsafe());
  }

  void removeData(final Bytes encodedKey) {
    if (closed) {
      throw new IllegalStateException("Active head MapDB cache is closed");
    }
    dataMap.remove(encodedKey.toArrayUnsafe());
  }

  void putMeta(final Bytes metaKey, final Bytes value) {
    if (closed) {
      throw new IllegalStateException("Active head MapDB cache is closed");
    }
    metaMap.put(metaKey.toArrayUnsafe(), value.toArrayUnsafe());
  }

  Optional<Bytes> getMeta(final Bytes metaKey) {
    if (closed) {
      return Optional.empty();
    }
    final byte[] raw = metaMap.get(metaKey.toArrayUnsafe());
    return raw == null ? Optional.empty() : Optional.of(Bytes.wrap(raw));
  }

  void removeMeta(final Bytes metaKey) {
    if (closed) {
      return;
    }
    metaMap.remove(metaKey.toArrayUnsafe());
  }

  void commit() {
    if (!closed) {
      db.commit();
    }
  }

  /** Eviction metadata: last block number when entry was accessed or written. */
  void touchAccessBlock(final Bytes dataKey, final long blockNumber) {
    putMeta(
        accessMetaKey(dataKey),
        Bytes.ofUnsignedLong(blockNumber));
  }

  Optional<Long> lastAccessBlock(final Bytes dataKey) {
    return getMeta(accessMetaKey(dataKey))
        .flatMap(
            b -> {
              if (b.size() < 8) {
                return Optional.empty();
              }
              return Optional.of(b.getLong(0));
            });
  }

  static Bytes accessMetaKey(final Bytes dataKey) {
    return HeadMapDbCacheCategory.ACCESS_METADATA.encodeKey(dataKey);
  }

  Iterable<Bytes> dataKeys() {
    return () ->
        dataMap.keySet().stream()
            .map(Bytes::wrap)
            .filter(
                b ->
                    b.size() > 0
                        && b.get(0) != HeadMapDbCacheCategory.ACCESS_METADATA.id()
                        && b.get(0) != HeadMapDbCacheCategory.FLAT_ACCOUNT_RESOLUTION.id()
                        && b.get(0) != HeadMapDbCacheCategory.TRIE_NODE_RESOLUTION.id())
            .iterator();
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      db.close();
    }
  }
}
