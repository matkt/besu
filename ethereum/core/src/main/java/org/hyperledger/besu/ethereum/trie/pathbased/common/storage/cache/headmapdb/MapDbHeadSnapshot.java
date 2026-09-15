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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Immutable point-in-time view of the head MapDB cache.
 *
 * <p>MapDB 3.1 does not expose a native snapshot API for in-memory databases; this class materializes
 * an immutable copy of the active cache data map at creation time.
 */
public final class MapDbHeadSnapshot implements Closeable {

  private final Map<Bytes, Bytes> frozenEntries;
  private volatile boolean closed;

  MapDbHeadSnapshot(final ActiveMapDbHeadCache activeCache) {
    final Map<Bytes, Bytes> copy = new HashMap<>();
    for (final byte[] key : activeCache.dataMap().keySet()) {
      final byte[] value = activeCache.dataMap().get(key);
      if (value != null) {
        copy.put(Bytes.wrap(key), Bytes.wrap(value));
      }
    }
    this.frozenEntries = Map.copyOf(copy);
  }

  Optional<Bytes> get(final Bytes encodedKey) {
    if (closed) {
      return Optional.empty();
    }
    return Optional.ofNullable(frozenEntries.get(encodedKey));
  }

  @Override
  public void close() {
    closed = true;
  }

  public boolean isClosed() {
    return closed;
  }
}
