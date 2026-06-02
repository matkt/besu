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
package org.hyperledger.besu.services.kvstore;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

/**
 * Chronicle Map sizing for snap-sync frozen trie nodes. Persisted maps embed these parameters;
 * changing them requires deleting {@code frozen_snap_trie_nodes} and re-running snap sync.
 *
 * <p>MPT branch nodes are RLP lists of up to 17 items (16 child hashes + optional value slot), each
 * up to 32 bytes. A dense branch is ~550–800+ bytes RLP; leaves and extensions are smaller, hence a
 * sub-kilobyte average.
 */
public final class FrozenSnapTrieNodeChronicleConfig {

  public static final String MAP_NAME = "snap-trie-nodes";
  public static final int KEY_SIZE_BYTES = 32;

  /** Blend of small leaves/extensions and larger branches. */
  public static final int AVERAGE_VALUE_SIZE_BYTES = 384;

  /** Room for a full branch RLP plus margin (Chronicle uses {@link #CHRONICLE_CHUNK_SIZE_BYTES}). */
  public static final int MAX_VALUE_SIZE_BYTES = 1024;

  public static final int CHRONICLE_CHUNK_SIZE_BYTES = 32;

  /**
   * Modest initial capacity to limit mmap/RSS; grows on disk via {@link #MAX_BLOAT_FACTOR} and
   * {@link #USE_SPARSE_FILE} as entries are added.
   */
  public static final long INITIAL_ENTRIES = 2_000_000L;

  /** Effective entry ceiling scales up to roughly {@code INITIAL_ENTRIES * MAX_BLOAT_FACTOR}. */
  public static final double MAX_BLOAT_FACTOR = 16.0;

  /**
   * Sparse persisted file: unwritten regions do not reserve disk or RSS until touched (Linux).
   */
  public static final boolean USE_SPARSE_FILE = true;

  public static final int MAX_CHUNKS_PER_ENTRY =
      (MAX_VALUE_SIZE_BYTES + CHRONICLE_CHUNK_SIZE_BYTES - 1) / CHRONICLE_CHUNK_SIZE_BYTES;

  private FrozenSnapTrieNodeChronicleConfig() {}

  public static ChronicleMapBuilder<byte[], byte[]> newBuilder() {
    return ChronicleMap.of(byte[].class, byte[].class)
        .name(MAP_NAME)
        .entries(INITIAL_ENTRIES)
        .averageKeySize(KEY_SIZE_BYTES)
        .averageValueSize(AVERAGE_VALUE_SIZE_BYTES)
        .maxBloatFactor(MAX_BLOAT_FACTOR)
        .maxChunksPerEntry(MAX_CHUNKS_PER_ENTRY)
        .sparseFile(USE_SPARSE_FILE)
        .checksumEntries(false);
  }
}
