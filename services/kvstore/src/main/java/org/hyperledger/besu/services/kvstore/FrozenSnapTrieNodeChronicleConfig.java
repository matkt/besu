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
 * Chronicle Map sizing for snap-sync frozen trie nodes (single file {@link #MAP_FILE_NAME}).
 *
 * <p>Memory is dominated by mmap of the map file. {@link #INITIAL_ENTRIES} and {@link
 * #MAX_BLOAT_FACTOR} bound reserved / tier growth; {@link #USE_SPARSE_FILE} keeps on-disk size from
 * committing unused pages to RSS until written. Changing these requires deleting {@code
 * frozen_snap_trie_nodes} and re-running snap sync.
 */
public final class FrozenSnapTrieNodeChronicleConfig {

  public static final String MAP_FILE_NAME = "snap_trie_nodes.dat";
  public static final String MAP_NAME = "snap-trie-nodes";
  public static final int KEY_SIZE_BYTES = 32;

  /** Blend of small leaves/extensions and larger branches. */
  public static final int AVERAGE_VALUE_SIZE_BYTES = 384;

  public static final int MAX_VALUE_SIZE_BYTES = 1024;
  public static final int CHRONICLE_CHUNK_SIZE_BYTES = 32;

  /**
   * Expected entry count at create time (lower = less upfront mmap). The map grows when full but
   * extra tiers cost memory; tune for your chain (e.g. ~1M for testnets, raise for mainnet snap).
   */
  public static final long INITIAL_ENTRIES = 1_000_000L;

  /** Caps tier expansion vs {@link #INITIAL_ENTRIES} to limit mmap growth. */
  public static final double MAX_BLOAT_FACTOR = 4.0;

  /** Sparse file: large on-disk footprint does not map all pages into RAM until touched. */
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
