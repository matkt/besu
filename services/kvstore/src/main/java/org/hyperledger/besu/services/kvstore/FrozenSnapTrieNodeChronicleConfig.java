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

import org.apache.tuweni.bytes.Bytes32;

/**
 * Chronicle Map sizing for snap-sync frozen trie nodes. Persisted maps embed these parameters;
 * changing them requires deleting {@code frozen_snap_trie_nodes} and re-running snap sync.
 *
 * <p>Writes are sharded across {@link #NUM_SHARDS} files so snap-sync pipeline threads can persist
 * in parallel (one Chronicle file per shard, each with a dedicated writer thread).
 */
public final class FrozenSnapTrieNodeChronicleConfig {

  public static final String MAP_FILE_PREFIX = "snap_trie_nodes";
  public static final String MAP_NAME_PREFIX = "snap-trie-nodes";
  public static final int KEY_SIZE_BYTES = 32;

  /** Parallel shard count (must be a power of two). */
  public static final int NUM_SHARDS = 16;

  /** Blend of small leaves/extensions and larger branches. */
  public static final int AVERAGE_VALUE_SIZE_BYTES = 384;

  public static final int MAX_VALUE_SIZE_BYTES = 1024;
  public static final int CHRONICLE_CHUNK_SIZE_BYTES = 32;

  /** Per-shard entry capacity; total ≈ {@code ENTRIES_PER_SHARD * NUM_SHARDS}. */
  public static final long ENTRIES_PER_SHARD = 2_000_000L;

  public static final double MAX_BLOAT_FACTOR = 16.0;
  public static final boolean USE_SPARSE_FILE = true;

  public static final int MAX_CHUNKS_PER_ENTRY =
      (MAX_VALUE_SIZE_BYTES + CHRONICLE_CHUNK_SIZE_BYTES - 1) / CHRONICLE_CHUNK_SIZE_BYTES;

  private FrozenSnapTrieNodeChronicleConfig() {}

  public static int shardIndex(final Bytes32 hash) {
    return hash.get(31) & (NUM_SHARDS - 1);
  }

  public static String shardFileName(final int shardIndex) {
    return String.format("%s_%02x.dat", MAP_FILE_PREFIX, shardIndex);
  }

  public static ChronicleMapBuilder<byte[], byte[]> newBuilderForShard(final int shardIndex) {
    return ChronicleMap.of(byte[].class, byte[].class)
        .name(MAP_NAME_PREFIX + "-" + shardIndex)
        .entries(ENTRIES_PER_SHARD)
        .averageKeySize(KEY_SIZE_BYTES)
        .averageValueSize(AVERAGE_VALUE_SIZE_BYTES)
        .maxBloatFactor(MAX_BLOAT_FACTOR)
        .maxChunksPerEntry(MAX_CHUNKS_PER_ENTRY)
        .sparseFile(USE_SPARSE_FILE)
        .checksumEntries(false);
  }
}
