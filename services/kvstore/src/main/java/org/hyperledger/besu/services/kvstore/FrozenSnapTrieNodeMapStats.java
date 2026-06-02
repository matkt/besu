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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import net.openhft.chronicle.map.ChronicleMap;

/**
 * Prints entry count and on-disk size for a snap-sync frozen trie node Chronicle Map. Usage:
 *
 * <pre>
 * frozen-snap-trie-map-stats.sh /path/to/data-dir
 * frozen-snap-trie-map-stats.sh /path/to/data-dir/frozen_snap_trie_nodes
 * </pre>
 */
public final class FrozenSnapTrieNodeMapStats {

  private static final String FROZEN_MARKER_FILE = ".frozen";
  private static final String DEFAULT_SUBDIR = "frozen_snap_trie_nodes";

  private FrozenSnapTrieNodeMapStats() {}

  public static void main(final String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println(
          "Usage: FrozenSnapTrieNodeMapStats <besu-data-dir|frozen_snap_trie_nodes-dir>");
      System.exit(2);
    }
    final Path mapDirectory = resolveMapDirectory(Path.of(args[0]));
    final boolean frozen = Files.exists(mapDirectory.resolve(FROZEN_MARKER_FILE));
    long totalBytes = 0;
    long totalEntries = 0;
    int shards = 0;
    for (int shard = 0; shard < FrozenSnapTrieNodeChronicleConfig.NUM_SHARDS; shard++) {
      final Path mapFile = mapDirectory.resolve(FrozenSnapTrieNodeChronicleConfig.shardFileName(shard));
      if (!Files.isRegularFile(mapFile)) {
        continue;
      }
      shards++;
      totalBytes += Files.size(mapFile);
      try (ChronicleMap<byte[], byte[]> map = openReadOnly(mapFile.toFile(), shard)) {
        totalEntries += map.longSize();
      }
    }
    if (shards == 0) {
      System.err.println("No shard map files found under " + mapDirectory);
      System.exit(1);
    }

    System.out.println("directory=" + mapDirectory.toAbsolutePath());
    System.out.println("frozen=" + frozen);
    System.out.println("shards=" + shards);
    System.out.println("entries=" + totalEntries);
    System.out.println("bytesOnDisk=" + totalBytes);
  }

  static Path resolveMapDirectory(final Path input) {
    for (int shard = 0; shard < FrozenSnapTrieNodeChronicleConfig.NUM_SHARDS; shard++) {
      if (Files.isRegularFile(input.resolve(FrozenSnapTrieNodeChronicleConfig.shardFileName(shard)))) {
        return input;
      }
    }
    if (input.getFileName() != null
        && DEFAULT_SUBDIR.equals(input.getFileName().toString())) {
      return input;
    }
    final Path nested = input.resolve(DEFAULT_SUBDIR);
    for (int shard = 0; shard < FrozenSnapTrieNodeChronicleConfig.NUM_SHARDS; shard++) {
      if (Files.isRegularFile(nested.resolve(FrozenSnapTrieNodeChronicleConfig.shardFileName(shard)))) {
        return nested;
      }
    }
    return input;
  }

  private static ChronicleMap<byte[], byte[]> openReadOnly(final File mapFile, final int shard)
      throws IOException {
    return FrozenSnapTrieNodeChronicleConfig.newBuilderForShard(shard)
        .recoverPersistedTo(mapFile, true);
  }
}
