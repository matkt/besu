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
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import net.openhft.chronicle.map.ChronicleMap;

import org.hyperledger.besu.services.kvstore.FrozenSnapTrieNodeChronicleConfig;

/** Read-only stats for sharded {@code frozen_snap_trie_nodes/snap_trie_nodes_XX.dat}. */
public final class FrozenSnapTrieNodeMapStats {

  private static final String FROZEN_MARKER = ".frozen";

  private FrozenSnapTrieNodeMapStats() {}

  public static void main(final String[] args) throws Exception {
    boolean scanSample = false;
    Path input = null;
    for (final String arg : args) {
      if ("--scan-sample".equals(arg)) {
        scanSample = true;
      } else if (!arg.startsWith("-")) {
        input = Path.of(arg);
      } else {
        System.err.println("Unknown option: " + arg);
        System.exit(2);
      }
    }
    if (input == null) {
      System.err.println(
          "Usage: FrozenSnapTrieNodeMapStats [--scan-sample] <frozen_snap_trie_nodes-dir>");
      System.exit(2);
    }

    final Path directory = Files.isDirectory(input) ? input : input.getParent();
    if (!Files.isDirectory(directory)) {
      System.err.println("Directory not found: " + directory);
      System.exit(1);
    }

    final boolean frozen = Files.exists(directory.resolve(FROZEN_MARKER));
    long totalFileBytes = 0;
    long totalLongSize = 0;
    int openShards = 0;

    if (Files.isRegularFile(directory.resolve(FrozenSnapTrieNodeChronicleConfig.LEGACY_MAP_FILE_NAME))) {
      System.err.println("note=legacy single-file map present; stats below are for shard files only");
    }

    for (int shard = 0; shard < FrozenSnapTrieNodeChronicleConfig.NUM_SHARDS; shard++) {
      final Path mapPath = directory.resolve(FrozenSnapTrieNodeChronicleConfig.shardFileName(shard));
      if (!Files.isRegularFile(mapPath)) {
        continue;
      }
      openShards++;
      totalFileBytes += Files.size(mapPath);
      final ChronicleMap<byte[], byte[]> map = openReadOnly(mapPath.toFile(), shard);
      try {
        final long longSize = map.longSize();
        totalLongSize += longSize;
        System.out.printf(
            "shard=%02x path=%s file_bytes=%d longSize=%d%n",
            shard, mapPath, Files.size(mapPath), longSize);
        if (scanSample) {
          final AtomicLong scanned = new AtomicLong();
          map.forEach((k, v) -> scanned.incrementAndGet());
          System.out.printf("shard=%02x scanCount=%d%n", shard, scanned.get());
        }
      } finally {
        map.close();
      }
    }

    if (openShards == 0) {
      System.err.println("No shard map files found under " + directory);
      System.exit(1);
    }

    System.out.printf("directory=%s%n", directory.toAbsolutePath());
    System.out.printf("frozen=%s%n", frozen);
    System.out.printf("shards=%d%n", openShards);
    System.out.printf("total_file_bytes=%d%n", totalFileBytes);
    System.out.printf("total_longSize=%d%n", totalLongSize);
  }

  private static ChronicleMap<byte[], byte[]> openReadOnly(final File mapFile, final int shardIndex)
      throws java.io.IOException {
    return FrozenSnapTrieNodeChronicleConfig.newBuilderForShard(shardIndex)
        .recoverPersistedTo(mapFile, true);
  }
}
