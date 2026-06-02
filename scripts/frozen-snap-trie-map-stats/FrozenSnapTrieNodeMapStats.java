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

/**
 * Read-only stats for {@code frozen_snap_trie_nodes/snap_trie_nodes.dat}. Builder settings must
 * match {@code ChronicleMapFrozenSnapTrieNodeStorage}.
 */
public final class FrozenSnapTrieNodeMapStats {

  private static final String MAP_FILE_NAME = "snap_trie_nodes.dat";
  private static final String FROZEN_MARKER = ".frozen";
  private static final int KEY_SIZE_BYTES = 32;
  private static final int AVERAGE_VALUE_SIZE_BYTES = 128;
  private static final long DEFAULT_MAX_ENTRIES = 64_000_000L;

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
          "Usage: FrozenSnapTrieNodeMapStats [--scan-sample] <data-dir-or-map-file>");
      System.exit(2);
    }

    final Path mapPath = resolveMapPath(input);
    if (!Files.isRegularFile(mapPath)) {
      System.err.println("Map file not found: " + mapPath);
      System.exit(1);
    }
    final Path directory = mapPath.getParent();
    final boolean frozen = Files.exists(directory.resolve(FROZEN_MARKER));
    final long fileBytes = Files.size(mapPath);

    if (!frozen) {
      System.err.println(
          "note=frozen=false: map may still be open for writing by Besu; longSize() can read 0"
              + " until the file is reopened read-only. Prefer stopping Besu, or use --scan-sample.");
    }
    if (fileBytes > 1_000_000_000L) {
      System.err.println(
          "note=large file_bytes with few entries usually means Chronicle pre-allocated the map"
              + " (64M max entries); file size is not entry count.");
    }

    final ChronicleMap<byte[], byte[]> map = openReadOnly(mapPath.toFile());
    try {
      final long longSize = map.longSize();
      final int mapSize = map.size();

      System.out.printf("path=%s%n", mapPath.toAbsolutePath());
      System.out.printf("frozen=%s%n", frozen);
      System.out.printf("file_bytes=%d%n", fileBytes);
      System.out.printf("longSize=%d%n", longSize);
      System.out.printf("mapSize=%d%n", mapSize);

      if (scanSample) {
        final AtomicLong scanned = new AtomicLong();
        map.forEach((k, v) -> scanned.incrementAndGet());
        System.out.printf("scanCount=%d%n", scanned.get());
      }
    } finally {
      map.close();
    }
  }

  private static Path resolveMapPath(final Path input) {
    if (Files.isDirectory(input)) {
      return input.resolve(MAP_FILE_NAME);
    }
    if (input.getFileName().toString().equals(MAP_FILE_NAME)) {
      return input;
    }
    return input.resolve(MAP_FILE_NAME);
  }

  private static ChronicleMap<byte[], byte[]> openReadOnly(final File mapFile)
      throws java.io.IOException {
    return ChronicleMap.of(byte[].class, byte[].class)
        .name("snap-trie-nodes")
        .entries(DEFAULT_MAX_ENTRIES)
        .averageKeySize(KEY_SIZE_BYTES)
        .averageValueSize(AVERAGE_VALUE_SIZE_BYTES)
        .recoverPersistedTo(mapFile, true);
  }
}
