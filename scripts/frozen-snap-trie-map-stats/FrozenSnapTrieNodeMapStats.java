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

/**
 * Read-only stats for {@code frozen_snap_trie_nodes/snap_trie_nodes.dat}.
 */
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
    final Path mapPath = directory.resolve(FrozenSnapTrieNodeChronicleConfig.MAP_FILE_NAME);
    if (!Files.isRegularFile(mapPath)) {
      System.err.println("Map file not found: " + mapPath);
      System.exit(1);
    }

    final boolean frozen = Files.exists(directory.resolve(FROZEN_MARKER));
    final ChronicleMap<byte[], byte[]> map = openReadOnly(mapPath.toFile());
    try {
      final long longSize = map.longSize();
      System.out.printf("directory=%s%n", directory.toAbsolutePath());
      System.out.printf("mapFile=%s%n", mapPath.getFileName());
      System.out.printf("frozen=%s%n", frozen);
      System.out.printf("file_bytes=%d%n", Files.size(mapPath));
      System.out.printf("longSize=%d%n", longSize);
      if (scanSample) {
        final AtomicLong scanned = new AtomicLong();
        map.forEach((k, v) -> scanned.incrementAndGet());
        System.out.printf("scanCount=%d%n", scanned.get());
      }
    } finally {
      map.close();
    }
  }

  private static ChronicleMap<byte[], byte[]> openReadOnly(final File mapFile)
      throws java.io.IOException {
    return FrozenSnapTrieNodeChronicleConfig.newBuilder().recoverPersistedTo(mapFile, true);
  }
}
