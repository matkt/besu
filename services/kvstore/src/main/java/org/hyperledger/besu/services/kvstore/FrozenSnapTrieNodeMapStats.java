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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Prints entry count and on-disk size for a snap-sync frozen trie node PlainTable RocksDB. Usage:
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
    final Path dbDirectory = resolveDbDirectory(Path.of(args[0]));
    if (!Files.isDirectory(dbDirectory)) {
      System.err.println("No RocksDB found under " + dbDirectory);
      System.exit(1);
    }

    final boolean frozen = Files.exists(dbDirectory.resolve(FROZEN_MARKER_FILE));

    long totalBytes = 0;
    try (Stream<Path> stream = Files.walk(dbDirectory)) {
      totalBytes =
          stream
              .filter(Files::isRegularFile)
              .mapToLong(
                  p -> {
                    try {
                      return Files.size(p);
                    } catch (final IOException e) {
                      return 0L;
                    }
                  })
              .sum();
    }

    long entries = 0;
    try (FrozenSnapTrieNodeStorage storage = FrozenSnapTrieNodeStorage.open(dbDirectory)) {
      entries = storage.entryCount();
    }

    System.out.println("directory=" + dbDirectory.toAbsolutePath());
    System.out.println("frozen=" + frozen);
    System.out.println("entries=" + entries);
    System.out.println("bytesOnDisk=" + totalBytes);
  }

  static Path resolveDbDirectory(final Path input) {
    // Check if a RocksDB CURRENT file exists directly in the given directory
    if (Files.isRegularFile(input.resolve("CURRENT"))) {
      return input;
    }
    // Check the default sub-directory
    final Path nested = input.resolve(DEFAULT_SUBDIR);
    if (Files.isRegularFile(nested.resolve("CURRENT"))) {
      return nested;
    }
    return input;
  }
}
