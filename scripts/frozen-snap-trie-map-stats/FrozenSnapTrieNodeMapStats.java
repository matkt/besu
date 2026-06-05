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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/** Read-only stats for the frozen snap-sync trie node PlainTable RocksDB. */
public final class FrozenSnapTrieNodeMapStats {

  private static final String FROZEN_MARKER = ".frozen";
  private static final String DEFAULT_SUBDIR = "frozen_snap_trie_nodes";

  private FrozenSnapTrieNodeMapStats() {}

  public static void main(final String[] args) throws Exception {
    if (args.length != 1 || args[0].startsWith("-")) {
      System.err.println(
          "Usage: FrozenSnapTrieNodeMapStats <besu-data-dir|frozen_snap_trie_nodes-dir>");
      System.exit(2);
    }

    RocksDB.loadLibrary();

    final Path dbDirectory = resolveDbDirectory(Path.of(args[0]));
    if (!Files.isDirectory(dbDirectory)) {
      System.err.println("No RocksDB directory found under " + dbDirectory);
      System.exit(1);
    }

    final boolean frozen = Files.exists(dbDirectory.resolve(FROZEN_MARKER));

    long totalBytes = 0;
    try (Stream<Path> stream = Files.walk(dbDirectory)) {
      totalBytes =
          stream
              .filter(Files::isRegularFile)
              .mapToLong(
                  p -> {
                    try {
                      return Files.size(p);
                    } catch (IOException e) {
                      return 0L;
                    }
                  })
              .sum();
    }

    final long entries = readEntryCount(dbDirectory);

    System.out.printf("directory=%s%n", dbDirectory.toAbsolutePath());
    System.out.printf("frozen=%s%n", frozen);
    System.out.printf("entries=%d%n", entries);
    System.out.printf("bytesOnDisk=%d%n", totalBytes);
  }

  private static long readEntryCount(final Path dbDirectory) throws RocksDBException {
    final PlainTableConfig tableConfig =
        new PlainTableConfig()
            .setKeySize(32)
            .setBloomBitsPerKey(10)
            .setHashTableRatio(0)
            .setIndexSparseness(16)
            .setFullScanMode(false)
            .setStoreIndexInFile(false);

    try (DBOptions dbOptions =
            new DBOptions()
                .setCreateIfMissing(false)
                .setAllowMmapReads(true)
                .setAllowMmapWrites(false)
                .setMaxOpenFiles(-1);
        org.rocksdb.ColumnFamilyOptions cfOptions =
            new org.rocksdb.ColumnFamilyOptions().setTableFormatConfig(tableConfig)) {

      final List<ColumnFamilyDescriptor> cfDescriptors =
          List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

      try (RocksDB db =
          RocksDB.openReadOnly(dbOptions, dbDirectory.toString(), cfDescriptors, cfHandles)) {
        final ColumnFamilyHandle cfHandle = cfHandles.get(0);
        try {
          final String prop = db.getProperty(cfHandle, "rocksdb.estimate-num-keys");
          return prop == null ? 0L : Long.parseLong(prop.trim());
        } finally {
          cfHandle.close();
        }
      }
    }
  }

  private static Path resolveDbDirectory(final Path input) {
    if (Files.isRegularFile(input.resolve("CURRENT"))) {
      return input;
    }
    final Path nested = input.resolve(DEFAULT_SUBDIR);
    if (Files.isRegularFile(nested.resolve("CURRENT"))) {
      return nested;
    }
    return input;
  }
}
