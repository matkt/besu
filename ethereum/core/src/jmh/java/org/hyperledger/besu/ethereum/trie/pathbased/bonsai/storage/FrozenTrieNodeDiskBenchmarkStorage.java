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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_BACKGROUND_THREAD_COUNT;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_ENABLE_READ_CACHE_FOR_SNAPSHOTS;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_IS_HIGH_SPEC;
import static org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBCLIOptions.DEFAULT_MAX_OPEN_FILES;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBKeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** Opens on-disk RocksDB (Bonsai {@code TRIE_BRANCH_STORAGE} column) for trie-node read benchmarks. */
final class FrozenTrieNodeDiskBenchmarkStorage {

  private static final String METADATA_FILENAME = "DATABASE_METADATA.json";
  /** Small block cache so reads are less likely to be served only from RAM. */
  private static final long BENCHMARK_CACHE_CAPACITY = 8L * 1024 * 1024;

  private final RocksDBKeyValueStorageFactory factory;
  private final BesuConfiguration configuration;
  private final MetricsSystem metricsSystem = new NoOpMetricsSystem();

  private FrozenTrieNodeDiskBenchmarkStorage(
      final RocksDBKeyValueStorageFactory factory, final BesuConfiguration configuration) {
    this.factory = factory;
    this.configuration = configuration;
  }

  static FrozenTrieNodeDiskBenchmarkStorage create(final Path dataDirectory) throws IOException {
    Files.createDirectories(dataDirectory);
    writeBonsaiMetadata(dataDirectory);
    final RocksDBKeyValueStorageFactory factory =
        new RocksDBKeyValueStorageFactory(
            () ->
                new RocksDBFactoryConfiguration(
                    DEFAULT_MAX_OPEN_FILES,
                    DEFAULT_BACKGROUND_THREAD_COUNT,
                    BENCHMARK_CACHE_CAPACITY,
                    DEFAULT_IS_HIGH_SPEC,
                    DEFAULT_ENABLE_READ_CACHE_FOR_SNAPSHOTS,
                    false,
                    Optional.empty(),
                    Optional.empty()),
            Arrays.asList(KeyValueSegmentIdentifier.values()),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
    return new FrozenTrieNodeDiskBenchmarkStorage(factory, besuConfiguration(dataDirectory));
  }

  SegmentedKeyValueStorage openTrieBranchStorage() {
    return factory.create(List.of(TRIE_BRANCH_STORAGE), configuration, metricsSystem);
  }

  static void deleteRecursively(final Path root) throws IOException {
    if (root == null || !Files.exists(root)) {
      return;
    }
    try (var paths = Files.walk(root)) {
      paths.sorted((a, b) -> b.compareTo(a))
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (final IOException ignored) {
                  // best effort
                }
              });
    }
  }

  private static void writeBonsaiMetadata(final Path dataDirectory) throws IOException {
    final String content = "{\"v2\":{\"format\":\"BONSAI\",\"version\":2}}";
    Files.writeString(dataDirectory.resolve(METADATA_FILENAME), content, StandardCharsets.UTF_8);
  }

  private static BesuConfiguration besuConfiguration(final Path dataPath) {
    final DataStorageConfiguration dataStorageConfiguration =
        new DataStorageConfiguration() {
          @Override
          public DataStorageFormat getDatabaseFormat() {
            return DataStorageFormat.BONSAI;
          }

          @Override
          public boolean getReceiptCompactionEnabled() {
            return true;
          }
        };
    return new BesuConfiguration() {
      @Override
      public String getConfiguredRpcHttpHost() {
        return "localhost";
      }

      @Override
      public long getConfiguredRpcHttpTimeoutSec() {
        return 60;
      }

      @Override
      public Integer getConfiguredRpcHttpPort() {
        return 8545;
      }

      @Override
      public Path getStoragePath() {
        return dataPath;
      }

      @Override
      public Path getDataPath() {
        return dataPath;
      }

      @Override
      public DataStorageFormat getDatabaseFormat() {
        return DataStorageFormat.BONSAI;
      }

      @Override
      public Wei getMinGasPrice() {
        return Wei.ZERO;
      }

      @Override
      public DataStorageConfiguration getDataStorageConfiguration() {
        return dataStorageConfiguration;
      }
    };
  }
}
