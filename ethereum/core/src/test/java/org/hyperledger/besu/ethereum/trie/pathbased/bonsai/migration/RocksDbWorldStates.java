/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration;

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.OptimisticRocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** RocksDB databases with the Bonsai column families under a directory, closed together. */
public final class RocksDbWorldStates implements AutoCloseable {

  private final Path dir;
  private final List<SegmentedKeyValueStorage> opened = new ArrayList<>();

  public RocksDbWorldStates(final Path dir) {
    this.dir = dir;
  }

  /** A new database, in a directory of its own named after {@code name}. */
  public SegmentedKeyValueStorage database(final String name) {
    final Path databaseDir = dir.resolve(name + "-" + opened.size());
    final SegmentedKeyValueStorage database =
        new OptimisticRocksDBColumnarKeyValueStorage(
            new RocksDBConfigurationBuilder().databaseDir(databaseDir).build(),
            Arrays.stream(KeyValueSegmentIdentifier.values())
                .filter(segment -> segment.includeInDatabaseFormat(DataStorageFormat.BONSAI))
                .<SegmentIdentifier>map(segment -> segment)
                .toList(),
            List.of(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
    opened.add(database);
    return database;
  }

  /** The Bonsai storage of {@code database}. */
  public static BonsaiWorldStateKeyValueStorage worldStateStorage(
      final SegmentedKeyValueStorage database) {
    return (BonsaiWorldStateKeyValueStorage)
        new KeyValueStorageProvider(
                segments -> database, new InMemoryKeyValueStorage(), new NoOpMetricsSystem())
            .createWorldStateStorage(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  }

  @Override
  public void close() throws IOException {
    for (final SegmentedKeyValueStorage database : opened) {
      database.close();
    }
  }
}
