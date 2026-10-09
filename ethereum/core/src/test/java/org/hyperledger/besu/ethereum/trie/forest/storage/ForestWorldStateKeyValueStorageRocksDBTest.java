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
package org.hyperledger.besu.ethereum.trie.forest.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.TransactionDBRocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;

import java.nio.file.Path;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Forest storage on the RocksDB TransactionDB it uses in production, with pessimistic locks. */
class ForestWorldStateKeyValueStorageRocksDBTest {

  @TempDir private Path dataDir;

  @Test
  void openUpdatersWritingTheSameNodeDoNotBlockEachOther() throws Exception {
    try (final SegmentedKeyValueStorage database =
        new TransactionDBRocksDBColumnarKeyValueStorage(
            new RocksDBConfigurationBuilder().databaseDir(dataDir).build(),
            List.of(KeyValueSegmentIdentifier.DEFAULT, KeyValueSegmentIdentifier.WORLD_STATE),
            List.of(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS)) {
      final ForestWorldStateKeyValueStorage storage =
          new ForestWorldStateKeyValueStorage(
              new SegmentedKeyValueStorageAdapter(KeyValueSegmentIdentifier.WORLD_STATE, database));
      final Bytes node = Bytes.of(1, 2, 3);
      final Bytes32 nodeHash = Bytes32.wrap(Hash.hash(node).getBytes());

      // two concurrent writers of the same content-addressed node, like the snap sync persist steps
      final ForestWorldStateKeyValueStorage.Updater first = storage.updater();
      first.putAccountStorageTrieNode(nodeHash, node);
      final ForestWorldStateKeyValueStorage.Updater second = storage.updater();

      final long start = System.nanoTime();
      assertThatCode(() -> second.putAccountStorageTrieNode(nodeHash, node))
          .doesNotThrowAnyException();
      assertThat(System.nanoTime() - start).isLessThan(500_000_000L);

      second.commit();
      first.commit();
      assertThat(storage.getAccountStorageTrieNode(nodeHash)).contains(node);
    }
  }
}
