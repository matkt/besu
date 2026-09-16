/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.plugin.services.storage.rocksdb.configuration;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.BLOCKCHAIN;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.LRUCache;

class RocksDBColumnFamilyCustomPropertyApplierTest {

  @Test
  void disablesPrefetchForBonsaiStateByEnablingIndexFilterCache() {
    try (final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        final LRUCache cache = new LRUCache(1_048_576)) {
      final BlockBasedTableConfig tableConfig =
          new BlockBasedTableConfig().setBlockCache(cache).setCacheIndexAndFilterBlocks(false);

      RocksDBColumnFamilyCustomPropertyApplier.apply(
          ACCOUNT_INFO_STATE, tableConfig, cfOptions, false, List.of());

      assertTrue(tableConfig.cacheIndexAndFilterBlocks());
    }
  }

  @Test
  void leavesBlockchainTableConfigWhenPrefetchEnabled() {
    try (final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        final LRUCache cache = new LRUCache(1_048_576)) {
      final BlockBasedTableConfig tableConfig =
          new BlockBasedTableConfig().setBlockCache(cache).setCacheIndexAndFilterBlocks(false);

      RocksDBColumnFamilyCustomPropertyApplier.apply(
          BLOCKCHAIN, tableConfig, cfOptions, true, List.of());

      assertFalse(tableConfig.cacheIndexAndFilterBlocks());
    }
  }

  @Test
  void appliesSegmentScopedCustomProperty() {
    try (final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        final LRUCache cache = new LRUCache(1_048_576)) {
      final BlockBasedTableConfig tableConfig =
          new BlockBasedTableConfig().setBlockCache(cache).setCacheIndexAndFilterBlocks(false);

      RocksDBColumnFamilyCustomPropertyApplier.apply(
          KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
          tableConfig,
          cfOptions,
          true,
          List.of("0x09:block_based_table_factory.cache_index_and_filter_blocks=true"));

      assertTrue(tableConfig.cacheIndexAndFilterBlocks());
    }
  }
}
