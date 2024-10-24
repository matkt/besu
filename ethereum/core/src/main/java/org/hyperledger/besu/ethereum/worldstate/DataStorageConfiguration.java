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
package org.hyperledger.besu.ethereum.worldstate;

import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import org.immutables.value.Value;

@Value.Immutable
@Value.Enclosing
public interface DataStorageConfiguration {

  long DEFAULT_DIFFBASED_MAX_LAYERS_TO_LOAD = 512;
  boolean DEFAULT_DIFFBASED_LIMIT_TRIE_LOGS_ENABLED = true;
  long MINIMUM_DIFFBASED_TRIE_LOG_RETENTION_LIMIT = DEFAULT_DIFFBASED_MAX_LAYERS_TO_LOAD;
  int DEFAULT_DIFFBASED_TRIE_LOG_PRUNING_WINDOW_SIZE = 5_000;
  boolean DEFAULT_RECEIPT_COMPACTION_ENABLED = true;

  DataStorageConfiguration DEFAULT_CONFIG =
      ImmutableDataStorageConfiguration.builder()
          .dataStorageFormat(DataStorageFormat.BONSAI)
          .diffbasedMaxLayersToLoad(DEFAULT_DIFFBASED_MAX_LAYERS_TO_LOAD)
          .unstable(Unstable.DEFAULT)
          .build();

  DataStorageConfiguration DEFAULT_BONSAI_CONFIG =
      ImmutableDataStorageConfiguration.builder()
          .dataStorageFormat(DataStorageFormat.BONSAI)
          .diffbasedMaxLayersToLoad(DEFAULT_DIFFBASED_MAX_LAYERS_TO_LOAD)
          .build();

  DataStorageConfiguration DEFAULT_BONSAI_PARTIAL_DB_CONFIG =
      ImmutableDataStorageConfiguration.builder()
          .dataStorageFormat(DataStorageFormat.BONSAI)
          .diffbasedMaxLayersToLoad(DEFAULT_DIFFBASED_MAX_LAYERS_TO_LOAD)
          .unstable(Unstable.DEFAULT_PARTIAL)
          .build();

  DataStorageConfiguration DEFAULT_FOREST_CONFIG =
      ImmutableDataStorageConfiguration.builder()
          .dataStorageFormat(DataStorageFormat.FOREST)
          .diffbasedMaxLayersToLoad(DEFAULT_DIFFBASED_MAX_LAYERS_TO_LOAD)
          .unstable(Unstable.DEFAULT)
          .build();

  DataStorageFormat getDataStorageFormat();

  Long getDiffbasedMaxLayersToLoad();

  @Value.Default
  default boolean getDiffbasedLimitTrieLogsEnabled() {
    return DEFAULT_DIFFBASED_LIMIT_TRIE_LOGS_ENABLED;
  }

  @Value.Default
  default int getDiffbasedTrieLogPruningWindowSize() {
    return DEFAULT_DIFFBASED_TRIE_LOG_PRUNING_WINDOW_SIZE;
  }

  @Value.Default
  default boolean getReceiptCompactionEnabled() {
    return DEFAULT_RECEIPT_COMPACTION_ENABLED;
  }

  @Value.Default
  default Unstable getUnstable() {
    return Unstable.DEFAULT;
  }

  @Value.Immutable
  interface Unstable {

    boolean DEFAULT_BONSAI_FULL_FLAT_DB_ENABLED = true;
    boolean DEFAULT_DIFFBASED_CODE_USING_CODE_HASH_ENABLED = true;

    boolean DEFAULT_PARALLEL_TRX_ENABLED = false;

    DataStorageConfiguration.Unstable DEFAULT =
        ImmutableDataStorageConfiguration.Unstable.builder().build();

    DataStorageConfiguration.Unstable DEFAULT_PARTIAL =
        ImmutableDataStorageConfiguration.Unstable.builder().bonsaiFullFlatDbEnabled(false).build();

    @Value.Default
    default boolean getBonsaiFullFlatDbEnabled() {
      return DEFAULT_BONSAI_FULL_FLAT_DB_ENABLED;
    }

    @Value.Default
    default boolean getDiffbasedCodeStoredByCodeHashEnabled() {
      return DEFAULT_DIFFBASED_CODE_USING_CODE_HASH_ENABLED;
    }

    @Value.Default
    default boolean isParallelTxProcessingEnabled() {
      return DEFAULT_PARALLEL_TRX_ENABLED;
    }
  }
}
