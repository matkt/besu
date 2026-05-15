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
public interface PathBasedExtraStorageConfiguration {

  PathBasedExtraStorageConfiguration DEFAULT =
      ImmutablePathBasedExtraStorageConfiguration.builder().build();

  PathBasedExtraStorageConfiguration DISABLED =
      ImmutablePathBasedExtraStorageConfiguration.builder()
          .limitTrieLogsEnabled(false)
          .unstable(PathBasedUnstable.DISABLED)
          .parallelTxProcessingEnabled(false)
          .parallelStateRootComputationEnabled(false)
          .build();

  long DEFAULT_MAX_LAYERS_TO_LOAD = 512;
  boolean DEFAULT_LIMIT_TRIE_LOGS_ENABLED = true;
  long MINIMUM_TRIE_LOG_RETENTION_LIMIT = DEFAULT_MAX_LAYERS_TO_LOAD;
  int DEFAULT_TRIE_LOG_PRUNING_WINDOW_SIZE = 5_000;
  boolean DEFAULT_PARALLEL_TX_PROCESSING = true;
  boolean DEFAULT_PARALLEL_STATE_ROOT_COMPUTATION = true;

  @Value.Default
  default Long getMaxLayersToLoad() {
    return DEFAULT_MAX_LAYERS_TO_LOAD;
  }

  @Value.Default
  default boolean getLimitTrieLogsEnabled() {
    return DEFAULT_LIMIT_TRIE_LOGS_ENABLED;
  }

  @Value.Default
  default int getTrieLogPruningWindowSize() {
    return DEFAULT_TRIE_LOG_PRUNING_WINDOW_SIZE;
  }

  @Value.Default
  default boolean getParallelTxProcessingEnabled() {
    return DEFAULT_PARALLEL_TX_PROCESSING;
  }

  @Value.Default
  default boolean getParallelStateRootComputationEnabled() {
    return DEFAULT_PARALLEL_STATE_ROOT_COMPUTATION;
  }

  @Value.Default
  default PathBasedUnstable getUnstable() {
    return PathBasedUnstable.DEFAULT;
  }

  /**
   * Whether snap-sync flat healing options are allowed for the given storage format (requires a
   * full flat layout for Bonsai and BinTrie FULL mode for {@link DataStorageFormat#BINTRIE}).
   */
  default boolean isSnapSynchronizerFlatModeCompatible(final DataStorageFormat dataStorageFormat) {
    return switch (dataStorageFormat) {
      case BONSAI, X_BONSAI_ARCHIVE -> getUnstable().getFullFlatDbEnabled();
      case BINTRIE -> getUnstable().getBinTrieFlatDbMode() == BinTrieFlatDbMode.FULL;
      default -> false;
    };
  }

  @Value.Immutable
  interface PathBasedUnstable {

    PathBasedExtraStorageConfiguration.PathBasedUnstable DEFAULT =
        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
            .binTrieFlatDbMode(BinTrieFlatDbMode.STEM)
            .build();

    PathBasedExtraStorageConfiguration.PathBasedUnstable PARTIAL_MODE =
        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
            .fullFlatDbEnabled(false)
            .binTrieFlatDbMode(BinTrieFlatDbMode.STEM)
            .build();

    PathBasedExtraStorageConfiguration.PathBasedUnstable DISABLED =
        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
            .fullFlatDbEnabled(false)
            .codeStoredByCodeHashEnabled(false)
            .binTrieFlatDbMode(BinTrieFlatDbMode.STEM)
            .build();

    boolean DEFAULT_FULL_FLAT_DB_ENABLED = true;
    boolean DEFAULT_CODE_USING_CODE_HASH_ENABLED = true;
    BinTrieFlatDbMode DEFAULT_BINTRIE_FLAT_DB_MODE = BinTrieFlatDbMode.STEM;

    @Value.Default
    default boolean getFullFlatDbEnabled() {
      return DEFAULT_FULL_FLAT_DB_ENABLED;
    }

    @Value.Default
    default BinTrieFlatDbMode getBinTrieFlatDbMode() {
      return DEFAULT_BINTRIE_FLAT_DB_MODE;
    }

    @Value.Default
    default boolean getCodeStoredByCodeHashEnabled() {
      return DEFAULT_CODE_USING_CODE_HASH_ENABLED;
    }
  }
}
