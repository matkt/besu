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
package org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig.createStatefulConfigWithTrie;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BalPrefetchCancellationTest {

  private static final Executor SYNC_EXECUTOR = Runnable::run;

  private BonsaiWorldStateKeyValueStorage storage;
  private BonsaiWorldState worldState;

  @BeforeEach
  void setUp() {
    storage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            ImmutableDataStorageConfiguration.builder()
                .dataStorageFormat(DataStorageFormat.BONSAI)
                .extraStorageConfiguration(
                    ImmutableExtraStorageConfiguration.builder()
                        .unstable(
                            ImmutableExtraStorageConfiguration.Unstable.builder()
                                .bonsaiCrossBlockCacheEnabled(true)
                                .build())
                        .build())
                .build());
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    for (int i = 0; i < 4; i++) {
      updater.putAccountInfoState(address(i).addressHash(), Bytes.of(i + 1));
    }
    updater.commit();
    // committing caches what it writes: start from a cold cache
    storage.getCacheManager().clear(ACCOUNT_INFO_STATE);
    worldState =
        new BonsaiWorldState(
            storage,
            new NoOpBonsaiCachedMerkleTrieLoader(),
            new NoOpBonsaiWorldStateCacheManager(
                storage, EvmConfiguration.DEFAULT, new BonsaiCodeCache()),
            new NoOpTrieLogManager(),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new BonsaiCodeCache());
  }

  @AfterEach
  void tearDown() throws Exception {
    if (storage.getCacheManager() instanceof final Closeable closeable) {
      closeable.close();
    }
    storage.close();
  }

  @Test
  void aCancelledPrefetchReadsNothing() {
    final BalPrefetch prefetch = new BalPrefetch();
    prefetch.cancel();

    new BalPrefetcher(true, 1)
        .prefetch(worldState, blockAccessList(), SYNC_EXECUTOR, SYNC_EXECUTOR, prefetch)
        .join();

    for (int i = 0; i < 4; i++) {
      assertThat(isCached(address(i))).isFalse();
    }
  }

  @Test
  void aPrefetchCancelledDuringABatchDoesNotReadTheNextOnes() {
    final BalPrefetch prefetch = new BalPrefetch();
    // e.g. the block turns out to be invalid while the first batch is being read
    final Executor cancelAfterTheFirstBatch =
        task -> {
          task.run();
          prefetch.cancel();
        };

    new BalPrefetcher(false, 1)
        .prefetch(worldState, blockAccessList(), SYNC_EXECUTOR, cancelAfterTheFirstBatch, prefetch)
        .join();

    assertThat(isCached(address(0))).isTrue();
    for (int i = 1; i < 4; i++) {
      assertThat(isCached(address(i))).isFalse();
    }
  }

  private BlockAccessList blockAccessList() {
    final List<BlockAccessList.AccountChanges> accounts = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      accounts.add(
          new BlockAccessList.AccountChanges(
              address(i), List.of(), List.of(), List.of(), List.of(), List.of()));
    }
    return new BlockAccessList(accounts);
  }

  private boolean isCached(final Address address) {
    return storage.isCached(ACCOUNT_INFO_STATE, address.addressHash().getBytes());
  }

  private static Address address(final int i) {
    return Address.fromHexString(String.format("0x%040x", i + 1));
  }
}
