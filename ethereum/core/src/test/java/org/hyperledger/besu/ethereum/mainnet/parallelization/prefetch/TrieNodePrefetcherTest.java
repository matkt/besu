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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig.createStatefulConfigWithTrie;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TrieNodePrefetcherTest {

  private static final Executor SYNC_EXECUTOR = Runnable::run;
  private static final int ACCOUNTS = 300;
  private static final int SLOTS = 50;

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
    worldState = worldState(storage);
    final WorldUpdater updater = worldState.updater();
    for (int i = 0; i < ACCOUNTS; i++) {
      final MutableAccount account = updater.createAccount(address(i), 0, Wei.of(i + 1));
      if (i % 10 == 0) {
        for (int slot = 0; slot < SLOTS; slot++) {
          // values differ per account: equal storage tries would share their (hash-keyed) nodes
          account.setStorageValue(UInt256.valueOf(slot), UInt256.valueOf(i * SLOTS + slot + 1));
        }
      }
    }
    updater.commit();
    worldState.persist(null);
    // committing caches what it writes: start from a cold cache
    cache().clear(TRIE_BRANCH_STORAGE);
    cache().clear(ACCOUNT_INFO_STATE);
    cache().clear(ACCOUNT_STORAGE_STORAGE);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (storage.getCacheManager() instanceof final Closeable closeable) {
      closeable.close();
    }
    storage.close();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 7, 256})
  void prefetchesTheTrieNodesOnThePathsOfTheBlockAccessList(final int batchSize) {
    final List<Integer> accessed = List.of(0, 3, 10, 57, 120, 299);
    final List<BlockAccessList.AccountChanges> accountChanges = new ArrayList<>();
    for (final int i : accessed) {
      accountChanges.add(accountChanges(address(i), i % 10 == 0 ? List.of(1, 7, 42) : List.of()));
    }
    // an account and a slot that do not exist: their walks stop where their paths leave the trie
    accountChanges.add(accountChanges(address(ACCOUNTS + 1), List.of(1)));
    accountChanges.add(accountChanges(address(20), List.of(SLOTS + 1)));

    final String summary =
        TrieNodePrefetcher.prefetch(
                new BonsaiWorldStateLayerStorage(storage),
                worldState.getWorldStateRootHash(),
                new BlockAccessList(accountChanges),
                SYNC_EXECUTOR,
                batchSize,
                new BalPrefetch())
            .join();

    assertThat(summary).contains("trie nodes read");
    for (final int i : accessed) {
      assertThat(accountPath(address(i)))
          .allSatisfy(hash -> assertThat(cache().getAccountTrieNode(hash)).isPresent());
      if (i % 10 == 0) {
        for (final int slot : List.of(1, 7, 42)) {
          assertThat(storagePath(address(i), slot))
              .allSatisfy(hash -> assertThat(cache().getStorageTrieNode(hash)).isPresent());
        }
      }
    }
    // the leaf of an account outside the block access list is not read
    final List<Bytes32> otherPath = accountPath(address(5));
    assertThat(cache().getAccountTrieNode(otherPath.getLast())).isEmpty();
    // nor are the storage trie nodes of an account without slots in it
    assertThat(cache().getStorageTrieNode(storagePath(address(30), 1).getFirst())).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 256})
  void doesNotReadNodesAlreadyCached(final int batchSize) {
    final BlockAccessList blockAccessList =
        new BlockAccessList(List.of(accountChanges(address(10), List.of(1, 2))));
    TrieNodePrefetcher.prefetch(
            storage,
            worldState.getWorldStateRootHash(),
            blockAccessList,
            SYNC_EXECUTOR,
            batchSize,
            new BalPrefetch())
        .join();

    final String summary =
        TrieNodePrefetcher.prefetch(
                storage,
                worldState.getWorldStateRootHash(),
                blockAccessList,
                SYNC_EXECUTOR,
                batchSize,
                new BalPrefetch())
            .join();

    // only the storage trie root, whose hash is not known before reading it, is read again
    assertThat(summary).startsWith("1 trie nodes read");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 256})
  void aCancelledPrefetchReadsNothing(final int batchSize) {
    final BlockAccessList blockAccessList =
        new BlockAccessList(List.of(accountChanges(address(10), List.of(1, 2))));
    final BalPrefetch prefetch = new BalPrefetch();
    prefetch.cancel();

    new BalPrefetcher(true, batchSize)
        .prefetch(worldState, blockAccessList, SYNC_EXECUTOR, SYNC_EXECUTOR, prefetch)
        .join();

    assertThat(storage.isCached(ACCOUNT_INFO_STATE, address(10).addressHash().getBytes()))
        .isFalse();
    assertThat(cache().getAccountTrieNode(accountPath(address(10)).getFirst())).isEmpty();
  }

  @Test
  void aPrefetchCancelledDuringALevelDoesNotReadTheNextOnes() {
    final BlockAccessList blockAccessList =
        new BlockAccessList(List.of(accountChanges(address(10), List.of())));
    final BalPrefetch prefetch = new BalPrefetch();
    // e.g. the block turns out to be invalid while the first level is being read
    final Executor cancelAfterTheFirstBatch =
        task -> {
          task.run();
          prefetch.cancel();
        };

    final String summary =
        TrieNodePrefetcher.prefetch(
                storage,
                worldState.getWorldStateRootHash(),
                blockAccessList,
                cancelAfterTheFirstBatch,
                256,
                prefetch)
            .join();

    assertThat(summary).startsWith("1 trie nodes read");
    final List<Bytes32> path = accountPath(address(10));
    assertThat(cache().getAccountTrieNode(path.getFirst())).isPresent();
    assertThat(cache().getAccountTrieNode(path.get(1))).isEmpty();
  }

  /** The hashes of the account trie nodes on the path of an account, root first. */
  private List<Bytes32> accountPath(final Address address) {
    final List<Bytes32> hashes = new ArrayList<>();
    new StoredMerklePatriciaTrie<Bytes, Bytes>(
            (location, hash) -> {
              hashes.add(hash);
              return storage.getAccountStateTrieNode(location, hash);
            },
            Bytes32.wrap(worldState.getWorldStateRootHash().getBytes()),
            Function.identity(),
            Function.identity())
        .get(address.addressHash().getBytes());
    return hashes;
  }

  /** The hashes of the storage trie nodes on the path of a slot, root first. */
  private List<Bytes32> storagePath(final Address address, final int slot) {
    final Hash accountHash = address.addressHash();
    final List<Bytes32> hashes = new ArrayList<>();
    new StoredMerklePatriciaTrie<Bytes, Bytes>(
            (location, hash) -> {
              hashes.add(hash);
              return storage.getAccountStorageTrieNode(accountHash, location, hash);
            },
            Bytes32.wrap(((BonsaiAccount) worldState.get(address)).getStorageRoot().getBytes()),
            Function.identity(),
            Function.identity())
        .get(new StorageSlotKey(UInt256.valueOf(slot)).getSlotHash().getBytes());
    return hashes;
  }

  private FlatDbCacheManager cache() {
    return storage.getCacheManager();
  }

  private static Address address(final int i) {
    return Address.fromHexString(String.format("0x%040x", i + 1));
  }

  private static BlockAccessList.AccountChanges accountChanges(
      final Address address, final List<Integer> slots) {
    final List<BlockAccessList.SlotRead> reads = new ArrayList<>();
    slots.forEach(slot -> reads.add(new BlockAccessList.SlotRead(slotKey(slot))));
    return new BlockAccessList.AccountChanges(
        address, List.of(), reads, List.of(), List.of(), List.of());
  }

  private static StorageSlotKey slotKey(final int slot) {
    return new StorageSlotKey(UInt256.valueOf(slot));
  }

  private static BonsaiWorldState worldState(final BonsaiWorldStateKeyValueStorage storage) {
    return new BonsaiWorldState(
        storage,
        new NoOpBonsaiCachedMerkleTrieLoader(),
        new NoOpBonsaiWorldStateCacheManager(
            storage, EvmConfiguration.DEFAULT, new BonsaiCodeCache()),
        new NoOpTrieLogManager(),
        EvmConfiguration.DEFAULT,
        createStatefulConfigWithTrie(),
        new BonsaiCodeCache());
  }
}
