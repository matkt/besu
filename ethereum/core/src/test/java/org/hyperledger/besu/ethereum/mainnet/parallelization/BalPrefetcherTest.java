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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig.createStatefulConfigWithTrie;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch.BalPrefetcher;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiSnapshotWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Ensures {@link BalPrefetcher} warms {@link
 * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.cache.VersionedFlatDbCacheManager}
 * via {@link BonsaiWorldStateKeyValueStorage#getMultipleKeys} rather than bypassing the cache.
 */
public class BalPrefetcherTest {

  private static final Executor SYNC_EXECUTOR = Runnable::run;

  private BonsaiWorldStateKeyValueStorage baseStorage;

  enum StorageMode {
    BASE,
    SNAPSHOT,
    LAYER
  }

  @BeforeEach
  void setUp() {
    baseStorage =
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
  }

  @AfterEach
  void tearDown() throws Exception {
    if (baseStorage != null) {
      if (baseStorage.getCacheManager() instanceof final Closeable closeable) {
        closeable.close();
      }
      baseStorage.close();
      baseStorage = null;
    }
  }

  static Stream<Arguments> storageModes() {
    return Stream.of(
        Arguments.of(StorageMode.BASE, false, 0),
        Arguments.of(StorageMode.BASE, true, 1),
        Arguments.of(StorageMode.SNAPSHOT, true, 2),
        Arguments.of(StorageMode.LAYER, false, 1));
  }

  @ParameterizedTest
  @MethodSource("storageModes")
  void prefetchAccountsIntoCache(
      final StorageMode mode, final boolean sortingEnabled, final int batchSize) {
    final Address address1 = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final Address address2 = Address.fromHexString("0x2222222222222222222222222222222222222222");
    final Bytes accountData1 = Bytes.of(1, 2, 3);
    final Bytes accountData2 = Bytes.of(4, 5, 6);

    final BonsaiWorldStateKeyValueStorage.Updater updater = baseStorage.updater();
    updater.putAccountInfoState(address1.addressHash(), accountData1);
    updater.putAccountInfoState(address2.addressHash(), accountData2);
    updater.commit();
    clearCache();

    final BonsaiWorldState worldState = createWorldState(createStorage(mode));
    final BlockAccessList blockAccessList =
        new BlockAccessList(
            List.of(
                accountChanges(address2, List.of(), List.of()),
                accountChanges(address1, List.of(), List.of())));

    new BalPrefetcher(sortingEnabled, batchSize)
        .prefetch(worldState, blockAccessList, SYNC_EXECUTOR)
        .join();

    assertCachedAccount(address1, accountData1);
    assertCachedAccount(address2, accountData2);
    worldState.close();
  }

  @ParameterizedTest
  @MethodSource("storageModes")
  void prefetchStorageSlotsIntoCache(
      final StorageMode mode, final boolean sortingEnabled, final int batchSize) {
    final Address address = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final StorageSlotKey slot1 = new StorageSlotKey(UInt256.valueOf(1));
    final StorageSlotKey slot2 = new StorageSlotKey(UInt256.valueOf(2));
    final Bytes storageValue1 = Bytes.of(10);
    final Bytes storageValue2 = Bytes.of(20);

    final BonsaiWorldStateKeyValueStorage.Updater updater = baseStorage.updater();
    updater.putAccountInfoState(address.addressHash(), Bytes.of(1));
    updater.putStorageValueBySlotHash(address.addressHash(), slot1.getSlotHash(), storageValue1);
    updater.putStorageValueBySlotHash(address.addressHash(), slot2.getSlotHash(), storageValue2);
    updater.commit();
    clearCache();

    final BonsaiWorldState worldState = createWorldState(createStorage(mode));
    final BlockAccessList blockAccessList =
        new BlockAccessList(
            List.of(
                accountChanges(
                    address,
                    List.of(new BlockAccessList.SlotChanges(slot1, List.of())),
                    List.of(new BlockAccessList.SlotRead(slot2)))));

    new BalPrefetcher(sortingEnabled, batchSize)
        .prefetch(worldState, blockAccessList, SYNC_EXECUTOR)
        .join();

    assertCachedStorage(address, slot1, storageValue1);
    assertCachedStorage(address, slot2, storageValue2);
    worldState.close();
  }

  @ParameterizedTest
  @MethodSource("storageModes")
  void prefetchContractCodeIntoVersionedCodeCache(
      final StorageMode mode, final boolean sortingEnabled, final int batchSize) {
    final Address address = Address.fromHexString("0x1111111111111111111111111111111111111111");
    // Minimal legacy bytecode containing a JUMPDEST so analysis produces a non-null mask.
    final Bytes bytecode = Bytes.fromHexString("0x5b6000");
    final Hash codeHash = Hash.hash(bytecode);
    final Bytes accountRlp = accountRlpWithCodeHash(codeHash);

    final BonsaiWorldStateKeyValueStorage.Updater updater = baseStorage.updater();
    updater.putAccountInfoState(address.addressHash(), accountRlp);
    updater.putCode(address.addressHash(), codeHash, bytecode);
    updater.commit();
    clearCache();

    final BonsaiWorldState worldState = createWorldState(createStorage(mode));
    final BlockAccessList blockAccessList =
        new BlockAccessList(List.of(accountChanges(address, List.of(), List.of())));

    new BalPrefetcher(sortingEnabled, batchSize)
        .prefetch(worldState, blockAccessList, SYNC_EXECUTOR)
        .join();

    final FlatDbCacheManager cacheManager = baseStorage.getCacheManager();
    final Code cached = cacheManager.getIfPresent(codeHash);
    assertThat(cached).isNotNull();
    assertThat(cached.getBytes()).isEqualTo(bytecode);
    assertThat(cached.getJumpDestBitMask()).isNotNull();
    worldState.close();
  }

  /**
   * Missing accounts and EOAs (empty code hash) must not warm code; only contracts with non-empty
   * code from retained account RLPs are prefetched.
   */
  @ParameterizedTest
  @MethodSource("storageModes")
  void prefetchCodeSkipsMissingAccountsAndEmptyCodeHash(
      final StorageMode mode, final boolean sortingEnabled, final int batchSize) {
    final Address missing = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final Address eoa = Address.fromHexString("0x2222222222222222222222222222222222222222");
    final Address contract = Address.fromHexString("0x3333333333333333333333333333333333333333");
    final Bytes bytecode = Bytes.fromHexString("0x5b6000");
    final Hash codeHash = Hash.hash(bytecode);

    final BonsaiWorldStateKeyValueStorage.Updater updater = baseStorage.updater();
    updater.putAccountInfoState(eoa.addressHash(), accountRlpWithCodeHash(Hash.EMPTY));
    updater.putAccountInfoState(contract.addressHash(), accountRlpWithCodeHash(codeHash));
    updater.putCode(contract.addressHash(), codeHash, bytecode);
    updater.commit();
    clearCache();

    final BonsaiWorldState worldState = createWorldState(createStorage(mode));
    final BlockAccessList blockAccessList =
        new BlockAccessList(
            List.of(
                accountChanges(missing, List.of(), List.of()),
                accountChanges(eoa, List.of(), List.of()),
                accountChanges(contract, List.of(), List.of())));

    new BalPrefetcher(sortingEnabled, batchSize)
        .prefetch(worldState, blockAccessList, SYNC_EXECUTOR)
        .join();

    assertThat(baseStorage.isCached(ACCOUNT_INFO_STATE, missing.addressHash().getBytes())).isTrue();
    assertCachedAccount(eoa, accountRlpWithCodeHash(Hash.EMPTY));
    assertCachedAccount(contract, accountRlpWithCodeHash(codeHash));

    final FlatDbCacheManager cacheManager = baseStorage.getCacheManager();
    assertThat(cacheManager.getIfPresent(Hash.EMPTY)).isNull();
    final Code cached = cacheManager.getIfPresent(codeHash);
    assertThat(cached).isNotNull();
    assertThat(cached.getBytes()).isEqualTo(bytecode);
    worldState.close();
  }

  /**
   * Prefetch on a parent snapshot must warm the shared {@link FlatDbCacheManager}; a separate
   * execution world state (layer, as used with BAL overlay) must then serve account / storage /
   * analyzed code via {@link BonsaiWorldState} reads without hitting the flat DB.
   */
  @Test
  void prefetchWarmsSharedCacheVisibleToSeparateWorldState() {
    final Address address = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final StorageSlotKey slot = new StorageSlotKey(UInt256.valueOf(7));
    final Bytes storageValue = Bytes.of(42);
    final Bytes bytecode = Bytes.fromHexString("0x5b6000");
    final Hash codeHash = Hash.hash(bytecode);
    final Bytes accountRlp = accountRlpWithCodeHash(codeHash);

    final BonsaiWorldStateKeyValueStorage.Updater updater = baseStorage.updater();
    updater.putAccountInfoState(address.addressHash(), accountRlp);
    updater.putStorageValueBySlotHash(address.addressHash(), slot.getSlotHash(), storageValue);
    updater.putCode(address.addressHash(), codeHash, bytecode);
    updater.commit();
    clearCache();

    // Prefetch path: snapshot world state (no BAL overlay), same as
    // BalConcurrentTransactionProcessor
    final BonsaiWorldState prefetchWs =
        createWorldState(new BonsaiSnapshotWorldStateKeyValueStorage(baseStorage));
    final BlockAccessList blockAccessList =
        new BlockAccessList(
            List.of(
                accountChanges(
                    address,
                    List.of(new BlockAccessList.SlotChanges(slot, List.of())),
                    List.of())));

    new BalPrefetcher(false, 0)
        .prefetch(prefetchWs, blockAccessList, SYNC_EXECUTOR)
        .join();
    prefetchWs.close();

    // Wipe flat DB under the cache so subsequent hits must come from VersionedFlatDbCacheManager.
    wipeFlatDb(address, slot, codeHash);

    // Execution path: separate layered world state sharing parent cache manager
    final BonsaiWorldState execWs = createWorldState(new BonsaiWorldStateLayerStorage(baseStorage));

    assertThat(execWs.getCode(address, codeHash))
        .hasValueSatisfying(
            c -> {
              assertThat(c.getBytes()).isEqualTo(bytecode);
              assertThat(c.getJumpDestBitMask()).isNotNull();
            });

    final Account account = execWs.get(address);
    assertThat(account).isNotNull();
    assertThat(account.getCodeHash()).isEqualTo(codeHash);

    final Code analyzed = account.getOrCreateCachedCode();
    assertThat(analyzed.getBytes()).isEqualTo(bytecode);
    assertThat(analyzed.getJumpDestBitMask()).isNotNull();
    assertThat(analyzed).isSameAs(baseStorage.getCacheManager().getIfPresent(codeHash));

    assertThat(execWs.getStorageValueByStorageSlotKey(address, slot))
        .contains(UInt256.fromBytes(storageValue));

    execWs.close();
  }

  /** Remove flat keys without going through CachedUpdater (avoids cache tombstones). */
  private void wipeFlatDb(final Address address, final StorageSlotKey slot, final Hash codeHash) {
    final SegmentedKeyValueStorageTransaction tx =
        baseStorage.getComposedWorldStateStorage().startTransaction();
    tx.remove(ACCOUNT_INFO_STATE, address.addressHash().getBytes().toArrayUnsafe());
    tx.remove(
        ACCOUNT_STORAGE_STORAGE,
        Bytes.concatenate(address.addressHash().getBytes(), slot.getSlotHash().getBytes())
            .toArrayUnsafe());
    tx.remove(CODE_STORAGE, codeHash.getBytes().toArrayUnsafe());
    tx.commit();
  }

  private static Bytes accountRlpWithCodeHash(final Hash codeHash) {
    final PmtStateTrieAccountValue accountValue =
        new PmtStateTrieAccountValue(0L, Wei.ZERO, Hash.EMPTY_TRIE_HASH, codeHash);
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    accountValue.writeTo(out);
    return out.encoded();
  }

  private BonsaiWorldStateKeyValueStorage createStorage(final StorageMode mode) {
    return switch (mode) {
      case BASE -> baseStorage;
      case SNAPSHOT -> new BonsaiSnapshotWorldStateKeyValueStorage(baseStorage);
      case LAYER -> new BonsaiWorldStateLayerStorage(baseStorage);
    };
  }

  private BonsaiWorldState createWorldState(final BonsaiWorldStateKeyValueStorage storage) {
    return new BonsaiWorldState(
        storage,
        new NoOpBonsaiCachedMerkleTrieLoader(),
        new NoOpBonsaiWorldStateCacheManager(storage, EvmConfiguration.DEFAULT),
        new NoOpTrieLogManager(),
        EvmConfiguration.DEFAULT,
        createStatefulConfigWithTrie());
  }

  private static BlockAccessList.AccountChanges accountChanges(
      final Address address,
      final List<BlockAccessList.SlotChanges> slotChanges,
      final List<BlockAccessList.SlotRead> slotReads) {
    return new BlockAccessList.AccountChanges(
        address, slotChanges, slotReads, List.of(), List.of(), List.of());
  }

  private void clearCache() {
    final FlatDbCacheManager cacheManager = baseStorage.getCacheManager();
    cacheManager.clear(ACCOUNT_INFO_STATE);
    cacheManager.clear(ACCOUNT_STORAGE_STORAGE);
    cacheManager.clear(CODE_STORAGE);
  }

  private void assertCachedAccount(final Address address, final Bytes expectedValue) {
    final Bytes key = address.addressHash().getBytes();
    assertThat(baseStorage.isCached(ACCOUNT_INFO_STATE, key)).isTrue();
    assertThat(baseStorage.getCachedValue(ACCOUNT_INFO_STATE, key))
        .isPresent()
        .get()
        .satisfies(
            value -> {
              assertThat(value.isRemoval()).isFalse();
              assertThat(value.getValue()).isEqualTo(expectedValue);
            });
  }

  private void assertCachedStorage(
      final Address address, final StorageSlotKey slot, final Bytes expectedValue) {
    final Bytes storageKey =
        Bytes.concatenate(address.addressHash().getBytes(), slot.getSlotHash().getBytes());
    assertThat(baseStorage.isCached(ACCOUNT_STORAGE_STORAGE, storageKey)).isTrue();
    assertThat(baseStorage.getCachedValue(ACCOUNT_STORAGE_STORAGE, storageKey))
        .isPresent()
        .get()
        .satisfies(
            value -> {
              assertThat(value.isRemoval()).isFalse();
              assertThat(value.getValue()).isEqualTo(expectedValue);
            });
  }
}
