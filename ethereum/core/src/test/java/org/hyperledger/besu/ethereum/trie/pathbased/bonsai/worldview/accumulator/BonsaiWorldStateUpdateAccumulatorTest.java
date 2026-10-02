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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig.createStatefulConfigWithTrie;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.BonsaiTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/** Tests for {@link BonsaiWorldStateUpdateAccumulator#importStateChangesFromPartialView}. */
class BonsaiWorldStateUpdateAccumulatorTest {

  private static final Address ACCOUNT =
      Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final StorageSlotKey SLOT = new StorageSlotKey(UInt256.valueOf(7));
  private static final UInt256 V0 = UInt256.valueOf(100);
  private static final UInt256 V1 = UInt256.valueOf(200);
  private static final UInt256 V2 = UInt256.valueOf(300);

  @Test
  void importPartialView_sameSlotTwoTransactions_keepsBlockStartPriorAndFinalUpdated() {
    try (BonsaiWorldState worldState = newEmptyWorldState()) {
      final BonsaiWorldStateUpdateAccumulator accumulator =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();
      accumulator.createAccount(ACCOUNT, 0L, Wei.ONE);
      accumulator.getAccount(ACCOUNT).setStorageValue(SLOT.getSlotKey().orElseThrow(), V0);
      accumulator.commit();
      worldState.persist(null);

      final PartialBlockAccessView tx0 = partialView(0, V0, V1);
      final PartialBlockAccessView tx1 = partialView(1, V1, V2);

      accumulator.importStateChangesFromPartialView(tx0);
      accumulator.importStateChangesFromPartialView(tx1);

      final BonsaiValue<UInt256> merged = accumulator.getStorageToUpdate().get(ACCOUNT).get(SLOT);
      assertThat(merged.getPrior()).isEqualTo(V0);
      assertThat(merged.getUpdated()).isEqualTo(V2);

      final TrieLogLayer trieLog =
          new BonsaiTrieLogFactory()
              .create(
                  accumulator,
                  new BlockHeaderTestFixture().number(1).stateRoot(Hash.EMPTY).buildHeader());
      final BonsaiValue<UInt256> trieLogSlot = trieLog.getStorageChanges(ACCOUNT).get(SLOT);
      assertThat(trieLogSlot.getPrior()).isEqualTo(V0);
      assertThat(trieLogSlot.getUpdated()).isEqualTo(V2);
    }
  }

  @Test
  void importPartialView_capturedPrior_givesSameValuesAndTrieLogAsLoadingTheAccount() {
    try (BonsaiWorldState loading = newWorldStateWithAccount();
        BonsaiWorldState seeded = newWorldStateWithAccount()) {
      final BonsaiWorldStateUpdateAccumulator loadingAccumulator =
          (BonsaiWorldStateUpdateAccumulator) loading.updater();
      final BonsaiWorldStateUpdateAccumulator seededAccumulator =
          (BonsaiWorldStateUpdateAccumulator) seeded.updater();
      final BonsaiAccount stored = (BonsaiAccount) seeded.get(ACCOUNT);

      loadingAccumulator.importStateChangesFromPartialView(accountView(0, Wei.of(20), 4L, null));
      seededAccumulator.importStateChangesFromPartialView(
          accountView(
              0,
              Wei.of(20),
              4L,
              new PmtStateTrieAccountValue(
                  stored.getNonce(),
                  stored.getBalance(),
                  stored.getStorageRoot(),
                  stored.getCodeHash())));

      loadingAccumulator.commit();
      seededAccumulator.commit();

      assertSameAccountValue(
          seededAccumulator.getAccountsToUpdate().get(ACCOUNT),
          loadingAccumulator.getAccountsToUpdate().get(ACCOUNT));
      assertSameAccountValue(
          trieLog(seededAccumulator).getAccountChanges().get(ACCOUNT),
          trieLog(loadingAccumulator).getAccountChanges().get(ACCOUNT));
    }
  }

  @Test
  void importPartialView_capturedPrior_isUsedInsteadOfReadingTheWorldState() {
    try (BonsaiWorldState worldState = newWorldStateWithAccount()) {
      final BonsaiWorldStateUpdateAccumulator accumulator =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();
      // Deliberately different from what is stored: proves the account is not read again.
      final AccountValue prior =
          new PmtStateTrieAccountValue(9L, Wei.of(99), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);

      accumulator.importStateChangesFromPartialView(accountView(0, Wei.of(20), 10L, prior));

      accumulator.commit();

      final BonsaiValue<BonsaiAccount> value = accumulator.getAccountsToUpdate().get(ACCOUNT);
      assertThat(value.getPrior().getNonce()).isEqualTo(9L);
      assertThat(value.getPrior().getBalance()).isEqualTo(Wei.of(99));
      assertThat(value.getUpdated().getNonce()).isEqualTo(10L);
      assertThat(value.getUpdated().getBalance()).isEqualTo(Wei.of(20));
    }
  }

  @Test
  void importPartialView_capturedPriorOfAnAccountAlreadyChanged_isIgnored() {
    try (BonsaiWorldState worldState = newWorldStateWithAccount()) {
      final BonsaiWorldStateUpdateAccumulator accumulator =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();
      final BonsaiAccount stored = (BonsaiAccount) worldState.get(ACCOUNT);

      accumulator.importStateChangesFromPartialView(accountView(0, Wei.of(20), 4L, null));
      // A later transaction read the account as left by the first one.
      accumulator.importStateChangesFromPartialView(
          accountView(
              1,
              Wei.of(30),
              5L,
              new PmtStateTrieAccountValue(
                  4L, Wei.of(20), stored.getStorageRoot(), stored.getCodeHash())));

      accumulator.commit();

      final BonsaiValue<BonsaiAccount> value = accumulator.getAccountsToUpdate().get(ACCOUNT);
      assertThat(value.getPrior().getNonce()).isEqualTo(stored.getNonce());
      assertThat(value.getPrior().getBalance()).isEqualTo(stored.getBalance());
      assertThat(value.getUpdated().getNonce()).isEqualTo(5L);
      assertThat(value.getUpdated().getBalance()).isEqualTo(Wei.of(30));
    }
  }

  @Test
  void trackedTransaction_capturesThePriorThroughTheUpdaterStack_andTheImportUsesIt() {
    try (BonsaiWorldState txWorldState = newWorldStateWithAccount();
        BonsaiWorldState blockWorldState = newWorldStateWithAccount()) {
      final BonsaiAccount stored = (BonsaiAccount) txWorldState.get(ACCOUNT);
      // As a parallel worker runs a transaction: an updater on the accumulator, and a frame
      // updater on top of it committed into it.
      final WorldUpdater txUpdater = txWorldState.updater().updater();
      final WorldUpdater frameUpdater = txUpdater.updater();
      final MutableAccount account = frameUpdater.getAccount(ACCOUNT);
      account.setBalance(Wei.of(20));
      account.setNonce(4L);
      frameUpdater.commit();
      final AccessLocationTracker tracker = new AccessLocationTracker(0);
      tracker.addTouchedAccount(ACCOUNT);

      final PartialBlockAccessView view = tracker.createPartialBlockAccessView(txUpdater);

      final AccountValue prior = view.accountChanges().getFirst().getPriorAccount().orElseThrow();
      assertSameAccount(prior, stored);

      final BonsaiWorldStateUpdateAccumulator blockAccumulator =
          (BonsaiWorldStateUpdateAccumulator) blockWorldState.updater();
      blockAccumulator.importStateChangesFromPartialView(view);
      blockAccumulator.commit();
      final BonsaiValue<BonsaiAccount> value = blockAccumulator.getAccountsToUpdate().get(ACCOUNT);
      assertSameAccount(value.getPrior(), stored);
      assertThat(value.getUpdated().getNonce()).isEqualTo(4L);
      assertThat(value.getUpdated().getBalance()).isEqualTo(Wei.of(20));
    }
  }

  private static PartialBlockAccessView accountView(
      final long txIndex, final Wei postBalance, final long nonce, final AccountValue prior) {
    final PartialBlockAccessView.PartialBlockAccessViewBuilder builder =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(txIndex);
    final PartialBlockAccessView.AccountChangesBuilder account =
        builder
            .getOrCreateAccountBuilder(ACCOUNT)
            .withPostBalance(postBalance)
            .withNonceChange(nonce);
    if (prior != null) {
      account.withPriorAccount(prior);
    }
    return builder.build();
  }

  private static void assertSameAccountValue(
      final BonsaiValue<? extends AccountValue> actual,
      final BonsaiValue<? extends AccountValue> expected) {
    assertSameAccount(actual.getPrior(), expected.getPrior());
    assertSameAccount(actual.getUpdated(), expected.getUpdated());
  }

  private static void assertSameAccount(final AccountValue actual, final AccountValue expected) {
    if (expected == null) {
      assertThat(actual).isNull();
      return;
    }
    assertThat(actual).isNotNull();
    assertThat(actual.getNonce()).isEqualTo(expected.getNonce());
    assertThat(actual.getBalance()).isEqualTo(expected.getBalance());
    assertThat(actual.getStorageRoot()).isEqualTo(expected.getStorageRoot());
    assertThat(actual.getCodeHash()).isEqualTo(expected.getCodeHash());
  }

  private static TrieLogLayer trieLog(final BonsaiWorldStateUpdateAccumulator accumulator) {
    return new BonsaiTrieLogFactory()
        .create(
            accumulator,
            new BlockHeaderTestFixture().number(1).stateRoot(Hash.EMPTY).buildHeader());
  }

  private static BonsaiWorldState newWorldStateWithAccount() {
    final BonsaiWorldState worldState = newEmptyWorldState();
    final BonsaiWorldStateUpdateAccumulator accumulator =
        (BonsaiWorldStateUpdateAccumulator) worldState.updater();
    accumulator.createAccount(ACCOUNT, 3L, Wei.of(10));
    accumulator.getAccount(ACCOUNT).setStorageValue(SLOT.getSlotKey().orElseThrow(), V0);
    accumulator.commit();
    worldState.persist(null);
    return worldState;
  }

  private static PartialBlockAccessView partialView(
      final long txIndex, final UInt256 prior, final UInt256 updated) {
    final PartialBlockAccessView.PartialBlockAccessViewBuilder builder =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(txIndex);
    builder.getOrCreateAccountBuilder(ACCOUNT).addStorageChange(SLOT, prior, updated);
    return builder.build();
  }

  private static BonsaiWorldState newEmptyWorldState() {
    final BonsaiWorldStateKeyValueStorage storage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
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
