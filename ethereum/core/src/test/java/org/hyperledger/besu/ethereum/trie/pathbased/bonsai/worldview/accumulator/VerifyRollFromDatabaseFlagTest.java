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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class VerifyRollFromDatabaseFlagTest {

  private static final Address TEST_ADDRESS =
      Address.fromHexString("0x1111111111111111111111111111111111111111");

  private Blockchain blockchain;
  private BonsaiWorldStateProvider archiveWithRead;
  private BonsaiWorldStateProvider archiveNoRead;
  private InMemoryKeyValueStorageProvider providerWithRead;
  private InMemoryKeyValueStorageProvider providerNoRead;
  private final CodeCache codeCache = new CodeCache();

  @BeforeEach
  void setUp() {
    blockchain = org.mockito.Mockito.mock(Blockchain.class);
    providerWithRead = new InMemoryKeyValueStorageProvider();
    archiveWithRead =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(
            blockchain, EvmConfiguration.DEFAULT, null);
    providerNoRead = new InMemoryKeyValueStorageProvider();
    archiveNoRead =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(
            blockchain, EvmConfiguration.DEFAULT, null);
  }

  private BonsaiWorldState createWorldState(
      final BonsaiWorldStateProvider archive,
      final InMemoryKeyValueStorageProvider provider,
      final boolean verifyRollFromDatabase) {
    final WorldStateConfig config =
        WorldStateConfig.newBuilder()
            .stateful(true)
            .parallelStateRootComputationEnabled(true)
            .trieDisabled(false)
            .verifyRollFromDatabase(verifyRollFromDatabase)
            .build();
    return new BonsaiWorldState(
        archive,
        new BonsaiWorldStateKeyValueStorage(
            provider, new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG),
        EvmConfiguration.DEFAULT,
        config,
        codeCache);
  }

  /**
   * Replace the world state's accumulator with one that uses the spy as wrapped view, so that we
   * can verify calls to get/getCode/getStorageValueByStorageSlotKey on the spy.
   */
  private BonsaiWorldState attachSpyWithAccumulatorUsingSpyAsView(
      final BonsaiWorldState real, final boolean verifyRollFromDatabase) {
    final BonsaiWorldState worldStateSpy = spy(real);
    final BonsaiWorldStateUpdateAccumulator oldAcc =
        (BonsaiWorldStateUpdateAccumulator) real.getAccumulator();
    final BonsaiWorldStateUpdateAccumulator newAcc =
        new BonsaiWorldStateUpdateAccumulator(
            worldStateSpy,
            oldAcc.getAccountPreloader(),
            oldAcc.getStoragePreloader(),
            oldAcc.getEvmConfiguration(),
            codeCache,
            verifyRollFromDatabase);
    worldStateSpy.setAccumulator(newAcc);
    return worldStateSpy;
  }

  @Test
  void rollForward_withVerifyRollFromDatabaseTrue_callsGetOnWorldView() {
    final BonsaiWorldState real = createWorldState(archiveWithRead, providerWithRead, true);
    final BonsaiWorldState worldState = attachSpyWithAccumulatorUsingSpyAsView(real, true);
    final TrieLogLayer layer = new TrieLogLayer();
    final PmtStateTrieAccountValue newAccountValue =
        new PmtStateTrieAccountValue(1, Wei.of(100), Hash.EMPTY, Hash.EMPTY);
    layer.addAccountChange(TEST_ADDRESS, null, newAccountValue);

    final BonsaiWorldStateUpdateAccumulator updater =
        (BonsaiWorldStateUpdateAccumulator) worldState.updater();
    updater.rollForward(layer);

    verify(worldState, atLeast(1)).get(eq(TEST_ADDRESS));
  }

  @Test
  void rollForward_withVerifyRollFromDatabaseFalse_doesNotCallGetOnWorldView() {
    final BonsaiWorldState real = createWorldState(archiveNoRead, providerNoRead, false);
    final BonsaiWorldState worldState = attachSpyWithAccumulatorUsingSpyAsView(real, false);
    final TrieLogLayer layer = new TrieLogLayer();
    final PmtStateTrieAccountValue newAccountValue =
        new PmtStateTrieAccountValue(1, Wei.of(100), Hash.EMPTY, Hash.EMPTY);
    layer.addAccountChange(TEST_ADDRESS, null, newAccountValue);

    final BonsaiWorldStateUpdateAccumulator updater =
        (BonsaiWorldStateUpdateAccumulator) worldState.updater();
    updater.rollForward(layer);

    verify(worldState, never()).get(any(Address.class));
  }

  @Test
  void rollForward_withVerifyRollFromDatabaseTrue_callsGetCodeWhenCodeChangeInLayer() {
    final BonsaiWorldState real = createWorldState(archiveWithRead, providerWithRead, true);
    final BonsaiWorldState worldState = attachSpyWithAccumulatorUsingSpyAsView(real, true);
    final TrieLogLayer layer = new TrieLogLayer();
    final Bytes oldCode = Bytes.EMPTY;
    final Bytes newCode = Bytes.of(0x60, 0x00, 0x60, 0x00, 0x52); // PUSH1 0 PUSH1 0 MSTORE
    layer.addCodeChange(TEST_ADDRESS, oldCode, newCode, Hash.EMPTY);

    final BonsaiWorldStateUpdateAccumulator updater =
        (BonsaiWorldStateUpdateAccumulator) worldState.updater();
    updater.rollForward(layer);

    verify(worldState, atLeast(1)).getCode(any(Address.class), any(Hash.class));
  }

  @Test
  void rollForward_withVerifyRollFromDatabaseFalse_doesNotCallGetCodeWhenCodeChangeInLayer() {
    final BonsaiWorldState real = createWorldState(archiveNoRead, providerNoRead, false);
    final BonsaiWorldState worldState = attachSpyWithAccumulatorUsingSpyAsView(real, false);
    final TrieLogLayer layer = new TrieLogLayer();
    final Bytes oldCode = Bytes.EMPTY;
    final Bytes newCode = Bytes.of(0x60, 0x00, 0x60, 0x00, 0x52);
    layer.addCodeChange(TEST_ADDRESS, oldCode, newCode, Hash.EMPTY);

    final BonsaiWorldStateUpdateAccumulator updater =
        (BonsaiWorldStateUpdateAccumulator) worldState.updater();
    updater.rollForward(layer);

    verify(worldState, never()).getCode(any(Address.class), any(Hash.class));
  }

  @Test
  void rollForward_withVerifyRollFromDatabaseTrue_callsGetStorageValueByStorageSlotKey() {
    final BonsaiWorldState real = createWorldState(archiveWithRead, providerWithRead, true);
    final BonsaiWorldState worldState = attachSpyWithAccumulatorUsingSpyAsView(real, true);
    final TrieLogLayer layer = new TrieLogLayer();
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    layer.addStorageChange(TEST_ADDRESS, slotKey, UInt256.ZERO, UInt256.valueOf(42));

    final BonsaiWorldStateUpdateAccumulator updater =
        (BonsaiWorldStateUpdateAccumulator) worldState.updater();
    updater.rollForward(layer);

    verify(worldState, atLeast(1)).getStorageValueByStorageSlotKey(eq(TEST_ADDRESS), eq(slotKey));
  }

  @Test
  void rollForward_withVerifyRollFromDatabaseFalse_doesNotCallGetStorageValueByStorageSlotKey() {
    final BonsaiWorldState real = createWorldState(archiveNoRead, providerNoRead, false);
    final BonsaiWorldState worldState = attachSpyWithAccumulatorUsingSpyAsView(real, false);
    final TrieLogLayer layer = new TrieLogLayer();
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.ONE);
    layer.addStorageChange(TEST_ADDRESS, slotKey, UInt256.ZERO, UInt256.valueOf(42));

    final BonsaiWorldStateUpdateAccumulator updater =
        (BonsaiWorldStateUpdateAccumulator) worldState.updater();
    updater.rollForward(layer);

    verify(worldState, never()).getStorageValueByStorageSlotKey(any(Address.class), any());
  }

  @Test
  void rollBack_withVerifyRollFromDatabaseFalse_doesNotCallGetOnWorldView() {
    final BonsaiWorldState real = createWorldState(archiveNoRead, providerNoRead, false);
    final BonsaiWorldState worldState = attachSpyWithAccumulatorUsingSpyAsView(real, false);
    final TrieLogLayer layer = new TrieLogLayer();
    final PmtStateTrieAccountValue priorValue =
        new PmtStateTrieAccountValue(0, Wei.ZERO, Hash.EMPTY, Hash.EMPTY);
    final PmtStateTrieAccountValue updatedValue =
        new PmtStateTrieAccountValue(1, Wei.of(100), Hash.EMPTY, Hash.EMPTY);
    layer.addAccountChange(TEST_ADDRESS, priorValue, updatedValue);

    final BonsaiWorldStateUpdateAccumulator updater =
        (BonsaiWorldStateUpdateAccumulator) worldState.updater();
    updater.rollBack(layer);

    verify(worldState, never()).get(any(Address.class));
  }
}
