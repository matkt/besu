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
package org.hyperledger.besu.ethereum.trie.pathbased.pbt.staterootcommitter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.StateRootCommitterFactory;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary.DefaultBinaryStateRootCommitter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BinaryBalStateRootCommitterTest {

  private static final GenesisConfig EMPTY_BINARY_GENESIS =
      GenesisConfig.fromResource(
          "/org/hyperledger/besu/ethereum/trie/pathbased/pbt/empty-amsterdam-genesis.json");

  private ExecutionContextTestFixture contextTestFixture;
  private ProtocolContext protocolContext;
  private BlockHeader chainHeadHeader;
  private StateRootCommitterFactory factory;

  @BeforeEach
  void setUp() {
    contextTestFixture =
        ExecutionContextTestFixture.builder(EMPTY_BINARY_GENESIS)
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    protocolContext = contextTestFixture.getProtocolContext();
    chainHeadHeader = contextTestFixture.getBlockchain().getChainHeadHeader();
    factory = new StateRootCommitterFactory(ImmutableBalConfiguration.builder().build());
  }

  @AfterEach
  void tearDown() throws Exception {
    contextTestFixture.getStateArchive().close();
  }

  @Test
  void binaryAndBalCommitterProduceSameRoot_balanceAndNonce() {
    final Address address = testAddress("a1");
    final Wei newBalance = Wei.of(999_999L);
    final long newNonce = 5L;
    final BlockAccessList bal = balanceAndNonceBal(address, newBalance, newNonce);
    final Hash expectedRoot =
        computeBinaryRoot(
            accumulator -> {
              final MutableAccount account = accumulator.getOrCreate(address);
              account.setBalance(newBalance);
              account.setNonce(newNonce);
            });
    final BlockHeader blockHeader = childHeader(expectedRoot);

    try (BonsaiWorldState worldState = getWorldState(false)) {
      applyBalanceAndNonce(worldState, address, newBalance, newNonce);
      final StateRootCommitter committer =
          factory.forBlock(
              protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
      worldState.persist(blockHeader, committer);
      assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
    }
  }

  @Test
  void binaryAndBalCommitterProduceSameRoot_codeAndStorage() {
    final Address address = testAddress("ee");
    final StorageSlotKey slotKey = new StorageSlotKey(UInt256.valueOf(5));
    final UInt256 slotValue = UInt256.valueOf(42);
    final Bytes code = Bytes.fromHexString("0x608060405234801561001057600080fd5b50");
    final Wei balance = Wei.of(3_000_000L);

    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    address,
                    List.of(new SlotChanges(slotKey, List.of(new StorageChange(0, slotValue)))),
                    List.of(),
                    List.of(new BalanceChange(0, balance)),
                    List.of(),
                    List.of(new CodeChange(0, code)))));

    final Hash expectedRoot =
        computeBinaryRoot(
            accumulator -> {
              final MutableAccount account = accumulator.getOrCreate(address);
              account.setBalance(balance);
              account.setCode(code);
              account.setStorageValue(slotKey.getSlotKey().orElseThrow(), slotValue);
            });
    final BlockHeader blockHeader = childHeader(expectedRoot);

    try (BonsaiWorldState worldState = getWorldState(false)) {
      final WorldUpdater updater = worldState.updater();
      final MutableAccount account = updater.getOrCreate(address);
      account.setBalance(balance);
      account.setCode(code);
      account.setStorageValue(slotKey.getSlotKey().orElseThrow(), slotValue);
      updater.commit();

      final StateRootCommitter committer =
          factory.forBlock(
              protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
      worldState.persist(blockHeader, committer);
      assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
    }
  }

  @Test
  void emptyBalAccessList_producesParentRoot() {
    final BlockAccessList bal = new BlockAccessList(List.of());

    try (BonsaiWorldState worldState = getWorldState(false)) {
      final Hash expectedRoot = worldState.rootHash();
      final BlockHeader blockHeader = childHeader(expectedRoot);
      final StateRootCommitter committer =
          factory.forBlock(
              protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
      worldState.persist(blockHeader, committer);
      assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
    }
  }

  @Test
  void balRootMismatchThrowsException() {
    final Address address = testAddress("b2");
    final Wei balBalance = Wei.of(1_000_000L);
    final BlockAccessList bal = balanceAndNonceBal(address, balBalance, 0L);
    final Hash wrongRoot =
        Hash.fromHexString("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
    final BlockHeader blockHeader = childHeader(wrongRoot);

    try (BonsaiWorldState worldState = getWorldState(false)) {
      applyBalanceAndNonce(worldState, address, balBalance, 0L);
      final StateRootCommitter committer =
          factory.forBlock(
              protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());

      assertThatThrownBy(() -> worldState.persist(blockHeader, committer))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("BAL-computed root does not match block header state root");
    }
  }

  @Test
  void cancel_preventsSubsequentCompute() {
    final Address address = testAddress("28");
    final Wei newBalance = Wei.of(9_999_999L);
    final BlockAccessList bal = balanceAndNonceBal(address, newBalance, 0L);
    final Hash expectedRoot =
        computeBinaryRoot(accumulator -> accumulator.getOrCreate(address).setBalance(newBalance));
    final BlockHeader blockHeader = childHeader(expectedRoot);

    try (BonsaiWorldState worldState = getWorldState(false)) {
      applyBalanceAndNonce(worldState, address, newBalance, 0L);
      final StateRootCommitter committer =
          factory.forBlock(
              protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
      committer.cancel();

      assertThatThrownBy(() -> committer.compute(worldState, blockHeader, worldState.updater()))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Background BAL state root computation was cancelled");
    }
  }

  @Test
  void frozenStorage_balComputesRootWithoutWriting() {
    final Address address = testAddress("7e");
    final Wei balBalance = Wei.of(2_345_678L);
    final long balNonce = 7L;
    final BlockAccessList bal = balanceAndNonceBal(address, balBalance, balNonce);
    final Hash expectedRoot =
        computeBinaryRoot(
            accumulator -> {
              final MutableAccount account = accumulator.getOrCreate(address);
              account.setBalance(balBalance);
              account.setNonce(balNonce);
            });
    final BlockHeader blockHeader = childHeader(expectedRoot);

    try (BonsaiWorldState worldState = getWorldState(false)) {
      worldState.freezeStorage();
      assertThat(worldState.isStorageFrozen()).isTrue();
      assertThat(worldState.get(address)).isNull();

      final StateRootCommitter committer =
          factory.forBlock(
              protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
      worldState.persist(blockHeader, committer);

      assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
      assertThat(worldState.get(address)).isNull();
    }
  }

  @Test
  void eip7702Delegation_matchesBinaryCommitter() {
    final Address authority = testAddress("70");
    final Address target = testAddress("71");
    final Bytes delegationCode =
        Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, target.getBytes());
    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    authority,
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(new CodeChange(0, delegationCode)))));

    final Hash expectedRoot =
        computeBinaryRoot(
            accumulator -> accumulator.getOrCreate(authority).setCode(delegationCode));
    final BlockHeader blockHeader = childHeader(expectedRoot);

    try (BonsaiWorldState worldState = getWorldState(false)) {
      final WorldUpdater updater = worldState.updater();
      updater.getOrCreate(authority).setCode(delegationCode);
      updater.commit();

      final StateRootCommitter committer =
          factory.forBlock(
              protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());
      worldState.persist(blockHeader, committer);
      assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
    }
  }

  private BlockHeader childHeader(final Hash stateRoot) {
    return new BlockHeaderTestFixture()
        .parentHash(chainHeadHeader.getHash())
        .number(chainHeadHeader.getNumber() + 1L)
        .stateRoot(stateRoot)
        .buildHeader();
  }

  private static Address testAddress(final String suffix) {
    return Address.fromHexString("0x00000000000000000000000000000000000000" + suffix);
  }

  private static BlockAccessList balanceAndNonceBal(
      final Address address, final Wei balance, final long nonce) {
    return new BlockAccessList(
        List.of(
            new AccountChanges(
                address,
                List.of(),
                List.of(),
                List.of(new BalanceChange(0, balance)),
                List.of(new NonceChange(0, nonce)),
                List.of())));
  }

  private static void applyBalanceAndNonce(
      final BonsaiWorldState worldState,
      final Address address,
      final Wei balance,
      final long nonce) {
    final WorldUpdater updater = worldState.updater();
    final MutableAccount account = updater.getOrCreate(address);
    account.setBalance(balance);
    if (nonce > 0) {
      account.setNonce(nonce);
    }
    updater.commit();
  }

  private BonsaiWorldState getWorldState(final boolean shouldUpdateHead) {
    return (BonsaiWorldState)
        protocolContext
            .getWorldStateArchive()
            .getWorldState(
                WorldStateQueryParams.newBuilder()
                    .withParentBlockHeader(chainHeadHeader)
                    .withShouldWorldStateUpdateHead(shouldUpdateHead)
                    .build())
            .orElseThrow();
  }

  private Hash computeBinaryRoot(
      final Consumer<BonsaiWorldStateUpdateAccumulator> accumulatorConsumer) {
    final BonsaiWorldState worldState =
        (BonsaiWorldState)
            protocolContext
                .getWorldStateArchive()
                .getWorldState(
                    WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(chainHeadHeader))
                .orElseThrow();
    try {
      final BonsaiWorldStateUpdateAccumulator accumulator = worldState.updater();
      accumulatorConsumer.accept(accumulator);
      accumulator.commit();
      return new DefaultBinaryStateRootCommitter().compute(worldState, null, accumulator).root();
    } finally {
      worldState.close();
    }
  }
}
