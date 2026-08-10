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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class BinaryStateRootCommitterTest {

  private ExecutionContextTestFixture contextTestFixture;
  private ProtocolContext protocolContext;
  private BlockHeader chainHeadHeader;
  /** Archive head after BINARY empty-root init; used for non-frozen persist tests. */
  private BlockHeader emptyBinaryHead;
  private StateRootCommitterFactory factory;

  @BeforeEach
  void setUp() {
    contextTestFixture =
        ExecutionContextTestFixture.builder(GenesisConfig.mainnet())
            .dataStorageFormat(DataStorageFormat.BINARY)
            .build();
    protocolContext = contextTestFixture.getProtocolContext();
    chainHeadHeader = contextTestFixture.getBlockchain().getChainHeadHeader();
    factory = new StateRootCommitterFactory(ImmutableBalConfiguration.builder().build());
    initializeEmptyBinaryTrieRoot();
  }

  /** Genesis is written with a Merkle root; BINARY committer expects an empty binary trie root. */
  private void initializeEmptyBinaryTrieRoot() {
    try (BonsaiWorldState worldState = getWorldState(true)) {
      final var updater = worldState.getWorldStateStorage().updater();
      updater
          .getWorldStateTransaction()
          .put(
              KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
              PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY,
              new byte[32]);
      updater
          .getWorldStateTransaction()
          .remove(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE, Bytes.EMPTY.toArrayUnsafe());
      updater.commit();
    }
    emptyBinaryHead =
        new BlockHeaderTestFixture()
            .parentHash(chainHeadHeader.getHash())
            .number(chainHeadHeader.getNumber())
            .stateRoot(Hash.ZERO)
            .buildHeader();
    protocolContext.getWorldStateArchive().resetArchiveStateTo(emptyBinaryHead);
  }

  @AfterEach
  void tearDown() throws Exception {
    contextTestFixture.getStateArchive().close();
  }

  @Nested
  class FactorySelection {

    @Test
    void factoryReturnsBinary_whenBinaryFormat() {
      final BlockHeader blockHeader = childHeader(chainHeadHeader.getStateRoot());

      try (BonsaiWorldState worldState = getWorldState(false)) {
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.empty(), worldState.isStorageFrozen());

        assertThat(committer).isInstanceOf(BinaryStateRootCommitter.class);
      }
    }

    @Test
    void factoryReturnsBinary_whenBalPresent() {
      final BlockAccessList bal =
          new BlockAccessList(
              List.of(
                  new AccountChanges(
                      testAddress("a1"),
                      List.of(),
                      List.of(),
                      List.of(new BalanceChange(0, Wei.of(1))),
                      List.of(new NonceChange(0, 1L)),
                      List.of())));
      final BlockHeader blockHeader = childHeader(chainHeadHeader.getStateRoot());

      try (BonsaiWorldState worldState = getWorldState(false)) {
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());

        assertThat(committer).isInstanceOf(BinaryStateRootCommitter.class);
      }
    }
  }

  @Nested
  class BinaryRootComputation {

    @Test
    void balanceAndNonceUpdatesProduceDeterministicRoot() {
      final Address address = testAddress("aa");
      final Wei balance = Wei.of(1_234_567L);
      final long nonce = 7L;

      final Hash firstRoot =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(address);
                account.setBalance(balance);
                account.setNonce(nonce);
              });
      final Hash secondRoot =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(address);
                account.setBalance(balance);
                account.setNonce(nonce);
              });

      assertThat(firstRoot).isEqualTo(secondRoot);
      assertThat(firstRoot).isNotEqualTo(chainHeadHeader.getStateRoot());
    }

    @Test
    void codeAndStorageUpdatesProduceDeterministicRoot() {
      final Address address = testAddress("bb");
      final StorageSlotKey slotKey = new StorageSlotKey(UInt256.valueOf(12));
      final UInt256 slotValue = UInt256.valueOf(0xdeadbeefL);
      final Bytes code = Bytes.fromHexString("0x60016000");

      final Hash root =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(address);
                account.setCode(code);
                account.setStorageValue(slotKey.getSlotKey().orElseThrow(), slotValue);
              });

      assertThat(root).isNotEqualTo(chainHeadHeader.getStateRoot());
      assertThat(root).isNotEqualTo(Hash.EMPTY);
    }

    @Test
    void binaryRootDiffersFromDefaultMerkleRoot() throws Exception {
      final Address address = testAddress("cc");
      final Wei balance = Wei.of(999_999L);
      final long nonce = 5L;

      final Hash binaryRoot =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(address);
                account.setBalance(balance);
                account.setNonce(nonce);
              });

      final ExecutionContextTestFixture bonsaiFixture =
          ExecutionContextTestFixture.builder(GenesisConfig.mainnet())
              .dataStorageFormat(DataStorageFormat.BONSAI)
              .build();
      try {
        final Hash defaultRoot =
            computeDefaultRoot(
                bonsaiFixture.getProtocolContext(),
                bonsaiFixture.getBlockchain().getChainHeadHeader(),
                accumulator -> {
                  final MutableAccount account = accumulator.getOrCreate(address);
                  account.setBalance(balance);
                  account.setNonce(nonce);
                });
        assertThat(binaryRoot).isNotEqualTo(defaultRoot);
      } finally {
        bonsaiFixture.getStateArchive().close();
      }
    }

    @Test
    void persistWithBinaryCommitter_updatesWorldStateRoot() {
      final Address address = testAddress("dd");
      final Wei balance = Wei.of(555_555L);
      final long nonce = 3L;

      final Hash expectedRoot =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(address);
                account.setBalance(balance);
                account.setNonce(nonce);
              });
      final BlockHeader blockHeader = childHeader(expectedRoot);

      try (BonsaiWorldState worldState = getWorldState(false)) {
        applyBalanceAndNonce(worldState, address, balance, nonce);
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.empty(), worldState.isStorageFrozen());
        worldState.persist(blockHeader, committer);

        assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
      }
    }

    @Test
    void storageZeroingRestoresRootWithoutSlotLeaf() {
      final Address address = testAddress("ff");
      final StorageSlotKey slotKey = new StorageSlotKey(UInt256.valueOf(3));
      final UInt256 slotValue = UInt256.valueOf(99);

      final Hash rootWithSlotOnly =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(address);
                account.setStorageValue(slotKey.getSlotKey().orElseThrow(), slotValue);
              });

      final Hash rootWithAccountOnly =
          computeBinaryRoot(accumulator -> accumulator.getOrCreate(address));

      final Hash rootAfterZeroing =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(address);
                account.setStorageValue(slotKey.getSlotKey().orElseThrow(), slotValue);
                account.setStorageValue(slotKey.getSlotKey().orElseThrow(), UInt256.ZERO);
              });

      assertThat(rootWithSlotOnly).isNotEqualTo(rootWithAccountOnly);
      assertThat(rootAfterZeroing).isEqualTo(rootWithAccountOnly);
    }

    @Test
    void codeReplacementRootMatchesDirectDeployment() {
      final Address address = testAddress("11");
      final Bytes firstCode = Bytes.fromHexString("0x6001");
      final Bytes finalCode = Bytes.fromHexString("0x600160005260016000f3");

      final Hash rootDirect =
          computeBinaryRoot(accumulator -> accumulator.getOrCreate(address).setCode(finalCode));

      final Hash rootAfterReplacement =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(address);
                account.setCode(firstCode);
                account.setCode(finalCode);
              });

      assertThat(rootAfterReplacement).isEqualTo(rootDirect);
    }

    @Test
    void eip7702DelegationUsesHeaderLeafNotCodeZone() {
      final Address authority = testAddress("70");
      final Address target = testAddress("71");
      final Bytes delegationCode =
          Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, target.getBytes());

      final Hash rootWithDelegation =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(authority);
                account.setCode(delegationCode);
              });

      final Hash rootCleared =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(authority);
                account.setCode(delegationCode);
                account.setCode(Bytes.EMPTY);
              });

      final Hash rootEmptyAccount =
          computeBinaryRoot(accumulator -> accumulator.getOrCreate(authority));

      assertThat(rootWithDelegation).isNotEqualTo(rootEmptyAccount);
      assertThat(rootCleared).isEqualTo(rootEmptyAccount);
    }

    @Test
    void eoaToDelegationToClearRestoresEmptyAccountRoot() {
      final Address authority = testAddress("74");
      final Address target = testAddress("75");
      final Bytes delegationCode =
          Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, target.getBytes());

      final Hash rootEmptyAccount =
          computeBinaryRoot(accumulator -> accumulator.getOrCreate(authority));

      final Hash rootAfterSwitchAndClear =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(authority);
                // EOA empty → delegation → clear back to empty code-hash mode
                account.setBalance(Wei.ZERO);
                account.setCode(delegationCode);
                account.setCode(Bytes.EMPTY);
              });

      assertThat(rootAfterSwitchAndClear).isEqualTo(rootEmptyAccount);
    }

    @Test
    void delegationToContractCodeMatchesDirectDeploy() {
      final Address authority = testAddress("76");
      final Address target = testAddress("77");
      final Bytes delegationCode =
          Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, target.getBytes());
      final Bytes contractCode = Bytes.fromHexString("0x600160005260016000f3");

      final Hash rootDirect =
          computeBinaryRoot(accumulator -> accumulator.getOrCreate(authority).setCode(contractCode));

      final Hash rootViaDelegationSwitch =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(authority);
                account.setCode(delegationCode);
                account.setCode(contractCode);
              });

      assertThat(rootViaDelegationSwitch).isEqualTo(rootDirect);
    }

    @Test
    void sameModeBalanceUpdateMatchesDirectDelegatedAccount() {
      final Address authority = testAddress("78");
      final Address target = testAddress("79");
      final Bytes delegationCode =
          Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, target.getBytes());
      final Wei balance = Wei.of(42);

      final Hash rootDirect =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(authority);
                account.setCode(delegationCode);
                account.setBalance(balance);
              });

      // Same final mode (delegation): balance-only churn must not disturb header mutual exclusion.
      final Hash rootAfterBalanceChurn =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(authority);
                account.setCode(delegationCode);
                account.setBalance(Wei.of(1));
                account.setBalance(balance);
              });

      assertThat(rootAfterBalanceChurn).isEqualTo(rootDirect);
    }

    @Test
    void accountDeletionClearsHeaderLeaves() {
      final Address address = testAddress("80");
      final Wei balance = Wei.of(100);
      final long nonce = 1L;

      final Hash rootWithAccount =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(address);
                account.setBalance(balance);
                account.setNonce(nonce);
              });
      final BlockHeader headerWithAccount = childHeader(rootWithAccount);

      // Non-frozen head: persist writes flat-DB + trie so delete loads prior (invariant).
      try (BonsaiWorldState worldState =
          (BonsaiWorldState)
              protocolContext
                  .getWorldStateArchive()
                  .getWorldState(
                      WorldStateQueryParams.newBuilder()
                          .withBlockHeader(emptyBinaryHead)
                          .withShouldWorldStateUpdateHead(true)
                          .build())
                  .orElseThrow()) {
        applyBalanceAndNonce(worldState, address, balance, nonce);
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, headerWithAccount, Optional.empty(), worldState.isStorageFrozen());
        worldState.persist(headerWithAccount, committer);
        assertThat(worldState.rootHash()).isEqualTo(rootWithAccount);
        assertThat(worldState.get(address)).isNotNull();

        final BonsaiWorldStateUpdateAccumulator accumulator =
            (BonsaiWorldStateUpdateAccumulator) worldState.updater();
        accumulator.deleteAccount(address);
        accumulator.commit();
        assertThat(accumulator.getAccountsToUpdate().get(address).getPrior()).isNotNull();

        final Hash rootAfterDelete =
            new BinaryStateRootCommitter().compute(worldState, null, accumulator).root();
        assertThat(rootAfterDelete).isNotEqualTo(rootWithAccount);
        assertThat(rootAfterDelete).isEqualTo(Hash.ZERO);
      }
    }

    @Test
    void createThenDeleteAccountInSameBatchRestoresEmptyRoot() {
      final Address address = testAddress("81");

      final Hash rootAfterCreateAndDelete =
          computeBinaryRoot(
              accumulator -> {
                accumulator.getOrCreate(address).setBalance(Wei.of(7));
                accumulator.deleteAccount(address);
              });

      assertThat(rootAfterCreateAndDelete).isEqualTo(Hash.ZERO);
    }

    @Test
    void storageRollbackAfterPersistRestoresAccountOnlyRoot() {
      final Address address = testAddress("82");
      final StorageSlotKey slotKey = new StorageSlotKey(UInt256.valueOf(9));
      final UInt256 slotValue = UInt256.valueOf(123);

      final Hash rootAccountOnly =
          computeBinaryRoot(accumulator -> accumulator.getOrCreate(address));
      final Hash rootWithSlot =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(address);
                account.setStorageValue(slotKey.getSlotKey().orElseThrow(), slotValue);
              });
      final BlockHeader headerWithSlot = childHeader(rootWithSlot);

      try (BonsaiWorldState worldState = getWorldState(false)) {
        final WorldUpdater updater = worldState.updater();
        final MutableAccount account = updater.getOrCreate(address);
        account.setStorageValue(slotKey.getSlotKey().orElseThrow(), slotValue);
        updater.commit();

        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, headerWithSlot, Optional.empty(), worldState.isStorageFrozen());
        worldState.persist(headerWithSlot, committer);
        assertThat(worldState.rootHash()).isEqualTo(rootWithSlot);

        final BonsaiWorldStateUpdateAccumulator accumulator =
            (BonsaiWorldStateUpdateAccumulator) worldState.updater();
        accumulator
            .getOrCreate(address)
            .setStorageValue(slotKey.getSlotKey().orElseThrow(), UInt256.ZERO);
        accumulator.commit();

        final Hash rootAfterZero =
            new BinaryStateRootCommitter().compute(worldState, null, accumulator).root();
        assertThat(rootAfterZero).isEqualTo(rootAccountOnly);
      }
    }

    @Test
    void codeClearOfDelegationRestoresEmptyAccountRoot() {
      final Address authority = testAddress("83");
      final Address target = testAddress("84");
      final Bytes delegationCode =
          Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, target.getBytes());

      final Hash rootEmptyAccount =
          computeBinaryRoot(accumulator -> accumulator.getOrCreate(authority));

      final Hash rootCleared =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(authority);
                account.setCode(delegationCode);
                account.setCode(Bytes.EMPTY);
              });

      assertThat(rootCleared).isEqualTo(rootEmptyAccount);
    }

    @Test
    void persistWithBinaryCommitter_codeAndStorageUpdates() {
      final Address address = testAddress("ee");
      final StorageSlotKey slotKey = new StorageSlotKey(UInt256.valueOf(5));
      final UInt256 slotValue = UInt256.valueOf(42);
      final Bytes code = Bytes.fromHexString("0x608060405234801561001057600080fd5b50");

      final Hash expectedRoot =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount account = accumulator.getOrCreate(address);
                account.setCode(code);
                account.setStorageValue(slotKey.getSlotKey().orElseThrow(), slotValue);
              });
      final BlockHeader blockHeader = childHeader(expectedRoot);

      try (BonsaiWorldState worldState = getWorldState(false)) {
        final WorldUpdater updater = worldState.updater();
        final MutableAccount account = updater.getOrCreate(address);
        account.setCode(code);
        account.setStorageValue(slotKey.getSlotKey().orElseThrow(), slotValue);
        updater.commit();

        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.empty(), worldState.isStorageFrozen());
        worldState.persist(blockHeader, committer);

        assertThat(worldState.rootHash()).isEqualTo(expectedRoot);
      }
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
                    .withBlockHeader(chainHeadHeader)
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
      final BonsaiWorldStateUpdateAccumulator accumulator =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();
      accumulatorConsumer.accept(accumulator);
      accumulator.commit();
      return new BinaryStateRootCommitter().compute(worldState, null, accumulator).root();
    } finally {
      worldState.close();
    }
  }

  private Hash computeDefaultRoot(
      final ProtocolContext context,
      final BlockHeader headHeader,
      final Consumer<BonsaiWorldStateUpdateAccumulator> accumulatorConsumer) {
    final BonsaiWorldState worldState =
        (BonsaiWorldState)
            context
                .getWorldStateArchive()
                .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(headHeader))
                .orElseThrow();
    try {
      final BonsaiWorldStateUpdateAccumulator accumulator =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();
      accumulatorConsumer.accept(accumulator);
      accumulator.commit();
      return new DefaultStateRootCommitter().compute(worldState, null, accumulator).root();
    } finally {
      worldState.close();
    }
  }
}
