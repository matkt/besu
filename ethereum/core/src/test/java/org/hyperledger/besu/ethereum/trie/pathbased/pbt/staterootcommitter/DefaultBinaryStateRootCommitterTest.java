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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.BalStateRootCommitter;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.StateRootCommitterFactory;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary.DefaultBinaryStateRootCommitter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DefaultBinaryStateRootCommitterTest extends AbstractBinaryStateRootCommitterTest {

  @Nested
  class FactorySelection {

    @Test
    void factoryReturnsBinary_whenBinaryFormat() {
      final BlockHeader blockHeader = childHeader(chainHeadHeader.getStateRoot());

      try (BonsaiWorldState worldState = getWorldState(false)) {
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.empty(), worldState.isStorageFrozen());

        assertThat(committer).isInstanceOf(DefaultBinaryStateRootCommitter.class);
      }
    }

    @Test
    void factoryReturnsBinaryBal_whenBalPresentAndEnabled() {
      final BlockAccessList bal = balanceAndNonceBal(testAddress("a1"), Wei.of(1), 1L);
      final BlockHeader blockHeader = childHeader(chainHeadHeader.getStateRoot());

      try (BonsaiWorldState worldState = getWorldState(false)) {
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());

        assertThat(committer).isInstanceOf(BalStateRootCommitter.class);
        assertThat(committer.getTrieBranchType()).isEqualTo(TrieBranchType.BINARY);
        // waits for the background computation before the archive closes
        committer.compute(worldState, null, worldState.updater());
      }
    }

    @Test
    void factoryReturnsBinary_whenBalPresentButDisabled() {
      final BlockAccessList bal = balanceAndNonceBal(testAddress("a1"), Wei.of(1), 1L);
      final BlockHeader blockHeader = childHeader(chainHeadHeader.getStateRoot());
      final StateRootCommitterFactory disabledFactory =
          new StateRootCommitterFactory(
              ImmutableBalConfiguration.builder().isBalStateRootEnabled(false).build());

      try (BonsaiWorldState worldState = getWorldState(false)) {
        final StateRootCommitter committer =
            disabledFactory.forBlock(
                protocolContext, blockHeader, Optional.of(bal), worldState.isStorageFrozen());

        assertThat(committer).isInstanceOf(DefaultBinaryStateRootCommitter.class);
      }
    }
  }

  @Nested
  class BinaryRootComputation {

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
                          .withBlockHeader(chainHeadHeader)
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
            new DefaultBinaryStateRootCommitter().compute(worldState, null, accumulator).root();
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
            new DefaultBinaryStateRootCommitter().compute(worldState, null, accumulator).root();
        assertThat(rootAfterZero).isEqualTo(rootAccountOnly);
      }
    }

    @Test
    void reloadAcrossBlocks_accumulatesPriorState() {
      final Address addressA = testAddress("a0");
      final Address addressB = testAddress("b0");
      final Wei balanceA = Wei.of(111);
      final Wei balanceB = Wei.of(222);
      final long nonceA = 1L;
      final long nonceB = 2L;

      // Reference root: a fresh trie holding both A and B, built from empty.
      final Hash expectedBoth =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount a = accumulator.getOrCreate(addressA);
                a.setBalance(balanceA);
                a.setNonce(nonceA);
                final MutableAccount b = accumulator.getOrCreate(addressB);
                b.setBalance(balanceB);
                b.setNonce(nonceB);
              });
      // Root of A alone, used as the post-persist head for block 1.
      final Hash rootA =
          computeBinaryRoot(
              accumulator -> {
                final MutableAccount a = accumulator.getOrCreate(addressA);
                a.setBalance(balanceA);
                a.setNonce(nonceA);
              });
      final BlockHeader headerA = childHeader(rootA);

      try (BonsaiWorldState worldState =
          (BonsaiWorldState)
              protocolContext
                  .getWorldStateArchive()
                  .getWorldState(
                      WorldStateQueryParams.newBuilder()
                          .withBlockHeader(chainHeadHeader)
                          .withShouldWorldStateUpdateHead(true)
                          .build())
                  .orElseThrow()) {
        // Block 1: persist A only. Trie nodes for A are written to storage.
        applyBalanceAndNonce(worldState, addressA, balanceA, nonceA);
        final StateRootCommitter committerA =
            factory.forBlock(
                protocolContext, headerA, Optional.empty(), worldState.isStorageFrozen());
        worldState.persist(headerA, committerA);
        assertThat(worldState.rootHash()).isEqualTo(rootA);

        // Block 2: add B without touching A. Correctness requires reloading A's
        // stored trie nodes; if the KV hash filter rejects them, the root would
        // reflect only B and fail to equal expectedBoth.
        final BonsaiWorldStateUpdateAccumulator accumulator =
            (BonsaiWorldStateUpdateAccumulator) worldState.updater();
        final MutableAccount b = accumulator.getOrCreate(addressB);
        b.setBalance(balanceB);
        b.setNonce(nonceB);
        accumulator.commit();

        final Hash rootAfterB =
            new DefaultBinaryStateRootCommitter().compute(worldState, null, accumulator).root();
        assertThat(rootAfterB).isEqualTo(expectedBoth);
      }
    }

    @Test
    void delegationBecomesContractAfterPersist() {
      final Address address = testAddress("85");
      final Bytes delegationCode =
          Bytes.concatenate(
              CodeDelegationHelper.CODE_DELEGATION_PREFIX, testAddress("86").getBytes());
      final Bytes contractCode = Bytes.fromHexString("0x600160005260016000f3");

      final Hash rootWithDelegation =
          computeBinaryRoot(
              accumulator -> accumulator.getOrCreate(address).setCode(delegationCode));
      final Hash rootWithContract =
          computeBinaryRoot(accumulator -> accumulator.getOrCreate(address).setCode(contractCode));
      final BlockHeader headerWithDelegation = childHeader(rootWithDelegation);

      try (BonsaiWorldState worldState = getWorldState(true)) {
        final WorldUpdater updater = worldState.updater();
        updater.getOrCreate(address).setCode(delegationCode);
        updater.commit();
        final StateRootCommitter committer =
            factory.forBlock(
                protocolContext,
                headerWithDelegation,
                Optional.empty(),
                worldState.isStorageFrozen());
        worldState.persist(headerWithDelegation, committer);
        assertThat(worldState.rootHash()).isEqualTo(rootWithDelegation);

        final BonsaiWorldStateUpdateAccumulator accumulator =
            (BonsaiWorldStateUpdateAccumulator) worldState.updater();
        accumulator.getOrCreate(address).setCode(contractCode);
        accumulator.commit();

        final Hash rootAfterChange =
            new DefaultBinaryStateRootCommitter().compute(worldState, null, accumulator).root();
        assertThat(rootAfterChange).isEqualTo(rootWithContract);
      }
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
}
