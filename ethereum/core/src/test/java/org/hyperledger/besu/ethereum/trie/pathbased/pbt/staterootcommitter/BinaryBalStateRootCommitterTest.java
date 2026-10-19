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
package org.hyperledger.besu.ethereum.trie.pathbased.pbt.staterootcommitter;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class BinaryBalStateRootCommitterTest extends AbstractBinaryStateRootCommitterTest {

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
}
