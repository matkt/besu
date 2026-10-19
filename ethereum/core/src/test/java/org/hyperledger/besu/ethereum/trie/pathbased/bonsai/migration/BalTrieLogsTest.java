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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.BasicDataEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.DelegationEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.StoredPartitionedBinaryTrie;
import org.hyperledger.besu.ethereum.trie.common.BinaryTrieAccountValue;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/** EIP-8347 BAL-replay rules, as roll-forward trie logs. */
class BalTrieLogsTest {

  private static final Address ALICE =
      Address.fromHexString("0x00000000000000000000000000000000000000a1");
  private static final Address BOB =
      Address.fromHexString("0x00000000000000000000000000000000000000b0");
  private static final StorageSlotKey SLOT = new StorageSlotKey(UInt256.valueOf(7));

  /** Parent-block PBT, in memory, and the code store. */
  private static final class State {
    final StoredPartitionedBinaryTrie pbt =
        new StoredPartitionedBinaryTrie((location, hash) -> Optional.empty(), Bytes32.ZERO);
    final Map<Hash, Bytes> code = new HashMap<>();

    State account(final Address address, final long nonce, final Wei balance, final Bytes code) {
      final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
      final boolean delegated = CodeDelegationHelper.hasCodeDelegation(code);
      pbt.put(
          TrieKeyDerivation.getTreeKeyForBasicData(address32),
          BasicDataEncoder.encodeBasicData(code.size(), nonce, balance.toUInt256()));
      if (delegated) {
        pbt.put(
            TrieKeyDerivation.getTreeKeyForDelegation(address32),
            DelegationEncoder.encodeDelegation(
                CodeDelegationHelper.getTargetAddress(code).getBytes()));
      } else {
        final Hash codeHash = code.isEmpty() ? Hash.EMPTY : Hash.hash(code);
        pbt.put(TrieKeyDerivation.getTreeKeyForCodeHash(address32), codeHash.getBytes());
        this.code.put(codeHash, code);
      }
      return this;
    }

    State slot(final Address address, final StorageSlotKey slot, final UInt256 value) {
      pbt.put(
          TrieKeyDerivation.getTreeKeyForStorageSlot(
              TrieKeyDerivation.address20ToAddress32(address.getBytes()),
              slot.getSlotKey().orElseThrow()),
          Bytes32.leftPad(value));
      return this;
    }
  }

  private static AccountChanges changes(
      final Address address,
      final List<SlotChanges> slots,
      final List<BalanceChange> balance,
      final List<NonceChange> nonce,
      final List<CodeChange> code) {
    return new AccountChanges(address, slots, List.of(), balance, nonce, code);
  }

  private static TrieLog forward(final State state, final AccountChanges... accounts) {
    return BalTrieLogs.forward(
        new BlockHeaderTestFixture().number(5).buildHeader(),
        new BlockAccessList(List.of(accounts)),
        state.pbt,
        (address, codeHash) -> state.code.getOrDefault(codeHash, Bytes.EMPTY));
  }

  @Test
  void transferToAFreshAddressCreatesTheAccountWithEmptyCodeHash() {
    final TrieLog log =
        forward(
            new State(),
            changes(
                BOB, List.of(), List.of(new BalanceChange(0, Wei.of(10))), List.of(), List.of()));

    final TrieLog.LogTuple<?> change = log.getAccountChanges().get(BOB);
    assertThat(change.getPrior()).isNull();
    assertThat(change.getUpdated())
        .isEqualTo(new BinaryTrieAccountValue(0, Wei.of(10), Hash.EMPTY));
  }

  @Test
  void balanceOnlyChangeKeepsNonceAndCodeHash() {
    final Bytes code = Bytes.fromHexString("0x6000");
    final Hash codeHash = Hash.hash(code);
    final State state = new State().account(ALICE, 3, Wei.of(100), code);

    final TrieLog log =
        forward(
            state,
            changes(
                ALICE, List.of(), List.of(new BalanceChange(0, Wei.of(40))), List.of(), List.of()));

    final TrieLog.LogTuple<?> change = log.getAccountChanges().get(ALICE);
    assertThat(change.getPrior()).isEqualTo(new BinaryTrieAccountValue(3, Wei.of(100), codeHash));
    assertThat(change.getUpdated()).isEqualTo(new BinaryTrieAccountValue(3, Wei.of(40), codeHash));
    assertThat(log.getCodeChanges()).isEmpty();
  }

  @Test
  void accountLeftEmptyIsDeleted() {
    final State state = new State().account(ALICE, 0, Wei.of(5), Bytes.EMPTY);

    final TrieLog log =
        forward(
            state,
            changes(
                ALICE, List.of(), List.of(new BalanceChange(0, Wei.ZERO)), List.of(), List.of()));

    assertThat(log.getAccountChanges().get(ALICE).getUpdated()).isNull();
  }

  @Test
  void zeroWriteDeletesAPresentSlotAndIgnoresAnAbsentOne() {
    final State state =
        new State().account(ALICE, 1, Wei.ONE, Bytes.EMPTY).slot(ALICE, SLOT, UInt256.valueOf(9));
    final SlotChanges cleared = new SlotChanges(SLOT, List.of(new StorageChange(0, UInt256.ZERO)));
    final StorageSlotKey other = new StorageSlotKey(UInt256.valueOf(8));
    final SlotChanges clearedAbsent =
        new SlotChanges(other, List.of(new StorageChange(0, UInt256.ZERO)));

    final TrieLog log =
        forward(
            state,
            changes(ALICE, List.of(cleared, clearedAbsent), List.of(), List.of(), List.of()));

    final var slots = log.getStorageChanges().get(ALICE);
    assertThat(slots.get(SLOT).getPrior()).isEqualTo(UInt256.valueOf(9));
    assertThat(slots.get(SLOT).getUpdated()).isEqualTo(UInt256.ZERO);
    assertThat(slots).doesNotContainKey(other);
    assertThat(log.getAccountChanges()).doesNotContainKey(ALICE);
  }

  @Test
  void nonZeroWriteNeedsNoPrior() {
    final State state = new State().account(ALICE, 1, Wei.ONE, Bytes.EMPTY);
    final SlotChanges write =
        new SlotChanges(SLOT, List.of(new StorageChange(0, UInt256.valueOf(4))));

    final TrieLog log =
        forward(state, changes(ALICE, List.of(write), List.of(), List.of(), List.of()));

    assertThat(log.getStorageChanges().get(ALICE).get(SLOT).getPrior()).isNull();
    assertThat(log.getStorageChanges().get(ALICE).get(SLOT).getUpdated())
        .isEqualTo(UInt256.valueOf(4));
  }

  @Test
  void clearingADelegationReadsThePriorIndicatorFromThePbt() {
    final Bytes indicator =
        Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, BOB.getBytes());
    final State state = new State().account(ALICE, 4, Wei.ONE, indicator);

    final TrieLog log =
        forward(
            state,
            changes(
                ALICE, List.of(), List.of(), List.of(), List.of(new CodeChange(0, Bytes.EMPTY))));

    assertThat(log.getAccountChanges().get(ALICE).getPrior())
        .isEqualTo(new BinaryTrieAccountValue(4, Wei.ONE, Hash.hash(indicator)));
    assertThat(log.getAccountChanges().get(ALICE).getUpdated())
        .isEqualTo(new BinaryTrieAccountValue(4, Wei.ONE, Hash.EMPTY));
    assertThat(log.getCodeChanges().get(ALICE).getPrior()).isEqualTo(indicator);
    assertThat(log.getCodeChanges().get(ALICE).getUpdated()).isNull();
  }

  @Test
  void delegatingAnEoaSetsTheIndicatorCodeHashWithNoPriorCode() {
    final State state = new State().account(ALICE, 2, Wei.ONE, Bytes.EMPTY);
    final Bytes indicator =
        Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, BOB.getBytes());

    final TrieLog log =
        forward(
            state,
            changes(
                ALICE,
                List.of(),
                List.of(),
                List.of(new NonceChange(0, 3)),
                List.of(new CodeChange(0, indicator))));

    assertThat(log.getAccountChanges().get(ALICE).getUpdated())
        .isEqualTo(new BinaryTrieAccountValue(3, Wei.ONE, Hash.hash(indicator)));
    assertThat(log.getCodeChanges().get(ALICE).getPrior()).isNull();
    assertThat(log.getCodeChanges().get(ALICE).getUpdated()).isEqualTo(indicator);
  }
}
