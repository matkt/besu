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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListChanges.AccountFinalChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListChanges.StorageFinalChange;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.BasicDataEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.DelegationEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.StoredPartitionedBinaryTrie;
import org.hyperledger.besu.ethereum.trie.common.BinaryTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogLayer;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Builds one roll-forward {@link TrieLog} for consecutive blocks from their EIP-7928 Block-Level
 * Access Lists.
 *
 * <p>A BAL records post-values only, so the result rolls forward, never back. The last value a BAL
 * of the run writes is the value after its last block, and what none of them writes is still the
 * parent's: {@code pbt} is the PBT of the parent of the first block, read for what the BALs leave
 * out: the unchanged fields of the basic-data leaf, and whether an account or slot exists, which
 * the binary committer needs to create, delete, and switch EIP-7702 delegation leaves. The updated
 * values follow EIP-8347 BAL-replay: an account left with nonce 0, balance 0 and no code is
 * deleted, and a zero storage write deletes the slot.
 */
final class BalTrieLogs {

  private BalTrieLogs() {}

  /**
   * The roll-forward trie log of the blocks of {@code bals}, the last one being {@code header}.
   *
   * @param bals BALs of consecutive blocks, oldest first
   * @param pbt PBT of the parent of the first block
   * @param code code of an existing account by address and code hash (the code store)
   */
  static TrieLog forward(
      final BlockHeader header,
      final List<BlockAccessList> bals,
      final StoredPartitionedBinaryTrie pbt,
      final BiFunction<Address, Hash, Bytes> code) {
    final TrieLogLayer layer =
        new TrieLogLayer().setBlockHash(header.getBlockHash()).setBlockNumber(header.getNumber());
    for (final AccountFinalChanges changes : latestChanges(bals)) {
      final Address address = changes.address();
      final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
      if (changes.balance().isPresent()
          || changes.nonce().isPresent()
          || changes.code().isPresent()) {
        addAccountChange(layer, header, changes, address32, pbt, code);
      }
      for (final StorageFinalChange slot : changes.storageChanges()) {
        if (!slot.value().isZero()) {
          layer.addStorageChange(address, slot.slot(), null, slot.value());
          continue;
        }
        // A zero write deletes the slot: a no-op when absent, else its prior marks the change.
        pbt.get(
                TrieKeyDerivation.getTreeKeyForStorageSlot(
                    address32, slot.slot().getSlotKey().orElseThrow()))
            .map(value -> UInt256.fromBytes(Bytes32.wrap(value)))
            .ifPresent(prior -> layer.addStorageChange(address, slot.slot(), prior, UInt256.ZERO));
      }
    }
    layer.freeze();
    return layer;
  }

  /** Each account's last balance, nonce, code and slot values over {@code bals}, in block order. */
  private static Collection<AccountFinalChanges> latestChanges(final List<BlockAccessList> bals) {
    final Map<Address, AccountFinalChanges> latest = new LinkedHashMap<>();
    for (final BlockAccessList bal : bals) {
      for (final AccountFinalChanges changes : BlockAccessListChanges.latestChanges(bal)) {
        latest.merge(changes.address(), changes, BalTrieLogs::later);
      }
    }
    return latest.values();
  }

  private static AccountFinalChanges later(
      final AccountFinalChanges earlier, final AccountFinalChanges later) {
    final Map<StorageSlotKey, StorageFinalChange> slots = new LinkedHashMap<>();
    earlier.storageChanges().forEach(slot -> slots.put(slot.slot(), slot));
    later.storageChanges().forEach(slot -> slots.put(slot.slot(), slot));
    return new AccountFinalChanges(
        later.address(),
        later.balance().or(earlier::balance),
        later.nonce().or(earlier::nonce),
        later.code().or(earlier::code),
        List.copyOf(slots.values()));
  }

  private static void addAccountChange(
      final TrieLogLayer layer,
      final BlockHeader header,
      final AccountFinalChanges changes,
      final Bytes32 address32,
      final StoredPartitionedBinaryTrie pbt,
      final BiFunction<Address, Hash, Bytes> code) {
    final Optional<BinaryTrieAccountValue> prior = account(pbt, address32);
    final long nonce =
        changes.nonce().orElse(prior.map(BinaryTrieAccountValue::getNonce).orElse(0L));
    final Wei balance =
        changes.balance().orElse(prior.map(BinaryTrieAccountValue::getBalance).orElse(Wei.ZERO));
    final Hash codeHash =
        changes
            .code()
            .map(Hash::hash)
            .orElse(prior.map(BinaryTrieAccountValue::getCodeHash).orElse(Hash.EMPTY));
    final boolean empty = nonce == 0L && balance.isZero() && Hash.EMPTY.equals(codeHash);
    layer.addAccountChange(
        changes.address(),
        prior.orElse(null),
        empty ? null : new BinaryTrieAccountValue(nonce, balance, codeHash));

    if (changes.code().isPresent()) {
      final Bytes priorCode =
          prior
              .filter(a -> !Hash.EMPTY.equals(a.getCodeHash()))
              .map(
                  a ->
                      delegation(pbt, address32)
                          .orElseGet(() -> code.apply(changes.address(), a.getCodeHash())))
              .orElse(Bytes.EMPTY);
      final Bytes updatedCode = changes.code().get();
      layer.addCodeChange(
          changes.address(),
          priorCode.isEmpty() ? null : priorCode,
          updatedCode.isEmpty() ? null : updatedCode,
          header.getBlockHash());
    }
  }

  /** The account as its PBT header stem holds it, if it exists. */
  private static Optional<BinaryTrieAccountValue> account(
      final StoredPartitionedBinaryTrie pbt, final Bytes32 address32) {
    return pbt.get(TrieKeyDerivation.getTreeKeyForBasicData(address32))
        .map(
            leaf -> {
              final BasicDataEncoder.BasicData basic =
                  BasicDataEncoder.decodeBasicData(Bytes32.wrap(leaf));
              final Hash codeHash =
                  delegation(pbt, address32)
                      .map(Hash::hash)
                      .or(
                          () ->
                              pbt.get(TrieKeyDerivation.getTreeKeyForCodeHash(address32))
                                  .map(Bytes32::wrap)
                                  .map(Hash::wrap))
                      .orElse(Hash.EMPTY);
              return new BinaryTrieAccountValue(basic.nonce(), Wei.of(basic.balance()), codeHash);
            });
  }

  /** The EIP-7702 indicator {@code 0xef0100 || target} held in the delegation leaf, if any. */
  private static Optional<Bytes> delegation(
      final StoredPartitionedBinaryTrie pbt, final Bytes32 address32) {
    return pbt.get(TrieKeyDerivation.getTreeKeyForDelegation(address32))
        .map(
            leaf ->
                Bytes.concatenate(
                    CodeDelegationHelper.CODE_DELEGATION_PREFIX,
                    leaf.slice(DelegationEncoder.DESIGNATOR.size(), Address.SIZE)));
  }
}
