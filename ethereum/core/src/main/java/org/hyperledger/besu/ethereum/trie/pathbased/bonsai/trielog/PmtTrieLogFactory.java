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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.MptAccountValue;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Trie-log factory for the Merkle-Patricia trie (MPT, mainnet) on-disk format.
 *
 * <p>Accounts are serialized in the MPT 4-field RLP {@code [nonce, balance, storageRoot, codeHash]}
 * and storage changes carry only the slot <em>hash</em> (no key preimage). The format-agnostic
 * accumulator→{@link TrieLogLayer} builder and the RLP helpers are inherited from {@link
 * AbstractTrieLogFactory}.
 */
public class PmtTrieLogFactory extends AbstractTrieLogFactory {

  @Override
  public byte[] serialize(final TrieLog layer) {
    final BytesValueRLPOutput rlpLog = new BytesValueRLPOutput();
    writeTo(layer, rlpLog);
    return rlpLog.encoded().toArrayUnsafe();
  }

  public static void writeTo(final TrieLog layer, final RLPOutput output) {
    layer.freeze();

    final Set<Address> addresses = new TreeSet<>();
    addresses.addAll(layer.getAccountChanges().keySet());
    addresses.addAll(layer.getCodeChanges().keySet());
    addresses.addAll(layer.getStorageChanges().keySet());

    output.startList(); // container
    output.writeBytes(layer.getBlockHash().getBytes());

    for (final Address address : addresses) {
      output.startList(); // this change
      output.writeBytes(address.getBytes());

      final TrieLog.LogTuple<AccountValue> accountChange = layer.getAccountChanges().get(address);
      if (accountChange == null || accountChange.isUnchanged()) {
        output.writeNull();
      } else {
        writeRlp(accountChange, output, PmtTrieLogFactory::writeAccountValue);
      }

      final TrieLog.LogTuple<Bytes> codeChange = layer.getCodeChanges().get(address);
      if (codeChange == null || codeChange.isUnchanged()) {
        output.writeNull();
      } else {
        writeRlp(codeChange, output, RLPOutput::writeBytes);
      }

      final Map<StorageSlotKey, TrieLog.LogTuple<UInt256>> storageChanges =
          layer.getStorageChanges().get(address);
      if (storageChanges == null) {
        output.writeNull();
      } else {
        output.startList();
        for (final Map.Entry<StorageSlotKey, TrieLog.LogTuple<UInt256>> storageChangeEntry :
            storageChanges.entrySet()) {
          output.startList();
          // do not write slotKey, it is not used in mainnet bonsai trielogs
          output.writeBytes(storageChangeEntry.getKey().getSlotHash().getBytes());
          writeInnerRlp(storageChangeEntry.getValue(), output, RLPOutput::writeUInt256Scalar);
          output.endList();
        }
        output.endList();
      }

      output.endList(); // this change
    }
    output.endList(); // container
  }

  /**
   * Serializes an account value into the trie log using the MPT 4-field RLP {@code [nonce, balance,
   * storageRoot, codeHash]}.
   *
   * <p>The trie log's on-disk account-value format is MPT RLP (the deserialize side reads {@link
   * PmtStateTrieAccountValue}). To keep that format stable while {@link
   * BonsaiAccount#getStorageRoot()} throws for binary accounts (no storage root), this helper
   * projects the {@link AccountValue} onto a {@link PmtStateTrieAccountValue} before writing. For a
   * binary {@link BonsaiAccount} the storage root is taken as {@link Hash#EMPTY_TRIE_HASH} (the
   * field is meaningless for binary accounts and ignored on rollback); {@link
   * BonsaiAccount#hasStorageRoot()} gates the throwing accessor. A non-{@code BonsaiAccount} {@link
   * MptAccountValue} (e.g. a deserialized {@link PmtStateTrieAccountValue}) contributes its own
   * root; any other {@link AccountValue} (e.g. a binary {@code BinaryAccountValue}) contributes
   * {@link Hash#EMPTY_TRIE_HASH}.
   */
  private static void writeAccountValue(final RLPOutput out, final AccountValue accountValue) {
    final Hash storageRoot;
    if (accountValue instanceof BonsaiAccount bonsaiAccount) {
      storageRoot =
          bonsaiAccount.hasStorageRoot() ? bonsaiAccount.getStorageRoot() : Hash.EMPTY_TRIE_HASH;
    } else if (accountValue instanceof MptAccountValue mpt) {
      storageRoot = mpt.getStorageRoot();
    } else {
      storageRoot = Hash.EMPTY_TRIE_HASH;
    }
    new PmtStateTrieAccountValue(
            accountValue.getNonce(),
            accountValue.getBalance(),
            storageRoot,
            accountValue.getCodeHash())
        .writeTo(out);
  }

  @Override
  public TrieLogLayer deserialize(final byte[] bytes) {
    return readFrom(new BytesValueRLPInput(Bytes.wrap(bytes), false));
  }

  public static TrieLogLayer readFrom(final RLPInput input) {
    final TrieLogLayer newLayer = new TrieLogLayer();

    input.enterList();
    newLayer.setBlockHash(Hash.wrap(input.readBytes32()));

    while (!input.isEndOfCurrentList()) {
      input.enterList();
      final Address address = Address.readFrom(input);

      if (input.nextIsNull()) {
        input.skipNext();
      } else {
        input.enterList();
        final PmtStateTrieAccountValue oldValue =
            nullOrValue(input, PmtStateTrieAccountValue::readFrom);
        final PmtStateTrieAccountValue newValue =
            nullOrValue(input, PmtStateTrieAccountValue::readFrom);
        final boolean isCleared = getOptionalIsCleared(input);
        input.leaveList();
        newLayer.getAccountChanges().put(address, new BonsaiValue<>(oldValue, newValue, isCleared));
      }

      if (input.nextIsNull()) {
        input.skipNext();
      } else {
        input.enterList();
        final Bytes oldCode = nullOrValue(input, RLPInput::readBytes);
        final Bytes newCode = nullOrValue(input, RLPInput::readBytes);
        final boolean isCleared = getOptionalIsCleared(input);
        input.leaveList();
        newLayer.getCodeChanges().put(address, new BonsaiValue<>(oldCode, newCode, isCleared));
      }

      if (input.nextIsNull()) {
        input.skipNext();
      } else {
        final Map<StorageSlotKey, BonsaiValue<UInt256>> storageChanges = new TreeMap<>();
        input.enterList();
        while (!input.isEndOfCurrentList()) {
          input.enterList();
          final Hash slotHash = Hash.wrap(input.readBytes32());
          final StorageSlotKey storageSlotKey = new StorageSlotKey(slotHash, Optional.empty());
          final UInt256 oldValue = nullOrValue(input, RLPInput::readUInt256Scalar);
          final UInt256 newValue = nullOrValue(input, RLPInput::readUInt256Scalar);
          final boolean isCleared = getOptionalIsCleared(input);
          storageChanges.put(storageSlotKey, new BonsaiValue<>(oldValue, newValue, isCleared));
          input.leaveList();
        }
        input.leaveList();
        newLayer.getStorageChanges().put(address, storageChanges);
      }

      // TODO add trie nodes

      // lenient leave list for forward compatible additions.
      input.leaveListLenient();
    }
    input.leaveListLenient();
    newLayer.freeze();

    return newLayer;
  }
}
