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
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Trie-log factory for the partitioned binary trie ({@code DataStorageFormat#BINARY}).
 *
 * <p>It mirrors {@link PmtTrieLogFactory} but additionally serializes the storage slot <b>key
 * preimage</b> for each storage change, so that a binary state-root can be recomputed after a
 * trie-log {@code rollForward}/{@code rollBack}. The binary committer ({@code
 * BinaryStateRootCommitter}) derives the binary-tree key from {@link StorageSlotKey#getSlotKey()};
 * the MPT (mainnet) factory serializes only the slot <em>hash</em>, which leaves {@code
 * getSlotKey()} empty and makes post-roll binary root computation impossible.
 *
 * <h2>Wire format</h2>
 *
 * <p>Accounts are encoded in the <b>binary-native 3-field</b> RLP {@code [nonce, balance,
 * codeHash]} — there is no per-account storage root in the partitioned binary trie, so no
 * placeholder storage-root field is written. (A legacy MPT {@link PmtStateTrieAccountValue} that
 * happens to be replayed through this factory is still written via its own 4-field {@code [nonce,
 * balance, storageRoot, codeHash]} {@code writeTo}; the PBT factory's primary path is the 3-field
 * binary form.)
 *
 * <p>The per-storage-change entry is the MPT 4-element list {@code [slotHash, prior, updated,
 * isCleared]} with an optional 5th element {@code slotKey} (the 32-byte UInt256 preimage, written
 * only when present):
 *
 * <pre>{@code
 * [slotHash, prior, updated, isCleared, slotKey?]
 * }</pre>
 *
 * <h2>Cross-format reading</h2>
 *
 * <p>Accounts: this factory reads both the binary 3-field list and the legacy MPT 4-field list
 * {@code [nonce, balance, storageRoot, codeHash]}; for the 4-field form the storage-root field is
 * consumed and ignored (binary accounts carry no storage root), mirroring {@code
 * BinaryStorageRootStrategy.readStorageRoot}. Both inputs decode to a {@link BinaryAccountValue}
 * that re-serializes as 3-field, so the binary form round-trips.
 *
 * <p>Storage: this factory deserializes both PBT logs (5 elements, slotKey present) and legacy MPT
 * logs (4 elements, no slotKey); in the legacy case {@code getSlotKey()} is left empty. The reverse
 * (MPT factory reading a PBT log) is <b>not</b> supported — the MPT reader's {@code leaveList()}
 * would reject the trailing 5th element — but that path never occurs in normal operation since a
 * chain is either all-MPT or all-binary. A BINARY-written log always round-trips through this
 * factory.
 */
public class PbtTrieLogFactory extends AbstractTrieLogFactory {

  @Override
  public byte[] serialize(final TrieLog layer) {
    final BytesValueRLPOutput rlpLog = new BytesValueRLPOutput();
    writeTo(layer, rlpLog);
    return rlpLog.encoded().toArrayUnsafe();
  }

  /**
   * Serializes the trie log, including the storage slot key preimage when present.
   *
   * @param layer the trie log.
   * @param output the RLP output.
   */
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
        writeRlp(accountChange, output, PbtTrieLogFactory::writeAccountValue);
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
          output.writeBytes(storageChangeEntry.getKey().getSlotHash().getBytes());
          writeInnerRlp(storageChangeEntry.getValue(), output, RLPOutput::writeUInt256Scalar);
          // PBT extension: carry the slot key preimage so the binary root can be recomputed.
          // Written as a fixed-width 32-byte value (not a scalar) so UInt256.ZERO round-trips
          // distinctly from an absent preimage (null).
          final Optional<UInt256> slotKey = storageChangeEntry.getKey().getSlotKey();
          if (slotKey.isPresent()) {
            output.writeBytes(slotKey.get());
          } else {
            output.writeNull();
          }
          output.endList();
        }
        output.endList();
      }

      output.endList(); // this change
    }
    output.endList(); // container
  }

  @Override
  public TrieLogLayer deserialize(final byte[] bytes) {
    return readFrom(new BytesValueRLPInput(Bytes.wrap(bytes), false));
  }

  /**
   * Deserializes a trie log. Reads both PBT logs (5-element entries with slotKey) and legacy MPT
   * logs (4-element entries without slotKey); in the legacy case {@code getSlotKey()} is left
   * empty.
   *
   * @param input the RLP input.
   * @return the deserialized trie log layer.
   */
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
        final AccountValue oldValue = nullOrValue(input, PbtTrieLogFactory::readAccountValue);
        final AccountValue newValue = nullOrValue(input, PbtTrieLogFactory::readAccountValue);
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
          final UInt256 oldValue = nullOrValue(input, RLPInput::readUInt256Scalar);
          final UInt256 newValue = nullOrValue(input, RLPInput::readUInt256Scalar);
          final boolean isCleared = getOptionalIsCleared(input);
          // PBT extension: optional 5th element is the slot key preimage (fixed-width 32 bytes).
          // Legacy MPT logs have exactly 4 elements and leave slotKey empty (binary root cannot
          // be recomputed). A null 5th element also means absent.
          final Optional<UInt256> slotKey =
              input.isEndOfCurrentList()
                  ? Optional.empty()
                  : Optional.ofNullable(
                      nullOrValue(input, in -> UInt256.fromBytes(in.readBytes32())));
          final StorageSlotKey storageSlotKey = new StorageSlotKey(slotHash, slotKey);
          storageChanges.put(storageSlotKey, new BonsaiValue<>(oldValue, newValue, isCleared));
          input.leaveList();
        }
        input.leaveList();
        newLayer.getStorageChanges().put(address, storageChanges);
      }

      // lenient leave list for forward compatible additions.
      input.leaveListLenient();
    }
    input.leaveListLenient();
    newLayer.freeze();

    return newLayer;
  }

  /**
   * Serializes an account value into the trie log. Delegates to the value's own {@code writeTo},
   * which encodes the format-appropriate form: a {@link
   * org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount} encodes via its
   * {@code StorageRootStrategy} (binary 3-field {@code [nonce, balance, codeHash]}, no placeholder
   * storage root); a {@link BinaryAccountValue} writes the same 3-field form; a legacy {@link
   * PmtStateTrieAccountValue} writes the MPT 4-field form. The PBT factory's primary path is the
   * 3-field binary form.
   */
  private static void writeAccountValue(final RLPOutput out, final AccountValue accountValue) {
    accountValue.writeTo(out);
  }

  /**
   * Deserializes an account value, binary-native. Detects the list size: a 3-field list {@code
   * [nonce, balance, codeHash]} is a native binary account; a 4-field list {@code [nonce, balance,
   * storageRoot, codeHash]} is a legacy MPT account whose storage-root field is consumed and
   * ignored (binary accounts carry no storage root). Both decode to a {@link BinaryAccountValue}
   * that re-serializes as 3-field, so the binary form round-trips.
   */
  private static AccountValue readAccountValue(final RLPInput input) {
    final int listSize = input.enterList();
    final long nonce = input.readLongScalar();
    final Wei balance = Wei.of(input.readUInt256Scalar());
    if (listSize == 4) {
      // Legacy MPT-encoded account: consume and discard the storageRoot field.
      input.skipNext();
    }
    final Hash codeHash = Hash.wrap(input.readBytes32());
    input.leaveList();
    return new BinaryAccountValue(nonce, balance, codeHash);
  }

  /**
   * Binary-native, immutable {@link AccountValue} used for deserialized trie-log accounts. The
   * partitioned binary trie has no per-account storage root, so {@link #writeTo(RLPOutput)} emits
   * the 3-field list {@code [nonce, balance, codeHash]} with no placeholder storage root, and the
   * 3-field form round-trips through this factory.
   *
   * <p>This class does not implement {@link org.hyperledger.besu.datatypes.MptAccountValue}: there
   * is no {@code getStorageRoot()} accessor. Callers that project onto an MPT encoding (e.g. the
   * legacy factory serializing a binary value during a cross-format replay) must treat the storage
   * root as {@link Hash#EMPTY_TRIE_HASH}; the binary state-root committer never consults it.
   */
  public static final class BinaryAccountValue implements AccountValue {
    private final long nonce;
    private final Wei balance;
    private final Hash codeHash;

    public BinaryAccountValue(final long nonce, final Wei balance, final Hash codeHash) {
      this.nonce = nonce;
      this.balance = balance;
      this.codeHash = codeHash;
    }

    @Override
    public long getNonce() {
      return nonce;
    }

    @Override
    public Wei getBalance() {
      return balance;
    }

    @Override
    public Hash getCodeHash() {
      return codeHash;
    }

    @Override
    public void writeTo(final RLPOutput out) {
      out.startList();
      out.writeLongScalar(nonce);
      out.writeUInt256Scalar(balance);
      out.writeBytes(codeHash.getBytes());
      out.endList();
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (!(o instanceof BinaryAccountValue that)) return false;
      return nonce == that.nonce
          && Objects.equals(balance, that.balance)
          && Objects.equals(codeHash, that.codeHash);
    }

    @Override
    public int hashCode() {
      return Objects.hash(nonce, balance, codeHash);
    }
  }
}
