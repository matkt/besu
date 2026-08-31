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
import org.hyperledger.besu.ethereum.trie.common.BinaryTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogAccumulator;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Unified Bonsai trie-log factory for both MPT and partitioned-binary-trie branches.
 *
 * <p>Wire container layout:
 *
 * <ul>
 *   <li>Version 0 (legacy, implicit): {@code [blockHash, changes...]} with storage entries {@code
 *       [slotHash, prior, updated, isCleared?]}
 *   <li>Version 1 (extended): {@code [1, blockHash, introducedCodeHashes|null, changes...]} with
 *       storage entries {@code [slotKey, prior, updated, isCleared?]}
 * </ul>
 *
 * <p>Version is detected on decode: if the first element is not a 32-byte block hash, it is read as
 * an explicit version scalar; absent version means 0.
 */
public class BonsaiTrieLogFactory implements TrieLogFactory {

  /** Legacy wire format: no version prefix, storage entries carry slot hash only. */
  public static final int WIRE_VERSION_LEGACY = 0;

  /** Extended wire format: slot-key preimages and introduced-code hash list. */
  public static final int WIRE_VERSION_EXTENDED = 1;

  private static final int BINARY_ACCOUNT_FIELD_COUNT = 3;
  private static final int PATRICIA_ACCOUNT_FIELD_COUNT = 4;

  private final Optional<Long> binaryTrieMilestone;

  public BonsaiTrieLogFactory(final Optional<Long> binaryTrieMilestone) {
    this.binaryTrieMilestone = binaryTrieMilestone;
  }

  public BonsaiTrieLogFactory() {
    this(Optional.empty());
  }

  @Override
  public TrieLogLayer create(final TrieLogAccumulator accumulator, final BlockHeader blockHeader) {
    final TrieLogLayer layer = new TrieLogLayer();
    layer.setBlockHash(blockHeader.getBlockHash());
    layer.setBlockNumber(blockHeader.getNumber());
    for (final var updatedAccount : accumulator.getAccountsToUpdate().entrySet()) {
      final var bonsaiValue = updatedAccount.getValue();
      final var oldAccountValue = bonsaiValue.getPrior();
      final var newAccountValue = bonsaiValue.getUpdated();
      if (oldAccountValue == null && newAccountValue == null) {
        continue;
      }
      layer.addAccountChange(updatedAccount.getKey(), oldAccountValue, newAccountValue);
    }

    for (final var updatedCode : accumulator.getCodeToUpdate().entrySet()) {
      layer.addCodeChange(
          updatedCode.getKey(),
          updatedCode.getValue().getPrior(),
          updatedCode.getValue().getUpdated(),
          blockHeader.getBlockHash());
    }

    for (final var updatesStorage : accumulator.getStorageToUpdate().entrySet()) {
      final Address address = updatesStorage.getKey();
      for (final var slotUpdate : updatesStorage.getValue().entrySet()) {
        final var val = slotUpdate.getValue();
        if (val.getPrior() == null && val.getUpdated() == null) {
          continue;
        }
        layer.addStorageChange(address, slotUpdate.getKey(), val.getPrior(), val.getUpdated());
      }
    }

    if (binaryTrieMilestone.isPresent()) {
      layer.setWireVersion(WIRE_VERSION_EXTENDED);
      for (final Hash codeHash : accumulator.getIntroducedCodeHashes()) {
        layer.addIntroducedCodeHash(codeHash);
      }
    }
    return layer;
  }

  @Override
  public byte[] serialize(final TrieLog layer) {
    final BytesValueRLPOutput rlpLog = new BytesValueRLPOutput();
    writeTo(layer, rlpLog);
    return rlpLog.encoded().toArrayUnsafe();
  }

  public static void writeTo(final TrieLog layer, final RLPOutput output) {
    layer.freeze();
    final int wireVersion = wireVersion(layer);
    final Set<Address> addresses = collectChangedAddresses(layer);

    writeContainerHeader(output, layer, wireVersion);
    for (final Address address : addresses) {
      writeAddressChange(output, layer, address, wireVersion);
    }
    output.endList();
  }

  @Override
  public TrieLogLayer deserialize(final byte[] bytes) {
    return readFrom(new BytesValueRLPInput(Bytes.wrap(bytes), false));
  }

  public static TrieLogLayer readFrom(final RLPInput input) {
    final TrieLogLayer newLayer = new TrieLogLayer();

    input.enterList();
    final int wireVersion = readWireVersion(input);
    newLayer.setBlockHash(Hash.wrap(input.readBytes32()));
    newLayer.setWireVersion(wireVersion);

    if (wireVersion >= WIRE_VERSION_EXTENDED) {
      readIntroducedCodeHashesSlot(input, newLayer);
    }
    while (!input.isEndOfCurrentList()) {
      readAddressChange(input, newLayer, wireVersion);
    }

    input.leaveListLenient();
    newLayer.freeze();
    return newLayer;
  }

  private static int wireVersion(final TrieLog layer) {
    return layer instanceof TrieLogLayer trieLogLayer
        ? trieLogLayer.getWireVersion()
        : WIRE_VERSION_LEGACY;
  }

  private static Set<Address> collectChangedAddresses(final TrieLog layer) {
    final Set<Address> addresses = new TreeSet<>();
    addresses.addAll(layer.getAccountChanges().keySet());
    addresses.addAll(layer.getCodeChanges().keySet());
    addresses.addAll(layer.getStorageChanges().keySet());
    return addresses;
  }

  private static void writeContainerHeader(
      final RLPOutput output, final TrieLog layer, final int wireVersion) {
    output.startList();
    if (wireVersion >= WIRE_VERSION_EXTENDED) {
      output.writeInt(wireVersion);
    }
    output.writeBytes(layer.getBlockHash().getBytes());
    if (wireVersion >= WIRE_VERSION_EXTENDED) {
      writeIntroducedCodeHashesSlot(output, layer);
    }
  }

  private static void writeIntroducedCodeHashesSlot(final RLPOutput output, final TrieLog layer) {
    if (layer.getIntroducedCodeHashes().isEmpty()) {
      output.writeNull();
      return;
    }
    output.startList();
    for (final Hash codeHash : layer.getIntroducedCodeHashes()) {
      output.writeBytes(codeHash.getBytes());
    }
    output.endList();
  }

  private static void writeAddressChange(
      final RLPOutput output, final TrieLog layer, final Address address, final int wireVersion) {
    output.startList();
    output.writeBytes(address.getBytes());
    writeAccountChange(output, layer.getAccountChanges().get(address));
    writeCodeChange(output, layer.getCodeChanges().get(address));
    writeStorageChanges(output, layer.getStorageChanges().get(address), wireVersion);
    output.endList();
  }

  private static void writeAccountChange(
      final RLPOutput output, final TrieLog.LogTuple<AccountValue> accountChange) {
    if (accountChange == null || accountChange.isUnchanged()) {
      output.writeNull();
      return;
    }
    writeRlp(accountChange, output, (out, accountValue) -> accountValue.writeTo(out));
  }

  private static void writeCodeChange(
      final RLPOutput output, final TrieLog.LogTuple<Bytes> codeChange) {
    if (codeChange == null || codeChange.isUnchanged()) {
      output.writeNull();
      return;
    }
    writeRlp(codeChange, output, RLPOutput::writeBytes);
  }

  private static void writeStorageChanges(
      final RLPOutput output,
      final Map<StorageSlotKey, TrieLog.LogTuple<UInt256>> storageChanges,
      final int wireVersion) {
    if (storageChanges == null) {
      output.writeNull();
      return;
    }
    output.startList();
    for (final Map.Entry<StorageSlotKey, TrieLog.LogTuple<UInt256>> storageChangeEntry :
        storageChanges.entrySet()) {
      output.startList();
      writeStorageSlotKey(output, storageChangeEntry.getKey(), wireVersion);
      writeInnerRlp(storageChangeEntry.getValue(), output, RLPOutput::writeUInt256Scalar);
      output.endList();
    }
    output.endList();
  }

  private static void writeStorageSlotKey(
      final RLPOutput output, final StorageSlotKey slotKey, final int wireVersion) {
    if (wireVersion >= WIRE_VERSION_EXTENDED) {
      final UInt256 slotKeyPreimage =
          slotKey
              .getSlotKey()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Extended trie-log storage entry must include a slot key preimage"));
      output.writeBytes(slotKeyPreimage);
      return;
    }
    output.writeBytes(slotKey.getSlotHash().getBytes());
  }

  private static void readIntroducedCodeHashesSlot(
      final RLPInput input, final TrieLogLayer newLayer) {
    if (input.nextIsNull()) {
      input.skipNext();
      return;
    }
    input.enterList();
    while (!input.isEndOfCurrentList()) {
      newLayer.addIntroducedCodeHash(Hash.wrap(input.readBytes32()));
    }
    input.leaveList();
  }

  private static void readAddressChange(
      final RLPInput input, final TrieLogLayer newLayer, final int wireVersion) {
    input.enterList();
    final Address address = Address.readFrom(input);
    readAccountChange(input, newLayer, address);
    readCodeChange(input, newLayer, address);
    readStorageChanges(input, newLayer, address, wireVersion);
    input.leaveListLenient();
  }

  private static void readAccountChange(
      final RLPInput input, final TrieLogLayer newLayer, final Address address) {
    if (input.nextIsNull()) {
      input.skipNext();
      return;
    }
    input.enterList();
    final AccountValue oldValue = nullOrValue(input, BonsaiTrieLogFactory::readAccountValue);
    final AccountValue newValue = nullOrValue(input, BonsaiTrieLogFactory::readAccountValue);
    final boolean isCleared = getOptionalIsCleared(input);
    input.leaveList();
    newLayer.getAccountChanges().put(address, new BonsaiValue<>(oldValue, newValue, isCleared));
  }

  private static void readCodeChange(
      final RLPInput input, final TrieLogLayer newLayer, final Address address) {
    if (input.nextIsNull()) {
      input.skipNext();
      return;
    }
    input.enterList();
    final Bytes oldCode = nullOrValue(input, RLPInput::readBytes);
    final Bytes newCode = nullOrValue(input, RLPInput::readBytes);
    final boolean isCleared = getOptionalIsCleared(input);
    input.leaveList();
    newLayer.getCodeChanges().put(address, new BonsaiValue<>(oldCode, newCode, isCleared));
  }

  private static void readStorageChanges(
      final RLPInput input,
      final TrieLogLayer newLayer,
      final Address address,
      final int wireVersion) {
    if (input.nextIsNull()) {
      input.skipNext();
      return;
    }
    final Map<StorageSlotKey, BonsaiValue<UInt256>> storageChanges = new TreeMap<>();
    input.enterList();
    while (!input.isEndOfCurrentList()) {
      input.enterList();
      final StorageSlotKey storageSlotKey = readStorageSlotKey(input, wireVersion);
      final UInt256 oldValue = nullOrValue(input, RLPInput::readUInt256Scalar);
      final UInt256 newValue = nullOrValue(input, RLPInput::readUInt256Scalar);
      final boolean isCleared = getOptionalIsCleared(input);
      storageChanges.put(storageSlotKey, new BonsaiValue<>(oldValue, newValue, isCleared));
      input.leaveList();
    }
    input.leaveList();
    newLayer.getStorageChanges().put(address, storageChanges);
  }

  private static int readWireVersion(final RLPInput input) {
    if (input.nextSize() == Bytes32.SIZE) {
      return WIRE_VERSION_LEGACY;
    }
    return input.readInt();
  }

  private static StorageSlotKey readStorageSlotKey(final RLPInput input, final int wireVersion) {
    if (wireVersion >= WIRE_VERSION_EXTENDED) {
      final UInt256 slotKey = nullOrValue(input, in -> UInt256.fromBytes(in.readBytes32()));
      if (slotKey == null) {
        throw new IllegalArgumentException(
            "Extended trie-log storage entry must include a slot key preimage");
      }
      return new StorageSlotKey(slotKey);
    }
    return new StorageSlotKey(Hash.wrap(input.readBytes32()), Optional.empty());
  }

  /**
   * Account RLP list length selects the codec: 3 fields = binary trie, 4 fields = Patricia trie.
   */
  private static AccountValue readAccountValue(final RLPInput input) {
    final int fieldCount = input.enterList();
    if (fieldCount == BINARY_ACCOUNT_FIELD_COUNT) {
      return readBinaryAccountValue(input);
    }
    if (fieldCount == PATRICIA_ACCOUNT_FIELD_COUNT) {
      return readPatriciaAccountValue(input);
    }
    input.leaveList();
    throw new IllegalArgumentException(
        "Unexpected trie-log account field count: " + fieldCount + " (expected 3 or 4)");
  }

  private static BinaryTrieAccountValue readBinaryAccountValue(final RLPInput input) {
    final long nonce = input.readLongScalar();
    final Wei balance = Wei.of(input.readUInt256Scalar());
    final Hash codeHash = Hash.wrap(input.readBytes32());
    input.leaveList();
    return new BinaryTrieAccountValue(nonce, balance, codeHash);
  }

  private static PatriciaTrieAccountValue readPatriciaAccountValue(final RLPInput input) {
    final long nonce = input.readLongScalar();
    final Wei balance = Wei.of(input.readUInt256Scalar());
    final Hash storageRoot = Hash.wrap(input.readBytes32());
    final Hash codeHash = Hash.wrap(input.readBytes32());
    input.leaveList();
    return new PatriciaTrieAccountValue(nonce, balance, storageRoot, codeHash);
  }

  private static <T> T nullOrValue(final RLPInput input, final Function<RLPInput, T> reader) {
    if (input.nextIsNull()) {
      input.skipNext();
      return null;
    }
    return reader.apply(input);
  }

  private static boolean getOptionalIsCleared(final RLPInput input) {
    return Optional.of(input.isEndOfCurrentList())
        .filter(isEnd -> !isEnd)
        .map(__ -> nullOrValue(input, RLPInput::readInt))
        .filter(i -> i == 1)
        .isPresent();
  }

  private static <T> void writeRlp(
      final TrieLog.LogTuple<T> value,
      final RLPOutput output,
      final BiConsumer<RLPOutput, T> writer) {
    output.startList();
    writeInnerRlp(value, output, writer);
    output.endList();
  }

  private static <T> void writeInnerRlp(
      final TrieLog.LogTuple<T> value,
      final RLPOutput output,
      final BiConsumer<RLPOutput, T> writer) {
    if (value.getPrior() == null) {
      output.writeNull();
    } else {
      writer.accept(output, value.getPrior());
    }
    if (value.getUpdated() == null) {
      output.writeNull();
    } else {
      writer.accept(output, value.getUpdated());
    }
    if (!value.isLastStepCleared()) {
      output.writeNull();
    } else {
      output.writeInt(1);
    }
  }
}
