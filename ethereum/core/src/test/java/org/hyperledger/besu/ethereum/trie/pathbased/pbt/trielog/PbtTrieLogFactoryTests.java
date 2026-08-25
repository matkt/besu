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
package org.hyperledger.besu.ethereum.trie.pathbased.pbt.trielog;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.trie.common.BinaryTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.BonsaiTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/**
 * Validates the extended {@link BonsaiTrieLogFactory} wire format: the storage slot key preimage
 * must round-trip so the binary state root can be recomputed after a trie-log roll.
 */
class PbtTrieLogFactoryTests {

  private static final Address ACCOUNT_FIXTURE = Address.fromHexString("0xdeadbeef");
  private static final UInt256 SLOT_KEY = UInt256.valueOf(0x1234);
  private static final UInt256 SLOT_KEY_ZERO = UInt256.ZERO;

  private final BlockHeader headerFixture =
      new BlockHeaderTestFixture().parentHash(Hash.ZERO).coinbase(Address.ZERO).buildHeader();

  private final BonsaiTrieLogFactory extendedFactory = new BonsaiTrieLogFactory(Optional.of(0L));

  private TrieLogLayer newFixture(final StorageSlotKey slotKey) {
    return new TrieLogLayer()
        .setBlockHash(headerFixture.getBlockHash())
        .setWireVersion(BonsaiTrieLogFactory.WIRE_VERSION_EXTENDED)
        .addAccountChange(
            ACCOUNT_FIXTURE, null, new BinaryTrieAccountValue(0, Wei.fromEth(1), Hash.EMPTY))
        .addCodeChange(
            Address.ZERO, null, Bytes.fromHexString("0xfeeddeadbeef"), headerFixture.getBlockHash())
        .addStorageChange(Address.ZERO, slotKey, null, UInt256.ONE);
  }

  @Test
  void serializeDeserializeAreEqual_andSlotKeyPreimageRoundTrips() {
    final TrieLogLayer fixture = newFixture(new StorageSlotKey(SLOT_KEY));

    final byte[] rlp = extendedFactory.serialize(fixture);
    final TrieLogLayer deserialized = extendedFactory.deserialize(rlp);

    assertThat(deserialized).isEqualTo(fixture);
    final StorageSlotKey roundTripped =
        deserialized.getStorageChanges(Address.ZERO).keySet().iterator().next();
    assertThat(roundTripped.getSlotKey()).contains(SLOT_KEY);
  }

  @Test
  void zeroSlotKeyPreimageRoundTrips() {
    final TrieLogLayer fixture = newFixture(new StorageSlotKey(SLOT_KEY_ZERO));

    final byte[] rlp = extendedFactory.serialize(fixture);
    final TrieLogLayer deserialized = extendedFactory.deserialize(rlp);

    final StorageSlotKey roundTripped =
        deserialized.getStorageChanges(Address.ZERO).keySet().iterator().next();
    assertThat(roundTripped.getSlotKey()).contains(SLOT_KEY_ZERO);
  }

  @Test
  void extendedFactoryReadsLegacyLogLeavesSlotKeyEmpty() {
    final TrieLogLayer fixture = newFixture(new StorageSlotKey(SLOT_KEY));
    final TrieLogLayer legacyFixture =
        new TrieLogLayer()
            .setBlockHash(fixture.getBlockHash())
            .addAccountChange(
                ACCOUNT_FIXTURE, null, new BinaryTrieAccountValue(0, Wei.fromEth(1), Hash.EMPTY))
            .addCodeChange(
                Address.ZERO,
                null,
                Bytes.fromHexString("0xfeeddeadbeef"),
                headerFixture.getBlockHash())
            .addStorageChange(Address.ZERO, new StorageSlotKey(SLOT_KEY), null, UInt256.ONE);
    final byte[] legacyRlp = new BonsaiTrieLogFactory().serialize(legacyFixture);

    final TrieLogLayer deserialized = extendedFactory.deserialize(legacyRlp);
    final StorageSlotKey roundTripped =
        deserialized.getStorageChanges(Address.ZERO).keySet().iterator().next();
    assertThat(roundTripped.getSlotKey()).isEmpty();
  }

  @Test
  void legacyFactoryReadsExtendedLogWithSlotKeyPreimage() {
    final TrieLogLayer fixture = newFixture(new StorageSlotKey(SLOT_KEY));
    final byte[] extendedRlp = extendedFactory.serialize(fixture);

    final TrieLogLayer deserialized = new BonsaiTrieLogFactory().deserialize(extendedRlp);
    final StorageSlotKey roundTripped =
        deserialized.getStorageChanges(Address.ZERO).keySet().iterator().next();
    assertThat(roundTripped.getSlotKey()).contains(SLOT_KEY);
  }
}
