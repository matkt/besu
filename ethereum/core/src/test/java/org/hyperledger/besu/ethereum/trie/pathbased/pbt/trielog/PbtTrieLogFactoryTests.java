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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.PbtTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.PbtTrieLogFactory.BinaryAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/**
 * Validates the {@link PbtTrieLogFactory} wire format: the storage slot key preimage must
 * round-trip so the binary state root can be recomputed after a trie-log roll.
 */
class PbtTrieLogFactoryTests {

  private static final Address ACCOUNT_FIXTURE = Address.fromHexString("0xdeadbeef");
  private static final UInt256 SLOT_KEY = UInt256.valueOf(0x1234);
  private static final UInt256 SLOT_KEY_ZERO = UInt256.ZERO;

  private final BlockHeader headerFixture =
      new BlockHeaderTestFixture().parentHash(Hash.ZERO).coinbase(Address.ZERO).buildHeader();

  private TrieLogLayer newFixture(final StorageSlotKey slotKey) {
    return new TrieLogLayer()
        .setBlockHash(headerFixture.getBlockHash())
        .addAccountChange(
            ACCOUNT_FIXTURE, null, new BinaryAccountValue(0, Wei.fromEth(1), Hash.EMPTY))
        .addCodeChange(
            Address.ZERO, null, Bytes.fromHexString("0xfeeddeadbeef"), headerFixture.getBlockHash())
        .addStorageChange(Address.ZERO, slotKey, null, UInt256.ONE);
  }

  @Test
  void serializeDeserializeAreEqual_andSlotKeyPreimageRoundTrips() {
    final PbtTrieLogFactory factory = new PbtTrieLogFactory();
    final TrieLogLayer fixture = newFixture(new StorageSlotKey(SLOT_KEY));

    final byte[] rlp = factory.serialize(fixture);
    final TrieLogLayer deserialized = factory.deserialize(rlp);

    assertThat(deserialized).isEqualTo(fixture);
    final StorageSlotKey roundTripped =
        deserialized.getStorageChanges(Address.ZERO).keySet().iterator().next();
    assertThat(roundTripped.getSlotKey()).contains(SLOT_KEY);
  }

  @Test
  void zeroSlotKeyPreimageRoundTrips() {
    // UInt256.ZERO must survive distinctly from an absent preimage (slot 0 is a valid key).
    final PbtTrieLogFactory factory = new PbtTrieLogFactory();
    final TrieLogLayer fixture = newFixture(new StorageSlotKey(SLOT_KEY_ZERO));

    final byte[] rlp = factory.serialize(fixture);
    final TrieLogLayer deserialized = factory.deserialize(rlp);

    final StorageSlotKey roundTripped =
        deserialized.getStorageChanges(Address.ZERO).keySet().iterator().next();
    assertThat(roundTripped.getSlotKey()).contains(SLOT_KEY_ZERO);
  }

  @Test
  void pbtFactoryReadsLegacyMptLogLeavesSlotKeyEmpty() {
    // A log serialized by the legacy MPT factory (no slotKey) must still deserialize under the
    // PBT factory, with an empty slotKey (graceful migration).
    final TrieLogLayer fixture = newFixture(new StorageSlotKey(SLOT_KEY));
    final byte[] mptRlp =
        new org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.PmtTrieLogFactory()
            .serialize(fixture);

    final TrieLogLayer deserialized = new PbtTrieLogFactory().deserialize(mptRlp);
    final StorageSlotKey roundTripped =
        deserialized.getStorageChanges(Address.ZERO).keySet().iterator().next();
    assertThat(roundTripped.getSlotKey()).isEmpty();
  }

  @Test
  void mptFactoryCannotReadPbtLog() {
    // The reverse cross-read is intentionally unsupported: the MPT reader's leaveList()
    // rejects the trailing slotKey element. This never occurs in normal operation since a
    // chain is either all-MPT or all-binary.
    final TrieLogLayer fixture = newFixture(new StorageSlotKey(SLOT_KEY));
    final byte[] pbtRlp = new PbtTrieLogFactory().serialize(fixture);

    assertThatThrownBy(
            () ->
                new org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.PmtTrieLogFactory()
                    .deserialize(pbtRlp))
        .isInstanceOf(Exception.class);
  }
}
