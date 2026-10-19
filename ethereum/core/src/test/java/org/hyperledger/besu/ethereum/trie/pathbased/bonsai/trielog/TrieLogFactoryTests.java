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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.common.BinaryTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogFactory;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TrieLogFactoryTests {

  // main's encoding of the trie log of legacyFormatIsMainsFormat
  private static final String LEGACY_RLP =
      "0xf8d4a00000000000000000000000000000000000000000000000000000000000000000f84694000000000000000000000000000000000000000080c98086feeddeadbeef80e5e4a0290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563800180f8699400000000000000000000000000000000deadbeeff85080f84c80880de0b6b3a7640000a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470808080";

  final BlockchainSetupUtil setup = BlockchainSetupUtil.forTesting(DataStorageFormat.BONSAI);

  final Address accountFixture = Address.fromHexString("0xdeadbeef");

  final BlockHeader headerFixture =
      new BlockHeaderTestFixture()
          .parentHash(setup.getGenesisState().getBlock().getHash())
          .coinbase(Address.ZERO)
          .buildHeader();

  final TrieLogLayer trieLogFixture =
      new TrieLogLayer()
          .setBlockHash(headerFixture.getBlockHash())
          .addAccountChange(
              accountFixture,
              null,
              new PatriciaTrieAccountValue(0, Wei.fromEth(1), Hash.EMPTY, Hash.EMPTY))
          .addCodeChange(
              Address.ZERO,
              null,
              Bytes.fromHexString("0xfeeddeadbeef"),
              headerFixture.getBlockHash())
          .addStorageChange(Address.ZERO, new StorageSlotKey(UInt256.ZERO), null, UInt256.ONE);

  @Test
  public void testSerializeDeserializeAreEqual() {

    TrieLogFactory factory = new BonsaiTrieLogFactory();
    byte[] rlp = factory.serialize(trieLogFixture);

    TrieLog layer = factory.deserialize(rlp);
    assertThat(layer).isEqualTo(trieLogFixture);
  }

  @Test
  public void legacyFormatIsMainsFormat() {
    final TrieLogLayer layer =
        new TrieLogLayer()
            .setBlockHash(Hash.ZERO)
            .addAccountChange(
                accountFixture,
                null,
                new PatriciaTrieAccountValue(0, Wei.fromEth(1), Hash.EMPTY, Hash.EMPTY))
            .addCodeChange(Address.ZERO, null, Bytes.fromHexString("0xfeeddeadbeef"), Hash.ZERO)
            .addStorageChange(Address.ZERO, new StorageSlotKey(UInt256.ZERO), null, UInt256.ONE);

    assertThat(Bytes.wrap(new BonsaiTrieLogFactory().serialize(layer)).toHexString())
        .isEqualTo(LEGACY_RLP);
  }

  @Test
  public void accountWithoutStorageRootOrCodeHashReadsTheEmptyHashes() {
    for (final int wireVersion :
        new int[] {
          BonsaiTrieLogFactory.WIRE_VERSION_LEGACY, BonsaiTrieLogFactory.WIRE_VERSION_EXTENDED
        }) {
      final AccountValue account =
          new BonsaiTrieLogFactory()
              .deserialize(trieLogWithoutAccountHashes(wireVersion))
              .getAccountChanges()
              .get(accountFixture)
              .getUpdated();
      assertThat(account.getStorageRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
      assertThat(account.getCodeHash()).isEqualTo(Hash.EMPTY);
    }
  }

  @Test
  public void extendedFormatKeepsSlotKeysAndAccountShapes() {
    final PatriciaTrieAccountValue prior =
        new PatriciaTrieAccountValue(1, Wei.of(10), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
    final BinaryTrieAccountValue updated = new BinaryTrieAccountValue(2, Wei.of(20), Hash.EMPTY);
    final TrieLogLayer layer =
        new TrieLogLayer()
            .setBlockHash(headerFixture.getBlockHash())
            .setWireVersion(BonsaiTrieLogFactory.WIRE_VERSION_EXTENDED)
            .addAccountChange(accountFixture, prior, updated)
            .addStorageChange(Address.ZERO, new StorageSlotKey(UInt256.ZERO), null, UInt256.ONE);
    final TrieLogFactory factory = new BonsaiTrieLogFactory(Optional.of(0L));

    final TrieLog roundTripped = factory.deserialize(factory.serialize(layer));

    assertThat(roundTripped).isEqualTo(layer);
    assertThat(roundTripped.getAccountChanges().get(accountFixture).getPrior()).isEqualTo(prior);
    assertThat(roundTripped.getAccountChanges().get(accountFixture).getUpdated())
        .isEqualTo(updated);
    assertThat(roundTripped.getStorageChanges(Address.ZERO).keySet().iterator().next().getSlotKey())
        .isEqualTo(Optional.of(UInt256.ZERO));
  }

  @Test
  public void eachFactoryReadsTheOtherFormat() {
    final TrieLogFactory legacyFactory = new BonsaiTrieLogFactory();
    final TrieLogFactory extendedFactory = new BonsaiTrieLogFactory(Optional.of(0L));
    final TrieLogLayer extended =
        new TrieLogLayer()
            .setBlockHash(headerFixture.getBlockHash())
            .setWireVersion(BonsaiTrieLogFactory.WIRE_VERSION_EXTENDED)
            .addStorageChange(Address.ZERO, new StorageSlotKey(UInt256.ONE), null, UInt256.ONE);

    // main's format keeps no slot key, the extended one keeps it
    assertThat(
            extendedFactory
                .deserialize(legacyFactory.serialize(trieLogFixture))
                .getStorageChanges(Address.ZERO)
                .keySet()
                .iterator()
                .next()
                .getSlotKey())
        .isEmpty();
    assertThat(
            legacyFactory
                .deserialize(extendedFactory.serialize(extended))
                .getStorageChanges(Address.ZERO)
                .keySet()
                .iterator()
                .next()
                .getSlotKey())
        .isEqualTo(Optional.of(UInt256.ONE));
  }

  // one account change whose updated account has null in place of its storage root and code hash
  private byte[] trieLogWithoutAccountHashes(final int wireVersion) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    if (wireVersion != BonsaiTrieLogFactory.WIRE_VERSION_LEGACY) {
      out.writeInt(wireVersion);
    }
    out.writeBytes(headerFixture.getBlockHash().getBytes());
    out.startList();
    out.writeBytes(accountFixture.getBytes());
    out.startList();
    out.writeNull();
    out.startList();
    out.writeLongScalar(1);
    out.writeUInt256Scalar(Wei.ONE);
    out.writeNull();
    out.writeNull();
    out.endList();
    out.endList();
    out.writeNull();
    out.writeNull();
    out.endList();
    out.endList();
    return out.encoded().toArrayUnsafe();
  }
}
