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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.trie.common.BinaryTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/**
 * Switch / reorg encoding: prior must stay MPT (4-field) so flat rollback is decodable under {@code
 * MptStorageRootStrategy}; updated on a binary tip is 3-field.
 */
class BonsaiTrieLogAccountEncodingTest {

  private static final Hash STORAGE_ROOT =
      Hash.fromHexString("0x1111111111111111111111111111111111111111111111111111111111111111");
  private static final Hash CODE_HASH =
      Hash.fromHexString("0x2222222222222222222222222222222222222222222222222222222222222222");

  @Test
  void serializeRoundTripPreservesPatriciaPriorAndBinaryUpdated() {
    final Address address = Address.fromHexString("0xabcdef");
    final PatriciaTrieAccountValue prior =
        new PatriciaTrieAccountValue(1L, Wei.of(10), STORAGE_ROOT, CODE_HASH);
    final BinaryTrieAccountValue updated = new BinaryTrieAccountValue(2L, Wei.of(20), CODE_HASH);
    final TrieLogLayer layer =
        new TrieLogLayer()
            .setBlockHash(Hash.ZERO)
            .setWireVersion(BonsaiTrieLogFactory.WIRE_VERSION_EXTENDED)
            .addAccountChange(address, prior, updated);

    final BonsaiTrieLogFactory factory = new BonsaiTrieLogFactory(Optional.of(0L));
    final TrieLogLayer roundTripped = factory.deserialize(factory.serialize(layer));

    final AccountValue priorOut = roundTripped.getAccountChanges().get(address).getPrior();
    final AccountValue updatedOut = roundTripped.getAccountChanges().get(address).getUpdated();
    assertThat(priorOut).isInstanceOf(PatriciaTrieAccountValue.class);
    assertThat(updatedOut).isInstanceOf(BinaryTrieAccountValue.class);
    assertThat(fieldCount(RLP.encode(priorOut::writeTo))).isEqualTo(4);
    assertThat(fieldCount(RLP.encode(updatedOut::writeTo))).isEqualTo(3);
    assertThat(((PatriciaTrieAccountValue) priorOut).getStorageRoot()).isEqualTo(STORAGE_ROOT);
  }

  private static int fieldCount(final Bytes accountRlp) {
    final RLPInput in = RLP.input(accountRlp);
    final int count = in.enterList();
    in.leaveListLenient();
    return count;
  }
}
