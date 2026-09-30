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
package org.hyperledger.besu.ethereum.trie.patricia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class AscendingCollapsePatriciaTrieTest {

  @Test
  void matchesSimpleMerklePatriciaTrie() {
    final MerkleTrie<Bytes, Bytes> simple = new SimpleMerklePatriciaTrie<>(Function.identity());
    final AscendingCollapsePatriciaTrie ascending = new AscendingCollapsePatriciaTrie();
    final List<Bytes> keys = new ArrayList<>();
    for (int i = 0; i < 64; i++) {
      keys.add(Hash.hash(Bytes.ofUnsignedInt(i)).getBytes());
    }
    keys.sort(Comparator.naturalOrder());
    for (int i = 0; i < keys.size(); i++) {
      final int idx = i;
      final Bytes value = RLP.encode(out -> out.writeBytes(Bytes.ofUnsignedInt(idx + 1)));
      simple.put(keys.get(i), value);
      ascending.insert(keys.get(i), value);
    }
    assertThat(ascending.rootHash()).isEqualTo(simple.getRootHash());
  }

  @Test
  void emptyTrieHasTheEmptyRoot() {
    assertThat(new AscendingCollapsePatriciaTrie().rootHash())
        .isEqualTo(new SimpleMerklePatriciaTrie<>(Function.identity()).getRootHash());
  }

  @Test
  void rejectsDescendingKeys() {
    final AscendingCollapsePatriciaTrie ascending = new AscendingCollapsePatriciaTrie();
    final Bytes value = RLP.encode(out -> out.writeInt(1));
    ascending.insert(Bytes.fromHexString("0x" + "ff".repeat(32)), value);
    assertThatThrownBy(() -> ascending.insert(Bytes.fromHexString("0x" + "00".repeat(32)), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ascending");
  }
}
