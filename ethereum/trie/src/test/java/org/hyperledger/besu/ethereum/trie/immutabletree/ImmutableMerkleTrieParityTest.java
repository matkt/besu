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
package org.hyperledger.besu.ethereum.trie.immutabletree;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class ImmutableMerkleTrieParityTest {

  @Test
  void rootHashMatchesStoredMerklePatriciaTrie() {
    final PersistentImmutableTreeCache cache =
        new PersistentImmutableTreeCache((l, h) -> Optional.empty(), 512);
    final ImmutableMerkleTrie immutable =
        new ImmutableMerkleTrie(
            cache, MerkleTrie.EMPTY_TRIE_NODE_HASH, RootKind.STATE, (l, h) -> Optional.empty());

    final MerkleTrie<Bytes, Bytes> classic =
        new StoredMerklePatriciaTrie<>(
            (l, h) -> Optional.empty(),
            MerkleTrie.EMPTY_TRIE_NODE_HASH,
            Function.identity(),
            Function.identity());

    final Bytes[] keys =
        new Bytes[] {
          Bytes.fromHexString("0x01"),
          Bytes.fromHexString("0x02"),
          Bytes.fromHexString("0x0100"),
          Bytes.fromHexString("0xabcd"),
          Bytes.fromHexString("0xabce"),
          Bytes32.ZERO
        };
    final Bytes[] values =
        new Bytes[] {
          Bytes.fromHexString("0xaa"),
          Bytes.fromHexString("0xbb"),
          Bytes.fromHexString("0xcc"),
          Bytes.fromHexString("0xdd"),
          Bytes.fromHexString("0xee"),
          Bytes.fromHexString("0xff01")
        };

    for (int i = 0; i < keys.length; i++) {
      immutable.put(keys[i], values[i]);
      classic.put(keys[i], values[i]);
      assertThat(immutable.getRootHash())
          .as("root after put %s", keys[i])
          .isEqualTo(classic.getRootHash());
      assertThat(immutable.get(keys[i])).isEqualTo(classic.get(keys[i]));
    }

    immutable.remove(keys[2]);
    classic.remove(keys[2]);
    assertThat(immutable.getRootHash()).as("after remove").isEqualTo(classic.getRootHash());
  }
}
