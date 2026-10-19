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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.GenesisConfig;

import java.util.Optional;

import org.junit.jupiter.api.Test;

class BinaryTrieForkActivationTest {

  private static final String AMSTERDAM_ONLY_GENESIS =
      "/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/genesis-amsterdam-only.json";
  private static final String BINARY_TRIE_AT_GENESIS =
      "/org/hyperledger/besu/ethereum/trie/pathbased/bonsai/genesis-binary-trie-at-genesis.json";

  @Test
  void isBinaryTrieActive_requiresBinaryTrieMilestoneOnly() {
    assertThat(BinaryTrieForkSupport.isBinaryTrieActive(0L, Optional.empty())).isFalse();
    assertThat(BinaryTrieForkSupport.isBinaryTrieActive(0L, Optional.of(0L))).isTrue();
  }

  @Test
  void isBinaryTrieActiveAtGenesis_requiresBinaryTrieTimestamp() {
    final GenesisConfig amsterdamOnly = GenesisConfig.fromResource(AMSTERDAM_ONLY_GENESIS);
    final GenesisConfig binaryTrieAtGenesis = GenesisConfig.fromResource(BINARY_TRIE_AT_GENESIS);

    assertThat(BinaryTrieForkSupport.isBinaryTrieActiveAtGenesis(amsterdamOnly)).isFalse();
    assertThat(BinaryTrieForkSupport.isBinaryTrieActiveAtGenesis(binaryTrieAtGenesis)).isTrue();
  }

  @Test
  void isBinaryTrieTransition_onlyWhenCrossingFromPrePbtParentToPbtNext() {
    final Optional<Long> milestone = Optional.of(100L);

    // Case 2: parent pre-PBT, next at/after milestone
    assertThat(BinaryTrieForkSupport.isBinaryTrieTransition(99L, milestone, Optional.of(100L)))
        .isTrue();
    assertThat(BinaryTrieForkSupport.isBinaryTrieTransition(50L, milestone, Optional.of(150L)))
        .isTrue();

    // Case 1: both pre-PBT
    assertThat(BinaryTrieForkSupport.isBinaryTrieTransition(50L, milestone, Optional.of(99L)))
        .isFalse();

    // Case 3: parent already PBT (including parent.ts == milestone)
    assertThat(BinaryTrieForkSupport.isBinaryTrieTransition(100L, milestone, Optional.of(100L)))
        .isFalse();
    assertThat(BinaryTrieForkSupport.isBinaryTrieTransition(100L, milestone, Optional.of(200L)))
        .isFalse();
    assertThat(BinaryTrieForkSupport.isBinaryTrieTransition(150L, milestone, Optional.of(200L)))
        .isFalse();

    // No milestone / no next timestamp → never a transition
    assertThat(
            BinaryTrieForkSupport.isBinaryTrieTransition(99L, Optional.empty(), Optional.of(100L)))
        .isFalse();
    assertThat(BinaryTrieForkSupport.isBinaryTrieTransition(99L, milestone, Optional.empty()))
        .isFalse();
  }
}
