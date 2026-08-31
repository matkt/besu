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

import org.hyperledger.besu.config.GenesisConfig;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * Activation rules for the partitioned binary trie (PBT) storage branch.
 *
 * <p>PBT is distinct from the Amsterdam protocol fork: Amsterdam enables BAL, slot numbers, and
 * related execution rules, while {@code binaryTrieTime} selects the BINARY trie branch for state
 * root computation and trie-log encoding. PBT activates when {@code binaryTrieTime} is reached at
 * block timestamp; Amsterdam activation is not required.
 */
public final class BinaryTrieForkSupport {

  private BinaryTrieForkSupport() {}

  public static boolean isBinaryTrieActive(
      final long blockTimestamp, final Optional<Long> binaryTrieMilestone) {
    return isMilestoneActive(blockTimestamp, binaryTrieMilestone);
  }

  public static boolean isBinaryTrieActiveAtGenesis(final GenesisConfig genesis) {
    return isBinaryTrieForkActiveAtGenesis(genesis);
  }

  private static boolean isBinaryTrieForkActiveAtGenesis(final GenesisConfig genesis) {
    final OptionalLong binaryTrieTimestamp = genesis.getConfigOptions().getBinaryTrieTime();
    return binaryTrieTimestamp.isPresent()
        && genesis.getTimestamp() >= binaryTrieTimestamp.getAsLong();
  }

  private static boolean isMilestoneActive(
      final long blockTimestamp, final Optional<Long> milestone) {
    return milestone.map(m -> Long.compareUnsigned(blockTimestamp, m) >= 0).orElse(false);
  }

  /**
   * True when {@code parentBlockTimestamp} is still pre-PBT and {@code nextBlockTimestamp} is at or
   * past {@code binaryTrieTime} — i.e. the lookup is for the parent of the first PBT block.
   */
  public static boolean isBinaryTrieTransition(
      final long parentBlockTimestamp,
      final Optional<Long> binaryTrieMilestone,
      final Optional<Long> nextBlockTimestamp) {
    if (nextBlockTimestamp.isEmpty()) {
      return false;
    }
    return !isBinaryTrieActive(parentBlockTimestamp, binaryTrieMilestone)
        && isBinaryTrieActive(nextBlockTimestamp.get(), binaryTrieMilestone);
  }
}
