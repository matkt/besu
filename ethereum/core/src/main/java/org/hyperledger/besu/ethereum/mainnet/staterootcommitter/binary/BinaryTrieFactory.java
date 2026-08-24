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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary;

import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockProcessingExecutors;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.BalStateRootCommitter;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.ParallelStoredPartitionedBinaryTrie;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.StoredPartitionedBinaryTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.util.concurrent.ForkJoinPool;

import org.apache.tuweni.bytes.Bytes32;

/**
 * Builds partitioned binary tries rooted at a Bonsai world state.
 *
 * <p>Binary construction is format-specific and does not belong on the format-agnostic {@link
 * BonsaiWorldState}. Both Binary state-root committers ({@link DefaultBinaryStateRootCommitter} and
 * {@link BalStateRootCommitter}) obtain the state trie from this factory; {@link BinaryTrieWriter}
 * writes EIP-8297 leaves onto that trie.
 */
public final class BinaryTrieFactory {

  private BinaryTrieFactory() {}

  /** State trie rooted at the world state's current root. */
  public static StoredPartitionedBinaryTrie createStateTrie(final BonsaiWorldState worldState) {
    return createTrie(
        (location, hash) ->
            worldState.getWorldStateStorage().getTrieNode(TrieBranchType.BINARY, location, hash),
        Bytes32.wrap(worldState.getWorldStateRootHash().getBytes()),
        worldState.getWorldStateConfig(),
        BlockProcessingExecutors.accountTrieForkJoinPool());
  }

  /**
   * Builds a binary trie from an arbitrary {@link NodeLoader} and root hash. Defaults to the
   * account-trie fork join pool, matching {@link #createStateTrie}.
   */
  public static StoredPartitionedBinaryTrie createTrie(
      final NodeLoader nodeLoader,
      final Bytes32 rootHash,
      final WorldStateConfig worldStateConfig) {
    return createTrie(
        nodeLoader, rootHash, worldStateConfig, BlockProcessingExecutors.accountTrieForkJoinPool());
  }

  private static StoredPartitionedBinaryTrie createTrie(
      final NodeLoader nodeLoader,
      final Bytes32 rootHash,
      final WorldStateConfig worldStateConfig,
      final ForkJoinPool forkJoinPool) {
    if (worldStateConfig.isParallelStateRootComputationEnabled()) {
      return new ParallelStoredPartitionedBinaryTrie(nodeLoader, rootHash, forkJoinPool);
    }
    return new StoredPartitionedBinaryTrie(nodeLoader, rootHash);
  }
}
