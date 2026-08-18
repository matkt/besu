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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter.patricia;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockProcessingExecutors;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.BalStateRootCommitter;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NoOpMerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.patricia.ParallelStoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Builds Patricia tries rooted at a Bonsai world state.
 *
 * <p>Patricia construction is format-specific and does not belong on the format-agnostic {@link
 * BonsaiWorldState}. Both Patricia state-root committers ({@link DefaultPatriciaStateRootCommitter}
 * and {@link BalStateRootCommitter}) and the Patricia storage enumeration used for account deletion
 * share this factory, keeping {@code BonsaiWorldState} free of Patricia-trie-construction details.
 */
public final class PatriciaTrieFactory {

  private PatriciaTrieFactory() {}

  /** Account state trie rooted at the world state's current root, using the shared node cache. */
  public static MerkleTrie<Bytes, Bytes> createAccountStateTrie(final BonsaiWorldState worldState) {
    final BonsaiCachedMerkleTrieLoader cachedMerkleTrieLoader =
        worldState.getBonsaiCachedMerkleTrieLoader();
    return createTrie(
        (location, hash) ->
            cachedMerkleTrieLoader.getAccountStateTrieNode(
                worldState.getWorldStateStorage(), location, hash),
        Bytes32.wrap(worldState.getWorldStateRootHash().getBytes()),
        worldState.getWorldStateConfig(),
        BlockProcessingExecutors.accountTrieForkJoinPool());
  }

  /**
   * Storage trie for the given account rooted at {@code storageRoot}, using the shared node cache.
   */
  public static MerkleTrie<Bytes, Bytes> createStorageTrie(
      final BonsaiWorldState worldState, final Hash accountHash, final Hash storageRoot) {
    final BonsaiCachedMerkleTrieLoader cachedMerkleTrieLoader =
        worldState.getBonsaiCachedMerkleTrieLoader();
    return createTrie(
        (location, key) ->
            cachedMerkleTrieLoader.getAccountStorageTrieNode(
                worldState.getWorldStateStorage(), accountHash, location, key),
        Bytes32.wrap(storageRoot.getBytes()),
        worldState.getWorldStateConfig(),
        BlockProcessingExecutors.storageTrieForkJoinPool());
  }

  /**
   * Builds a Patricia trie from an arbitrary {@link NodeLoader} and root hash. Used for Patricia
   * storage enumeration during account deletion (see {@link #getAllAccountStorage}). Defaults to
   * the account-trie fork join pool, matching the prior {@code BonsaiWorldState.createTrie}
   * behaviour.
   */
  public static MerkleTrie<Bytes, Bytes> createTrie(
      final NodeLoader nodeLoader,
      final Bytes32 rootHash,
      final WorldStateConfig worldStateConfig) {
    return createTrie(
        nodeLoader, rootHash, worldStateConfig, BlockProcessingExecutors.accountTrieForkJoinPool());
  }

  /**
   * Enumerates every storage slot of an account by walking its Patricia storage trie. Used during
   * account deletion to clear storage slots that were not touched in the current transaction.
   *
   * <p>This is Patricia-specific: it builds a storage trie rooted at {@code rootHash} over the
   * world state's trie nodes and returns the trie entries keyed by hashed slot. Binary accounts
   * carry no storage root and must not reach this path (callers guard with {@code
   * hasStorageRoot()}).
   *
   * @param worldState the world state providing the trie node storage and config.
   * @param address the account whose storage is enumerated.
   * @param rootHash the root of the account's Patricia storage trie.
   * @return a map of hashed-slot-key → RLP-encoded storage value, as returned by {@link
   *     MerkleTrie#entriesFrom}.
   */
  public static Map<Bytes32, Bytes> getAllAccountStorage(
      final BonsaiWorldState worldState, final Address address, final Hash rootHash) {
    final MerkleTrie<Bytes, Bytes> storageTrie =
        createTrie(
            (location, key) ->
                worldState
                    .getWorldStateStorage()
                    .getTrieNode(
                        Bytes.concatenate(address.addressHash().getBytes(), location), key),
            Bytes32.wrap(rootHash.getBytes()),
            worldState.getWorldStateConfig());
    return storageTrie.entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE);
  }

  private static MerkleTrie<Bytes, Bytes> createTrie(
      final NodeLoader nodeLoader,
      final Bytes32 rootHash,
      final WorldStateConfig worldStateConfig,
      final ForkJoinPool forkJoinPool) {
    if (worldStateConfig.isTrieDisabled()) {
      return new NoOpMerkleTrie<>();
    }
    if (worldStateConfig.isParallelStateRootComputationEnabled()) {
      return new ParallelStoredMerklePatriciaTrie<>(
          nodeLoader, rootHash, Function.identity(), Function.identity(), forkJoinPool);
    }
    return new StoredMerklePatriciaTrie<>(
        nodeLoader, rootHash, Function.identity(), Function.identity());
  }
}
