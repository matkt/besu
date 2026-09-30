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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload;

import static org.hyperledger.besu.metrics.BesuMetricCategory.BLOCKCHAIN;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeStrategy.TrieNodeRequest;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class BonsaiCachedMerkleTrieLoader implements StorageSubscriber {

  private static final ExecutorService VIRTUAL_POOL = Executors.newVirtualThreadPerTaskExecutor();

  private static final int ACCOUNT_CACHE_SIZE = 100_000;
  private static final int STORAGE_CACHE_SIZE = 200_000;
  private final Cache<Bytes, Bytes> accountNodes =
      CacheBuilder.newBuilder().recordStats().maximumSize(ACCOUNT_CACHE_SIZE).build();
  private final Cache<Bytes, Bytes> storageNodes =
      CacheBuilder.newBuilder().recordStats().maximumSize(STORAGE_CACHE_SIZE).build();

  public BonsaiCachedMerkleTrieLoader(final ObservableMetricsSystem metricsSystem) {
    metricsSystem.createGuavaCacheCollector(BLOCKCHAIN, "accountsNodes", accountNodes);
    metricsSystem.createGuavaCacheCollector(BLOCKCHAIN, "storageNodes", storageNodes);
  }

  public void preLoadAccount(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash worldStateRootHash,
      final Address account) {
    CompletableFuture.runAsync(
        () -> cacheAccountNodes(worldStateKeyValueStorage, worldStateRootHash, account),
        VIRTUAL_POOL);
  }

  @VisibleForTesting
  public void cacheAccountNodes(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash worldStateRootHash,
      final Address account) {
    final long storageSubscriberId = worldStateKeyValueStorage.subscribe(this);
    try {
      final StoredMerklePatriciaTrie<Bytes, Bytes> accountTrie =
          new StoredMerklePatriciaTrie<>(
              (location, hash) -> {
                Optional<Bytes> node =
                    getAccountStateTrieNode(worldStateKeyValueStorage, location, hash);
                node.ifPresent(bytes -> accountNodes.put(Hash.hash(bytes).getBytes(), bytes));
                return node;
              },
              Bytes32.wrap(worldStateRootHash.getBytes()),
              Function.identity(),
              Function.identity());
      accountTrie.get(account.addressHash().getBytes());
    } catch (MerkleTrieException e) {
      // ignore exception for the cache
    } finally {
      worldStateKeyValueStorage.unSubscribe(storageSubscriberId);
    }
  }

  public void preLoadStorageSlot(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Address account,
      final StorageSlotKey slotKey) {
    CompletableFuture.runAsync(
        () -> cacheStorageNodes(worldStateKeyValueStorage, account, slotKey), VIRTUAL_POOL);
  }

  @VisibleForTesting
  public void cacheStorageNodes(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Address account,
      final StorageSlotKey slotKey) {
    final Hash accountHash = account.addressHash();
    final long storageSubscriberId = worldStateKeyValueStorage.subscribe(this);
    try {
      worldStateKeyValueStorage
          .getStateTrieNode(Bytes.concatenate(accountHash.getBytes(), Bytes.EMPTY))
          .ifPresent(
              storageRoot -> {
                try {
                  final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
                      new StoredMerklePatriciaTrie<Bytes, Bytes>(
                          (location, hash) -> {
                            Optional<Bytes> node =
                                getAccountStorageTrieNode(
                                    worldStateKeyValueStorage, accountHash, location, hash);
                            node.ifPresent(
                                bytes -> storageNodes.put(Hash.hash(bytes).getBytes(), bytes));
                            return node;
                          },
                          Bytes32.wrap(Hash.hash(storageRoot).getBytes()),
                          Function.identity(),
                          Function.identity());
                  storageTrie.get(slotKey.getSlotHash().getBytes());
                } catch (MerkleTrieException e) {
                  // ignore exception for the cache
                }
              });
    } finally {
      worldStateKeyValueStorage.unSubscribe(storageSubscriberId);
    }
  }

  public Optional<Bytes> getAccountStateTrieNode(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Bytes location,
      final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else {
      return Optional.ofNullable(accountNodes.getIfPresent(nodeHash))
          .or(() -> worldStateKeyValueStorage.getAccountStateTrieNode(location, nodeHash));
    }
  }

  public Optional<Bytes> getAccountStorageTrieNode(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else {
      return Optional.ofNullable(storageNodes.getIfPresent(nodeHash))
          .or(
              () ->
                  worldStateKeyValueStorage.getAccountStorageTrieNode(
                      accountHash, location, nodeHash));
    }
  }

  /**
   * Batch variant of {@link #getAccountStateTrieNode}: cached nodes are served from memory, the
   * others are read in one batch.
   */
  public List<Optional<Bytes>> getAccountStateTrieNodes(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final List<Bytes> locations,
      final List<Bytes32> nodeHashes) {
    return getTrieNodes(worldStateKeyValueStorage, accountNodes, null, locations, nodeHashes);
  }

  /**
   * Batch variant of {@link #getAccountStorageTrieNode}: cached nodes are served from memory, the
   * others are read in one batch.
   */
  public List<Optional<Bytes>> getAccountStorageTrieNodes(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash accountHash,
      final List<Bytes> locations,
      final List<Bytes32> nodeHashes) {
    return getTrieNodes(
        worldStateKeyValueStorage, storageNodes, accountHash, locations, nodeHashes);
  }

  private static List<Optional<Bytes>> getTrieNodes(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Cache<Bytes, Bytes> cache,
      final Hash accountHash,
      final List<Bytes> locations,
      final List<Bytes32> nodeHashes) {
    final List<Optional<Bytes>> results = new ArrayList<>(locations.size());
    final List<Integer> missIndexes = new ArrayList<>();
    final List<TrieNodeRequest> misses = new ArrayList<>();
    for (int i = 0; i < locations.size(); i++) {
      final Bytes32 nodeHash = nodeHashes.get(i);
      final Bytes cached =
          nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)
              ? MerkleTrie.EMPTY_TRIE_NODE
              : cache.getIfPresent(nodeHash);
      results.add(Optional.ofNullable(cached));
      if (cached == null) {
        missIndexes.add(i);
        misses.add(
            accountHash == null
                ? TrieNodeRequest.account(locations.get(i), nodeHash)
                : TrieNodeRequest.storage(accountHash, locations.get(i), nodeHash));
      }
    }
    if (!misses.isEmpty()) {
      final List<Optional<Bytes>> loaded = worldStateKeyValueStorage.getTrieNodes(misses);
      for (int i = 0; i < misses.size(); i++) {
        results.set(missIndexes.get(i), loaded.get(i));
      }
    }
    return results;
  }
}
