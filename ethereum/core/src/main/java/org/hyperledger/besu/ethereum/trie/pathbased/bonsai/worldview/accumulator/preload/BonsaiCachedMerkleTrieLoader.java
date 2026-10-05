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
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class BonsaiCachedMerkleTrieLoader implements StorageSubscriber {

  private static final ExecutorService VIRTUAL_POOL = Executors.newVirtualThreadPerTaskExecutor();

  private static final int ACCOUNT_CACHE_SIZE = 100_000;
  private static final int STORAGE_CACHE_SIZE = 200_000;

  // Guava locks a segment per write, and the preload walks write from many threads at once
  private static final int CACHE_CONCURRENCY =
      Math.max(4, Runtime.getRuntime().availableProcessors());

  private final Cache<Bytes, Bytes> accountNodes =
      CacheBuilder.newBuilder()
          .concurrencyLevel(CACHE_CONCURRENCY)
          .recordStats()
          .maximumSize(ACCOUNT_CACHE_SIZE)
          .build();
  private final Cache<Bytes, Bytes> storageNodes =
      CacheBuilder.newBuilder()
          .concurrencyLevel(CACHE_CONCURRENCY)
          .recordStats()
          .maximumSize(STORAGE_CACHE_SIZE)
          .build();

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
              (location, hash) ->
                  getOrLoad(
                      accountNodes,
                      hash,
                      () ->
                          worldStateKeyValueStorage.getAccountStateTrieNodeFromCacheOrStorage(
                              location, hash)),
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
      // the account holds the storage root hash, so the root node needn't be read and hashed
      worldStateKeyValueStorage
          .getAccount(accountHash)
          .map(rlp -> PmtStateTrieAccountValue.readFrom(RLP.input(rlp)).getStorageRoot())
          .filter(storageRoot -> !storageRoot.equals(Hash.EMPTY_TRIE_HASH))
          .ifPresent(
              storageRoot -> {
                try {
                  final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
                      new StoredMerklePatriciaTrie<Bytes, Bytes>(
                          (location, hash) ->
                              getOrLoad(
                                  storageNodes,
                                  hash,
                                  () ->
                                      worldStateKeyValueStorage
                                          .getAccountStorageTrieNodeFromCacheOrStorage(
                                              accountHash, location, hash)),
                          Bytes32.wrap(storageRoot.getBytes()),
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

  /**
   * Returns the node from the cache, or loads it and caches it under the hash it was requested by:
   * storage only returns nodes matching that hash and the other caches are keyed by it, so the node
   * needn't be hashed again. A cache hit is not written back.
   */
  private static Optional<Bytes> getOrLoad(
      final Cache<Bytes, Bytes> cache,
      final Bytes32 nodeHash,
      final Supplier<Optional<Bytes>> loader) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    }
    final Bytes cached = cache.getIfPresent(nodeHash);
    if (cached != null) {
      return Optional.of(cached);
    }
    final Optional<Bytes> node = loader.get();
    // the hash may be a slice of the parent node's RLP; copy it so the key doesn't pin that
    node.ifPresent(bytes -> cache.put(nodeHash.copy(), bytes));
    return node;
  }

  public Optional<Bytes> getAccountStateTrieNode(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Bytes location,
      final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    } else {
      return Optional.ofNullable(accountNodes.getIfPresent(nodeHash))
          .or(
              () ->
                  worldStateKeyValueStorage.getAccountStateTrieNodeFromCacheOrStorage(
                      location, nodeHash));
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
                  worldStateKeyValueStorage.getAccountStorageTrieNodeFromCacheOrStorage(
                      accountHash, location, nodeHash));
    }
  }
}
