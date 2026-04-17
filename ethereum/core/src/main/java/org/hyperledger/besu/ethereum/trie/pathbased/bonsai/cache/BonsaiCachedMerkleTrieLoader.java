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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache;

import static org.hyperledger.besu.metrics.BesuMetricCategory.BLOCKCHAIN;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class BonsaiCachedMerkleTrieLoader implements StorageSubscriber {

  private static final ExecutorService VIRTUAL_POOL = Executors.newVirtualThreadPerTaskExecutor();

  /**
   * Cap on concurrent preload tasks. Each preload descends a full trie path (6-8 chained RocksDB
   * reads), so allowing unbounded virtual threads causes IO queue saturation and cache thrash.
   * Capping at {@code CPU × 2} keeps the IO pipeline full without over-subscribing the disk.
   */
  private static final int MAX_CONCURRENT_PRELOADS =
      Math.max(4, Runtime.getRuntime().availableProcessors() * 2);

  private final Semaphore preloadPermits = new Semaphore(MAX_CONCURRENT_PRELOADS);

  /**
   * Depth threshold (in nibbles, i.e. {@code location.size()}) below which trie nodes are routed
   * to the high-priority "top-level" caches. Nodes near the root are re-read on every traversal
   * and must never be evicted by cold lower-level reads. At depth 4 the theoretical maximum is
   * {@code 16^4 = 65k} nodes; real-world trie sparsity gives a few thousand.
   */
  private static final int TOP_LEVEL_MAX_LOCATION_NIBBLES = 4;

  /** Oversized budget for top-level caches so they effectively never evict in practice. */
  private static final int TOP_LEVEL_ACCOUNT_CACHE_SIZE = 20_000;

  private static final int TOP_LEVEL_STORAGE_CACHE_SIZE = 20_000;

  private static final int ACCOUNT_CACHE_SIZE = 100_000;
  private static final int STORAGE_CACHE_SIZE = 200_000;

  /** Top-level account trie nodes (depth ≤ {@link #TOP_LEVEL_MAX_LOCATION_NIBBLES}). */
  private final Cache<Bytes, Bytes> topLevelAccountNodes =
      CacheBuilder.newBuilder().recordStats().maximumSize(TOP_LEVEL_ACCOUNT_CACHE_SIZE).build();

  /** Top-level storage trie nodes (depth ≤ {@link #TOP_LEVEL_MAX_LOCATION_NIBBLES}). */
  private final Cache<Bytes, Bytes> topLevelStorageNodes =
      CacheBuilder.newBuilder().recordStats().maximumSize(TOP_LEVEL_STORAGE_CACHE_SIZE).build();

  private final Cache<Bytes, Bytes> accountNodes =
      CacheBuilder.newBuilder().recordStats().maximumSize(ACCOUNT_CACHE_SIZE).build();
  private final Cache<Bytes, Bytes> storageNodes =
      CacheBuilder.newBuilder().recordStats().maximumSize(STORAGE_CACHE_SIZE).build();

  public BonsaiCachedMerkleTrieLoader(final ObservableMetricsSystem metricsSystem) {
    metricsSystem.createGuavaCacheCollector(BLOCKCHAIN, "accountsNodes", accountNodes);
    metricsSystem.createGuavaCacheCollector(BLOCKCHAIN, "storageNodes", storageNodes);
    metricsSystem.createGuavaCacheCollector(
        BLOCKCHAIN, "topLevelAccountNodes", topLevelAccountNodes);
    metricsSystem.createGuavaCacheCollector(
        BLOCKCHAIN, "topLevelStorageNodes", topLevelStorageNodes);
  }

  public void preLoadAccount(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash worldStateRootHash,
      final Address account) {
    CompletableFuture.runAsync(
        () -> {
          preloadPermits.acquireUninterruptibly();
          try {
            cacheAccountNodes(worldStateKeyValueStorage, worldStateRootHash, account);
          } finally {
            preloadPermits.release();
          }
        },
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
                node.ifPresent(bytes -> putAccountNode(location, bytes));
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
        () -> {
          preloadPermits.acquireUninterruptibly();
          try {
            cacheStorageNodes(worldStateKeyValueStorage, account, slotKey);
          } finally {
            preloadPermits.release();
          }
        },
        VIRTUAL_POOL);
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
                            node.ifPresent(bytes -> putStorageNode(location, bytes));
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
    }
    final Bytes fromTop = topLevelAccountNodes.getIfPresent(nodeHash);
    if (fromTop != null) {
      return Optional.of(fromTop);
    }
    final Bytes fromLru = accountNodes.getIfPresent(nodeHash);
    if (fromLru != null) {
      return Optional.of(fromLru);
    }
    return worldStateKeyValueStorage.getAccountStateTrieNode(location, nodeHash);
  }

  public Optional<Bytes> getAccountStorageTrieNode(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    }
    final Bytes fromTop = topLevelStorageNodes.getIfPresent(nodeHash);
    if (fromTop != null) {
      return Optional.of(fromTop);
    }
    final Bytes fromLru = storageNodes.getIfPresent(nodeHash);
    if (fromLru != null) {
      return Optional.of(fromLru);
    }
    return worldStateKeyValueStorage.getAccountStorageTrieNode(accountHash, location, nodeHash);
  }

  /**
   * Routes a freshly loaded account trie node to the right cache based on its depth. Nodes near
   * the root (depth ≤ {@link #TOP_LEVEL_MAX_LOCATION_NIBBLES}) land in the high-priority cache
   * where they effectively never evict; deeper nodes use the standard LRU.
   */
  private void putAccountNode(final Bytes location, final Bytes node) {
    final Bytes key = Hash.hash(node).getBytes();
    if (location.size() <= TOP_LEVEL_MAX_LOCATION_NIBBLES) {
      topLevelAccountNodes.put(key, node);
    } else {
      accountNodes.put(key, node);
    }
  }

  private void putStorageNode(final Bytes location, final Bytes node) {
    final Bytes key = Hash.hash(node).getBytes();
    if (location.size() <= TOP_LEVEL_MAX_LOCATION_NIBBLES) {
      topLevelStorageNodes.put(key, node);
    } else {
      storageNodes.put(key, node);
    }
  }
}
