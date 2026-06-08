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
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.NodeLoader.NodeSource;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;

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
  private final Cache<Bytes, NodeSource> accountNodeSources =
      CacheBuilder.newBuilder().maximumSize(ACCOUNT_CACHE_SIZE).build();
  private final Cache<Bytes, Bytes> storageNodes =
      CacheBuilder.newBuilder().recordStats().maximumSize(STORAGE_CACHE_SIZE).build();
  private final Cache<Bytes, NodeSource> storageNodeSources =
      CacheBuilder.newBuilder().maximumSize(STORAGE_CACHE_SIZE).build();

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
              accountStateNodeLoader(worldStateKeyValueStorage),
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
                          accountStorageNodeLoader(worldStateKeyValueStorage, accountHash),
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
    return accountStateNodeLoader(worldStateKeyValueStorage).getNode(location, nodeHash);
  }

  public Optional<Bytes> getAccountStorageTrieNode(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash) {
    return accountStorageNodeLoader(worldStateKeyValueStorage, accountHash)
        .getNode(location, nodeHash);
  }

  public NodeLoader accountStateNodeLoader(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage) {
    final NodeLoader storageLoader = worldStateKeyValueStorage.accountStateNodeLoader();
    return cachedNodeLoader(storageLoader, accountNodes, accountNodeSources);
  }

  public NodeLoader accountStorageNodeLoader(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage, final Hash accountHash) {
    final NodeLoader storageLoader = worldStateKeyValueStorage.accountStorageNodeLoader(accountHash);
    return cachedNodeLoader(storageLoader, storageNodes, storageNodeSources);
  }

  private NodeLoader cachedNodeLoader(
      final NodeLoader storageLoader,
      final Cache<Bytes, Bytes> nodeCache,
      final Cache<Bytes, NodeSource> sourceCache) {
    return new NodeLoader() {
      @Override
      public Optional<Bytes> getNode(final Bytes location, final Bytes32 hash) {
        return getNodeWithSource(location, hash, NodeSource.UNKNOWN)
            .map(NodeLoader.LoadedNode::getBytes);
      }

      @Override
      public Optional<NodeLoader.LoadedNode> getNodeWithSource(
          final Bytes location, final Bytes32 hash, final NodeSource preferredSource) {
        if (hash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
          return Optional.of(
              new NodeLoader.LoadedNode(MerkleTrie.EMPTY_TRIE_NODE, preferredSource));
        }
        final Bytes cachedNode = nodeCache.getIfPresent(hash);
        if (cachedNode != null) {
          return Optional.of(
              new NodeLoader.LoadedNode(
                  cachedNode,
                  Optional.ofNullable(sourceCache.getIfPresent(hash)).orElse(preferredSource)));
        }
        return storageLoader
            .getNodeWithSource(location, hash, preferredSource)
            .map(
                loadedNode -> {
                  nodeCache.put(hash, loadedNode.getBytes());
                  sourceCache.put(hash, loadedNode.getSource());
                  return loadedNode;
                });
      }
    };
  }
}
