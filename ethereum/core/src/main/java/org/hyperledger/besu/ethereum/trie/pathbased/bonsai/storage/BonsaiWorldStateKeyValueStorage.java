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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.BINARY_TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.PATRICIA_TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.TrieBranchSegments;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.cache.VersionedFlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategy;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.evm.account.AccountStorageEntry;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.WorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;
import org.hyperledger.besu.util.Subscribers;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiWorldStateKeyValueStorage implements WorldStateKeyValueStorage, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiWorldStateKeyValueStorage.class);

  public static final byte[] WORLD_ROOT_HASH_KEY = "worldRoot".getBytes(StandardCharsets.UTF_8);
  public static final byte[] WORLD_BLOCK_HASH_KEY =
      "worldBlockHash".getBytes(StandardCharsets.UTF_8);
  public static final byte[] WORLD_BLOCK_NUMBER_KEY =
      "worldBlockNumber".getBytes(StandardCharsets.UTF_8);

  private final AtomicBoolean shouldClose = new AtomicBoolean(false);
  protected final AtomicBoolean isClosed = new AtomicBoolean(false);
  protected final Subscribers<StorageSubscriber> subscribers = Subscribers.create();
  protected final SegmentedKeyValueStorage composedWorldStateStorage;
  protected final KeyValueStorage trieLogStorage;

  protected final BonsaiFlatDbStrategyProvider flatDbStrategyProvider;
  protected final FlatDbCacheManager cacheManager;
  private final DataStorageFormat dataStorageFormat;
  private volatile long cacheVersion;

  public BonsaiWorldStateKeyValueStorage(
      final StorageProvider provider,
      final MetricsSystem metricsSystem,
      final DataStorageConfiguration dataStorageConfiguration) {
    this(
        provider,
        metricsSystem,
        dataStorageConfiguration,
        createCacheManager(dataStorageConfiguration, metricsSystem));
  }

  public BonsaiWorldStateKeyValueStorage(
      final StorageProvider provider,
      final MetricsSystem metricsSystem,
      final DataStorageConfiguration dataStorageConfiguration,
      final FlatDbCacheManager cacheManager) {
    this.composedWorldStateStorage =
        provider.getStorageBySegmentIdentifiers(
            List.of(
                ACCOUNT_INFO_STATE,
                CODE_STORAGE,
                ACCOUNT_STORAGE_STORAGE,
                PATRICIA_TRIE_BRANCH_STORAGE,
                BINARY_TRIE_BRANCH_STORAGE));
    this.trieLogStorage =
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE);
    this.flatDbStrategyProvider =
        new BonsaiFlatDbStrategyProvider(metricsSystem, dataStorageConfiguration);
    flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);

    this.cacheManager = cacheManager;
    this.cacheVersion = cacheManager.getCurrentVersion();
    this.dataStorageFormat = dataStorageConfiguration.getDataStorageFormat();
  }

  public BonsaiWorldStateKeyValueStorage(
      final BonsaiFlatDbStrategyProvider flatDbStrategyProvider,
      final SegmentedKeyValueStorage composedWorldStateStorage,
      final KeyValueStorage trieLogStorage,
      final FlatDbCacheManager cacheManager,
      final long cacheVersion) {
    this(
        flatDbStrategyProvider,
        composedWorldStateStorage,
        trieLogStorage,
        cacheManager,
        cacheVersion,
        DataStorageFormat.BONSAI);
  }

  public BonsaiWorldStateKeyValueStorage(
      final BonsaiFlatDbStrategyProvider flatDbStrategyProvider,
      final SegmentedKeyValueStorage composedWorldStateStorage,
      final KeyValueStorage trieLogStorage,
      final FlatDbCacheManager cacheManager,
      final long cacheVersion,
      final DataStorageFormat dataStorageFormat) {
    this.composedWorldStateStorage = composedWorldStateStorage;
    this.trieLogStorage = trieLogStorage;
    this.flatDbStrategyProvider = flatDbStrategyProvider;
    this.cacheManager = cacheManager;
    this.cacheVersion = cacheVersion;
    this.dataStorageFormat = dataStorageFormat;
  }

  private static FlatDbCacheManager createCacheManager(
      final DataStorageConfiguration dataStorageConfiguration, final MetricsSystem metricsSystem) {
    if (dataStorageConfiguration
        .getPathBasedExtraStorageConfiguration()
        .getUnstable()
        .getBonsaiCrossBlockCacheEnabled()) {
      return new VersionedFlatDbCacheManager(
          dataStorageConfiguration
              .getPathBasedExtraStorageConfiguration()
              .getUnstable()
              .getBonsaiCrossBlockCacheAccountSize(),
          dataStorageConfiguration
              .getPathBasedExtraStorageConfiguration()
              .getUnstable()
              .getBonsaiCrossBlockCacheStorageSize(),
          metricsSystem);
    } else {
      return FlatDbCacheManager.NO_OP_CACHE;
    }
  }

  @Override
  public DataStorageFormat getDataStorageFormat() {
    return dataStorageFormat;
  }

  public FlatDbMode getFlatDbMode() {
    return flatDbStrategyProvider.getFlatDbMode();
  }

  public Optional<Bytes> getAccount(final Hash accountHash) {
    return cacheManager.getFromCacheOrStorage(
        ACCOUNT_INFO_STATE,
        accountHash.getBytes(),
        getCurrentVersion(),
        () ->
            getFlatDbStrategy()
                .getFlatAccount(
                    this::getActiveWorldStateRootHash,
                    this::getTrieNode,
                    accountHash,
                    composedWorldStateStorage));
  }

  public Optional<Bytes> getStorageValueByStorageSlotKey(
      final Hash accountHash, final StorageSlotKey storageSlotKey) {
    final Bytes key =
        Bytes.concatenate(accountHash.getBytes(), storageSlotKey.getSlotHash().getBytes());
    return cacheManager.getFromCacheOrStorage(
        ACCOUNT_STORAGE_STORAGE,
        key,
        getCurrentVersion(),
        () ->
            getFlatDbStrategy()
                .getFlatStorageValueByStorageSlotKey(
                    this::getActiveWorldStateRootHash,
                    () -> getAccount(accountHash),
                    this::getTrieNode,
                    accountHash,
                    storageSlotKey,
                    composedWorldStateStorage));
  }

  public Optional<Bytes> getCode(final Hash codeHash, final Hash accountHash) {
    if (codeHash.equals(Hash.EMPTY)) {
      return Optional.of(Bytes.EMPTY);
    }
    return getFlatDbStrategy().getFlatCode(codeHash, accountHash, composedWorldStateStorage);
  }

  public Optional<Bytes> getTrieNode(
      final TrieBranchType trieBranchType, final Bytes location, final Bytes32 nodeHash) {
    if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
    }
    return composedWorldStateStorage
        .get(TrieBranchSegments.segmentFor(trieBranchType), location.toArrayUnsafe())
        .map(Bytes::wrap);
  }

  public Optional<Bytes> getTrieNode(final Bytes location, final Bytes32 nodeHash) {
    return getTrieNode(TrieBranchType.PATRICIA, location, nodeHash);
  }

  public Optional<Bytes> getTrieNode(final TrieBranchType trieBranchType, final Bytes key) {
    return composedWorldStateStorage
        .get(TrieBranchSegments.segmentFor(trieBranchType), key.toArrayUnsafe())
        .map(Bytes::wrap);
  }

  public Optional<Bytes> getTrieNode(final Bytes key) {
    return getTrieNode(TrieBranchType.PATRICIA, key);
  }

  public NavigableMap<Bytes32, AccountStorageEntry> storageEntriesFrom(
      final Hash addressHash, final Bytes32 startKeyHash, final int limit) {
    throw new RuntimeException("Bonsai Tries does not currently support enumerating storage");
  }

  public void upgradeToFullFlatDbMode() {
    flatDbStrategyProvider.upgradeToFullFlatDbMode(composedWorldStateStorage);
    cacheManager.clear(ACCOUNT_INFO_STATE);
    cacheManager.clear(ACCOUNT_STORAGE_STORAGE);
  }

  public void upgradeToArchiveFlatDbMode() {
    flatDbStrategyProvider.upgradeToArchiveFlatDbMode(composedWorldStateStorage);
    // Invalidate cached world state snapshots that were created under the previous strategy.
    // Snapshots share the flatDbStrategyProvider, so after the switch they would use the new
    // ARCHIVE read path against stale snapshot data that lacks complete archive-keyed entries.
    subscribers.forEach(
        subscriber -> {
          try {
            subscriber.onClearFlatDatabaseStorage();
          } catch (final Exception e) {
            LOG.error("Error notifying subscriber of flat database storage upgrade", e);
          }
        });
  }

  public void downgradeToPartialFlatDbMode() {
    flatDbStrategyProvider.downgradeToPartialFlatDbMode(composedWorldStateStorage);
    cacheManager.clear(ACCOUNT_INFO_STATE);
    cacheManager.clear(ACCOUNT_STORAGE_STORAGE);
  }

  @Override
  public void clear() {
    subscribers.forEach(StorageSubscriber::onClearStorage);
    getFlatDbStrategy().clearAll(composedWorldStateStorage);
    composedWorldStateStorage.clear(PATRICIA_TRIE_BRANCH_STORAGE);
    composedWorldStateStorage.clear(BINARY_TRIE_BRANCH_STORAGE);
    trieLogStorage.clear();
    cacheManager.clear(ACCOUNT_INFO_STATE);
    cacheManager.clear(ACCOUNT_STORAGE_STORAGE);
    flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);
  }

  public void clearFlatDatabase() {
    subscribers.forEach(StorageSubscriber::onClearFlatDatabaseStorage);
    getFlatDbStrategy().resetOnResync(composedWorldStateStorage);
    cacheManager.clear(ACCOUNT_INFO_STATE);
    cacheManager.clear(ACCOUNT_STORAGE_STORAGE);
  }

  public BonsaiFlatDbStrategy getFlatDbStrategy() {
    return (BonsaiFlatDbStrategy)
        flatDbStrategyProvider.getFlatDbStrategy(composedWorldStateStorage);
  }

  @Override
  public Updater updater() {
    return new CachedUpdater(
        composedWorldStateStorage.startTransaction(),
        trieLogStorage.startTransaction(),
        getFlatDbStrategy(),
        composedWorldStateStorage);
  }

  public long getCacheSize(final SegmentIdentifier segment) {
    return cacheManager.getCacheSize(segment);
  }

  public boolean isCached(final SegmentIdentifier segment, final Bytes key) {
    return cacheManager.isCached(segment, key);
  }

  public Optional<FlatDbCacheManager.VersionedValue> getCachedValue(
      final SegmentIdentifier segment, final Bytes key) {
    return cacheManager.getCachedValue(segment, key);
  }

  public FlatDbCacheManager getCacheManager() {
    return cacheManager;
  }

  public long getCurrentVersion() {
    return cacheVersion;
  }

  public SegmentedKeyValueStorage getComposedWorldStateStorage() {
    return composedWorldStateStorage;
  }

  public KeyValueStorage getTrieLogStorage() {
    return trieLogStorage;
  }

  public Optional<byte[]> getTrieLog(final Hash blockHash) {
    return trieLogStorage.get(blockHash.getBytes().toArrayUnsafe());
  }

  public Stream<byte[]> streamTrieLogKeys(final long limit) {
    return trieLogStorage.streamKeys().limit(limit);
  }

  public Optional<Bytes> getWorldStateRootHash() {
    return getWorldStateRootHash(TrieBranchType.PATRICIA);
  }

  public Optional<Bytes> getWorldStateRootHash(final TrieBranchType trieBranchType) {
    return composedWorldStateStorage
        .get(TrieBranchSegments.segmentFor(trieBranchType), WORLD_ROOT_HASH_KEY)
        .map(Bytes::wrap);
  }

  /** Root hash from whichever trie branch column currently holds world metadata. */
  public Optional<Bytes> getActiveWorldStateRootHash() {
    return getWorldStateRootHash(resolveActiveTrieBranchType());
  }

  /** Returns which trie branch column holds the persisted world root hash. */
  public TrieBranchType resolveActiveTrieBranchType() {
    if (composedWorldStateStorage
        .get(BINARY_TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY)
        .isPresent()) {
      return TrieBranchType.BINARY;
    }
    if (composedWorldStateStorage
        .get(PATRICIA_TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY)
        .isPresent()) {
      return TrieBranchType.PATRICIA;
    }
    return TrieBranchType.PATRICIA;
  }

  public Optional<Hash> getWorldStateBlockHash() {
    return getWorldStateBlockHash(TrieBranchType.PATRICIA);
  }

  public Optional<Hash> getWorldStateBlockHash(final TrieBranchType trieBranchType) {
    return composedWorldStateStorage
        .get(TrieBranchSegments.segmentFor(trieBranchType), WORLD_BLOCK_HASH_KEY)
        .map(Bytes32::wrap)
        .map(Hash::wrap);
  }

  public Optional<Long> getWorldStateBlockNumber() {
    return getWorldStateBlockNumber(TrieBranchType.PATRICIA);
  }

  public Optional<Long> getWorldStateBlockNumber(final TrieBranchType trieBranchType) {
    return composedWorldStateStorage
        .get(TrieBranchSegments.segmentFor(trieBranchType), WORLD_BLOCK_NUMBER_KEY)
        .map(bytes -> Bytes.wrap(bytes).toLong());
  }

  public NavigableMap<Bytes32, Bytes> streamFlatAccounts(
      final Bytes startKeyHash, final Bytes32 endKeyHash, final long max) {
    return getFlatDbStrategy()
        .streamAccountFlatDatabase(composedWorldStateStorage, startKeyHash, endKeyHash, max);
  }

  public NavigableMap<Bytes32, Bytes> streamFlatAccounts(
      final Bytes startKeyHash, final Predicate<Pair<Bytes32, Bytes>> takeWhile) {
    return getFlatDbStrategy()
        .streamAccountFlatDatabase(composedWorldStateStorage, startKeyHash, takeWhile);
  }

  public NavigableMap<Bytes32, Bytes> streamFlatStorages(
      final Hash accountHash, final Bytes startKeyHash, final Bytes32 endKeyHash, final long max) {
    return getFlatDbStrategy()
        .streamStorageFlatDatabase(
            composedWorldStateStorage, accountHash, startKeyHash, endKeyHash, max);
  }

  public NavigableMap<Bytes32, Bytes> streamFlatStorages(
      final Hash accountHash,
      final Bytes startKeyHash,
      final Predicate<Pair<Bytes32, Bytes>> takeWhile) {
    return getFlatDbStrategy()
        .streamStorageFlatDatabase(composedWorldStateStorage, accountHash, startKeyHash, takeWhile);
  }

  public boolean isWorldStateAvailable(final Bytes32 rootHash, final Hash blockHash) {
    return isWorldStateAvailable(TrieBranchType.PATRICIA, rootHash, blockHash)
        || isWorldStateAvailable(TrieBranchType.BINARY, rootHash, blockHash);
  }

  public boolean isWorldStateAvailable(
      final TrieBranchType trieBranchType, final Bytes32 rootHash, final Hash blockHash) {
    return composedWorldStateStorage
        .get(TrieBranchSegments.segmentFor(trieBranchType), WORLD_ROOT_HASH_KEY)
        .map(Bytes32::wrap)
        .map(
            hash ->
                hash.equals(rootHash)
                    || trieLogStorage.containsKey(blockHash.getBytes().toArrayUnsafe()))
        .orElse(false);
  }

  public void clearTrieLog() {
    subscribers.forEach(StorageSubscriber::onClearTrieLog);
    trieLogStorage.clear();
  }

  public void clearTrie() {
    subscribers.forEach(StorageSubscriber::onClearTrie);
    composedWorldStateStorage.clear(PATRICIA_TRIE_BRANCH_STORAGE);
    composedWorldStateStorage.clear(BINARY_TRIE_BRANCH_STORAGE);
  }

  public boolean pruneTrieLog(final Hash blockHash) {
    try {
      return trieLogStorage.tryDelete(blockHash.getBytes().toArrayUnsafe());
    } catch (Exception e) {
      LOG.error("Error pruning trie log for block hash {}", blockHash, e);
      return false;
    }
  }

  @Override
  public synchronized void close() throws Exception {
    // when the storage clears, close
    shouldClose.set(true);
    tryClose();
  }

  public synchronized long subscribe(final StorageSubscriber sub) {
    if (isClosed.get()) {
      throw new RuntimeException("Storage is marked to close or has already closed");
    }
    return subscribers.subscribe(sub);
  }

  public synchronized void unSubscribe(final long id) {
    subscribers.unsubscribe(id);
    try {
      tryClose();
    } catch (Exception e) {
      LOG.atWarn()
          .setMessage("exception while trying to close : {}")
          .addArgument(e::getMessage)
          .log();
    }
  }

  protected synchronized void tryClose() throws Exception {
    if (shouldClose.get() && subscribers.getSubscriberCount() < 1) {
      doClose();
    }
  }

  protected synchronized void doClose() throws Exception {
    if (!isClosed.get()) {
      // alert any subscribers we are closing:
      subscribers.forEach(StorageSubscriber::onCloseStorage);

      // close all of the KeyValueStorages:
      composedWorldStateStorage.close();
      trieLogStorage.close();

      // set storage closed
      isClosed.set(true);
    }
  }

  /** Base updater that writes directly to storage without cache management. */
  public static class Updater implements WorldStateKeyValueStorage.Updater {

    protected final SegmentedKeyValueStorageTransaction composedWorldStateTransaction;
    protected final KeyValueStorageTransaction trieLogStorageTransaction;
    protected final FlatDbStrategy flatDbStrategy;
    protected final SegmentedKeyValueStorage worldStorage;

    public Updater(
        final SegmentedKeyValueStorageTransaction composedWorldStateTransaction,
        final KeyValueStorageTransaction trieLogStorageTransaction,
        final FlatDbStrategy flatDbStrategy,
        final SegmentedKeyValueStorage worldStorage) {

      this.composedWorldStateTransaction = composedWorldStateTransaction;
      this.trieLogStorageTransaction = trieLogStorageTransaction;
      this.flatDbStrategy = flatDbStrategy;
      this.worldStorage = worldStorage;
    }

    public Updater removeCode(final Hash accountHash, final Hash codeHash) {
      flatDbStrategy.removeFlatCode(
          worldStorage, composedWorldStateTransaction, accountHash, codeHash);
      return this;
    }

    public Updater putCode(final Hash accountHash, final Bytes code) {
      final Hash codeHash = code.size() == 0 ? Hash.EMPTY : Hash.hash(code);
      return putCode(accountHash, codeHash, code);
    }

    public Updater putCode(final Hash accountHash, final Hash codeHash, final Bytes code) {
      if (code.isEmpty()) {
        return this;
      }
      flatDbStrategy.putFlatCode(
          worldStorage, composedWorldStateTransaction, accountHash, codeHash, code);
      return this;
    }

    public Updater removeAccountInfoState(final Hash accountHash) {
      flatDbStrategy.removeFlatAccount(worldStorage, composedWorldStateTransaction, accountHash);
      return this;
    }

    public Updater putAccountInfoState(final Hash accountHash, final Bytes accountValue) {
      if (accountValue.isEmpty()) {
        return this;
      }
      flatDbStrategy.putFlatAccount(
          worldStorage, composedWorldStateTransaction, accountHash, accountValue);
      return this;
    }

    public Updater saveWorldState(final Bytes blockHash, final Bytes32 nodeHash, final Bytes node) {
      return saveWorldState(TrieBranchType.PATRICIA, blockHash, nodeHash, node);
    }

    public Updater saveWorldState(
        final TrieBranchType trieBranchType,
        final Bytes blockHash,
        final Bytes32 nodeHash,
        final Bytes node) {
      final SegmentIdentifier trieBranchSegment = TrieBranchSegments.segmentFor(trieBranchType);
      composedWorldStateTransaction.put(
          trieBranchSegment, Bytes.EMPTY.toArrayUnsafe(), node.toArrayUnsafe());
      composedWorldStateTransaction.put(
          trieBranchSegment, WORLD_ROOT_HASH_KEY, nodeHash.toArrayUnsafe());
      composedWorldStateTransaction.put(
          trieBranchSegment, WORLD_BLOCK_HASH_KEY, blockHash.toArrayUnsafe());
      return this;
    }

    /**
     * Writes a trie (branch) node to the segment for {@code trieBranchType}, keyed by {@code
     * location}. Callers are responsible for any key prefixing (see {@link #getTrieNode}).
     */
    public synchronized Updater putTrieNode(
        final TrieBranchType trieBranchType,
        final Bytes location,
        final Bytes32 nodeHash,
        final Bytes node) {
      if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        return this;
      }
      composedWorldStateTransaction.put(
          TrieBranchSegments.segmentFor(trieBranchType),
          location.toArrayUnsafe(),
          node.toArrayUnsafe());
      return this;
    }

    public synchronized Updater putTrieNode(
        final Bytes location, final Bytes32 nodeHash, final Bytes node) {
      return putTrieNode(TrieBranchType.PATRICIA, location, nodeHash, node);
    }

    /**
     * Removes a trie (branch) node from the segment for {@code trieBranchType}, keyed by {@code
     * location}. Callers are responsible for any key prefixing (see {@link #getTrieNode}).
     */
    public Updater removeTrieNode(final TrieBranchType trieBranchType, final Bytes location) {
      composedWorldStateTransaction.remove(
          TrieBranchSegments.segmentFor(trieBranchType), location.toArrayUnsafe());
      return this;
    }

    public Updater removeTrieNode(final Bytes location) {
      return removeTrieNode(TrieBranchType.PATRICIA, location);
    }

    public synchronized Updater putStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash, final Bytes storageValue) {
      flatDbStrategy.putFlatAccountStorageValueByStorageSlotHash(
          worldStorage, composedWorldStateTransaction, accountHash, slotHash, storageValue);
      return this;
    }

    public synchronized void removeStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash) {
      flatDbStrategy.removeFlatAccountStorageValueByStorageSlotHash(
          worldStorage, composedWorldStateTransaction, accountHash, slotHash);
    }

    public SegmentedKeyValueStorageTransaction getWorldStateTransaction() {
      return composedWorldStateTransaction;
    }

    public KeyValueStorageTransaction getTrieLogStorageTransaction() {
      return trieLogStorageTransaction;
    }

    @Override
    public void commit() {
      trieLogStorageTransaction.commit();
      composedWorldStateTransaction.commit();
    }

    public void commitTrieLogOnly() {
      trieLogStorageTransaction.commit();
      composedWorldStateTransaction.close();
    }

    public void commitComposedOnly() {
      composedWorldStateTransaction.commit();
      trieLogStorageTransaction.close();
    }

    public void rollback() {
      composedWorldStateTransaction.rollback();
      trieLogStorageTransaction.rollback();
    }
  }

  /**
   * Cached updater that stages changes and refreshes the cache only after a successful storage
   * commit ({@code updateCache()} is not run if {@code super.commit()} fails). Used only by base
   * storage (not snapshots or layers).
   */
  public class CachedUpdater extends Updater {

    private static final int INITIAL_CACHE_MAP_CAPACITY = 1024;

    /** Single map per segment. Value {@code null} encodes a staged removal (last-write-wins). */
    private final Map<SegmentIdentifier, Map<Bytes, Bytes>> pending = new HashMap<>();

    public CachedUpdater(
        final SegmentedKeyValueStorageTransaction composedWorldStateTransaction,
        final KeyValueStorageTransaction trieLogStorageTransaction,
        final FlatDbStrategy flatDbStrategy,
        final SegmentedKeyValueStorage worldStorage) {
      super(composedWorldStateTransaction, trieLogStorageTransaction, flatDbStrategy, worldStorage);
    }

    @Override
    public Updater putAccountInfoState(final Hash accountHash, final Bytes accountValue) {
      if (!accountValue.isEmpty()) {
        stagePut(ACCOUNT_INFO_STATE, accountHash.getBytes(), accountValue);
      }
      return super.putAccountInfoState(accountHash, accountValue);
    }

    @Override
    public Updater removeAccountInfoState(final Hash accountHash) {
      stageRemoval(ACCOUNT_INFO_STATE, accountHash.getBytes());
      return super.removeAccountInfoState(accountHash);
    }

    @Override
    public synchronized Updater putStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash, final Bytes storageValue) {
      stagePut(
          ACCOUNT_STORAGE_STORAGE,
          Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()),
          storageValue);
      return super.putStorageValueBySlotHash(accountHash, slotHash, storageValue);
    }

    @Override
    public synchronized void removeStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash) {
      stageRemoval(
          ACCOUNT_STORAGE_STORAGE, Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()));
      super.removeStorageValueBySlotHash(accountHash, slotHash);
    }

    private void stagePut(final SegmentIdentifier segment, final Bytes key, final Bytes value) {
      pending
          .computeIfAbsent(segment, s -> new HashMap<>(INITIAL_CACHE_MAP_CAPACITY))
          .put(key, value);
    }

    private void stageRemoval(final SegmentIdentifier segment, final Bytes key) {
      pending
          .computeIfAbsent(segment, s -> new HashMap<>(INITIAL_CACHE_MAP_CAPACITY))
          .put(key, null);
    }

    private void clearStaged() {
      pending.clear();
    }

    protected void incrementCacheVersion() {
      cacheVersion = cacheManager.incrementAndGetVersion();
    }

    protected void updateCache() {
      pending.forEach(
          (segment, updates) ->
              updates.forEach(
                  (key, value) -> {
                    if (value == null) {
                      cacheManager.removeFromCache(segment, key, cacheVersion);
                    } else {
                      cacheManager.putInCache(segment, key, value, cacheVersion);
                    }
                  }));
      clearStaged();
      cacheManager.scheduleAsyncMaintenance();
    }

    @Override
    public void commit() {
      incrementCacheVersion();
      super.commit();
      updateCache();
    }

    @Override
    public void commitTrieLogOnly() {
      clearStaged();
      super.commitTrieLogOnly();
    }

    @Override
    public void commitComposedOnly() {
      incrementCacheVersion();
      super.commitComposedOnly();
      updateCache();
    }

    @Override
    public void rollback() {
      clearStaged();
      super.rollback();
    }
  }
}
