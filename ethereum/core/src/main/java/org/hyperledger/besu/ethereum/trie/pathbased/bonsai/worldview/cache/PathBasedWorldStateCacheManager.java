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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.StorageSubscriber;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.data.BlockHeader;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PathBasedWorldStateCacheManager implements StorageSubscriber {
  public static final long RETAINED_LAYERS = 512; // at least 256 + typical rollbacks
  private static final Logger LOG = LoggerFactory.getLogger(PathBasedWorldStateCacheManager.class);
  private final PathBasedWorldStateProvider archive;
  private final EvmConfiguration evmConfiguration;
  protected final WorldStateConfig worldStateConfig;
  private final Map<Hash, BlockHeader> stateRootToBlockHeaderCache = new ConcurrentHashMap<>();

  private final BonsaiWorldStateKeyValueStorage rootWorldStateStorage;
  private final Map<Hash, BonsaiCachedWorldStateView> cachedWorldStatesByHash;

  protected PathBasedWorldStateCacheManager(
      final PathBasedWorldStateProvider archive,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Map<Hash, BonsaiCachedWorldStateView> cachedWorldStatesByHash,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig) {
    worldStateKeyValueStorage.subscribe(this);
    this.rootWorldStateStorage = worldStateKeyValueStorage;
    this.cachedWorldStatesByHash = cachedWorldStatesByHash;
    this.archive = archive;
    this.evmConfiguration = evmConfiguration;
    this.worldStateConfig = worldStateConfig;
  }

  public synchronized void addCachedLayer(
      final BlockHeader blockHeader,
      final Hash worldStateRootHash,
      final PathBasedWorldState forWorldState) {
    final Optional<BonsaiCachedWorldStateView> cachedPathBasedWorldView =
        Optional.ofNullable(this.cachedWorldStatesByHash.get(blockHeader.getBlockHash()));
    if (cachedPathBasedWorldView.isPresent()) {
      // only replace if it is a layered storage
      if (forWorldState.isModifyingHeadWorldState()
          && cachedPathBasedWorldView.get().getWorldStateStorage()
              instanceof BonsaiWorldStateLayerStorage) {
        LOG.atDebug()
            .setMessage("updating layered world state for block {}, state root hash {}")
            .addArgument(blockHeader::toLogString)
            .addArgument(() -> worldStateRootHash.getBytes().toShortHexString())
            .log();
        cachedPathBasedWorldView
            .get()
            .updateWorldStateStorage(
                createSnapshotKeyValueStorage(forWorldState.getWorldStateStorage()));
      }
    } else {
      LOG.atDebug()
          .setMessage("adding layered world state for block {}, state root hash {}")
          .addArgument(blockHeader::toLogString)
          .addArgument(() -> worldStateRootHash.getBytes().toShortHexString())
          .log();
      if (forWorldState.isModifyingHeadWorldState()) {
        cachedWorldStatesByHash.put(
            blockHeader.getBlockHash(),
            new BonsaiCachedWorldStateView(
                blockHeader, createSnapshotKeyValueStorage(forWorldState.getWorldStateStorage())));
      } else {
        // otherwise, add the layer to the cache
        cachedWorldStatesByHash.put(
            blockHeader.getBlockHash(),
            new BonsaiCachedWorldStateView(
                blockHeader,
                ((BonsaiWorldStateLayerStorage) forWorldState.getWorldStateStorage()).clone()));
      }
      // add stateroot -> blockHeader cache entry
      stateRootToBlockHeaderCache.put(blockHeader.getStateRoot(), blockHeader);
    }
    scrubCachedLayers(blockHeader.getNumber());
  }

  private synchronized void scrubCachedLayers(final long newMaxHeight) {
    final long waterline = newMaxHeight - RETAINED_LAYERS;
    if (cachedWorldStatesByHash.size() > RETAINED_LAYERS) {
      cachedWorldStatesByHash.values().stream()
          .filter(layer -> layer.getBlockNumber() < waterline)
          .toList()
          .forEach(
              layer -> {
                cachedWorldStatesByHash.remove(layer.getBlockHash());
                layer.close();
              });
    }
    stateRootToBlockHeaderCache.values().removeIf(h -> h.getNumber() < waterline);
  }

  public Optional<PathBasedWorldState> getWorldState(final Hash blockHash) {
    if (cachedWorldStatesByHash.containsKey(blockHash)) {
      // return a new worldstate using worldstate storage and an isolated copy of the updater
      return Optional.ofNullable(cachedWorldStatesByHash.get(blockHash))
          .map(
              cached ->
                  createWorldState(
                      archive,
                      createLayeredKeyValueStorage(cached.getWorldStateStorage()),
                      evmConfiguration));
    }
    LOG.atDebug()
        .setMessage("did not find worldstate in cache for {}")
        .addArgument(blockHash.getBytes().toShortHexString())
        .log();

    return Optional.empty();
  }

  public Optional<PathBasedWorldState> getNearestWorldState(final BlockHeader blockHeader) {
    LOG.atDebug()
        .setMessage("getting nearest worldstate for {}")
        .addArgument(blockHeader::toLogString)
        .log();

    return Optional.ofNullable(
            cachedWorldStatesByHash.get(blockHeader.getParentHash())) // search parent block
        .map(BonsaiCachedWorldStateView::getWorldStateStorage)
        .or(
            () -> {
              // or else search the nearest state in the cache
              LOG.atDebug()
                  .setMessage("searching cache for nearest worldstate for {}")
                  .addArgument(blockHeader::toLogString)
                  .log();

              final List<BonsaiCachedWorldStateView> cachedPathBasedWorldViews =
                  new ArrayList<>(cachedWorldStatesByHash.values());
              return cachedPathBasedWorldViews.stream()
                  .sorted(
                      Comparator.comparingLong(
                          view -> Math.abs(blockHeader.getNumber() - view.getBlockNumber())))
                  .map(BonsaiCachedWorldStateView::getWorldStateStorage)
                  .findFirst();
            })
        .map(
            storage ->
                createWorldState( // wrap the state in a layered worldstate
                    archive, createLayeredKeyValueStorage(storage), evmConfiguration));
  }

  public Optional<PathBasedWorldState> getHeadWorldState(
      final Function<Hash, Optional<BlockHeader>> hashBlockHeaderFunction) {

    LOG.atDebug().setMessage("getting head worldstate").log();

    // the block hash is read from the snapshot, so a head persist in between cannot pair the
    // snapshot with another block
    final BonsaiWorldStateKeyValueStorage headSnapshot =
        createSnapshotKeyValueStorage(rootWorldStateStorage);
    final Optional<BlockHeader> maybeHeadHeader;
    try {
      maybeHeadHeader = headSnapshot.getWorldStateBlockHash().flatMap(hashBlockHeaderFunction);
    } catch (final RuntimeException e) {
      closeSnapshot(headSnapshot);
      throw e;
    }
    if (maybeHeadHeader.isEmpty()) {
      closeSnapshot(headSnapshot);
      return Optional.empty();
    }
    final BlockHeader blockHeader = maybeHeadHeader.get();
    addCachedHeadSnapshot(blockHeader, headSnapshot);
    return getWorldState(blockHeader.getBlockHash());
  }

  private synchronized void addCachedHeadSnapshot(
      final BlockHeader blockHeader, final BonsaiWorldStateKeyValueStorage headSnapshot) {
    final BonsaiCachedWorldStateView cachedView =
        cachedWorldStatesByHash.get(blockHeader.getBlockHash());
    if (cachedView == null) {
      cachedWorldStatesByHash.put(
          blockHeader.getBlockHash(), new BonsaiCachedWorldStateView(blockHeader, headSnapshot));
      stateRootToBlockHeaderCache.put(blockHeader.getStateRoot(), blockHeader);
    } else if (cachedView.getWorldStateStorage() instanceof BonsaiWorldStateLayerStorage) {
      cachedView.updateWorldStateStorage(headSnapshot);
    } else {
      closeSnapshot(headSnapshot);
    }
    scrubCachedLayers(blockHeader.getNumber());
  }

  private static void closeSnapshot(final BonsaiWorldStateKeyValueStorage snapshot) {
    try {
      snapshot.close();
    } catch (final Exception e) {
      LOG.warn("Failed to close head world state snapshot", e);
    }
  }

  public boolean contains(final Hash blockHash) {
    return cachedWorldStatesByHash.containsKey(blockHash);
  }

  public void reset() {
    // each view holds a storage snapshot, closed once the world states using it are closed
    this.cachedWorldStatesByHash.values().forEach(BonsaiCachedWorldStateView::close);
    this.cachedWorldStatesByHash.clear();
    this.stateRootToBlockHeaderCache.clear();
  }

  /**
   * Returns the storage of a cached world state by its root hash. It takes no lock and loads
   * nothing, so a caller never holds back the blocks being imported. The storage can be closed once
   * the cache drops it: a reader subscribes to it to keep it open.
   *
   * @param rootHash the state root of the world state
   * @return the storage, empty when no cached world state has this root
   */
  public Optional<BonsaiWorldStateKeyValueStorage> getCachedStorageByRootHash(final Hash rootHash) {
    return Optional.ofNullable(stateRootToBlockHeaderCache.get(rootHash))
        .map(header -> cachedWorldStatesByHash.get(header.getBlockHash()))
        .map(BonsaiCachedWorldStateView::getWorldStateStorage);
  }

  @Override
  public void onClearStorage() {
    this.cachedWorldStatesByHash.clear();
    this.stateRootToBlockHeaderCache.clear();
  }

  @Override
  public void onClearFlatDatabaseStorage() {
    this.cachedWorldStatesByHash.clear();
    this.stateRootToBlockHeaderCache.clear();
  }

  @Override
  public void onClearTrieLog() {
    this.cachedWorldStatesByHash.clear();
    this.stateRootToBlockHeaderCache.clear();
  }

  @Override
  public void onClearTrie() {
    this.cachedWorldStatesByHash.clear();
    this.stateRootToBlockHeaderCache.clear();
  }

  @Override
  public void onCloseStorage() {
    this.cachedWorldStatesByHash.clear();
    this.stateRootToBlockHeaderCache.clear();
  }

  public abstract PathBasedWorldState createWorldState(
      final PathBasedWorldStateProvider archive,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration);

  public abstract BonsaiWorldStateKeyValueStorage createLayeredKeyValueStorage(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage);

  public abstract BonsaiWorldStateKeyValueStorage createSnapshotKeyValueStorage(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage);
}
