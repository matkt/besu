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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.BonsaiHeadLayerManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.BonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiWorldStateProvider extends PathBasedWorldStateProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiWorldStateProvider.class);

  private final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader;
  private final Optional<Long> amsterdamMilestone;
  private final boolean layeredHeadEnabled;
  private final BonsaiHeadLayerManager headLayerManager;

  public BonsaiWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final PathBasedExtraStorageConfiguration pathBasedExtraStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final PathBasedCodeCache codeCache) {
    this(
        worldStateKeyValueStorage,
        blockchain,
        pathBasedExtraStorageConfiguration,
        bonsaiCachedMerkleTrieLoader,
        pluginContext,
        evmConfiguration,
        codeCache,
        Optional.empty());
  }

  public BonsaiWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final PathBasedExtraStorageConfiguration pathBasedExtraStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final PathBasedCodeCache codeCache,
      final Optional<Long> amsterdamMilestone) {
    super(worldStateKeyValueStorage, blockchain, pathBasedExtraStorageConfiguration, pluginContext);
    this.bonsaiCachedMerkleTrieLoader = bonsaiCachedMerkleTrieLoader;
    this.amsterdamMilestone = amsterdamMilestone;
    this.evmConfiguration = evmConfiguration;
    this.layeredHeadEnabled =
        pathBasedExtraStorageConfiguration.getUnstable().getBonsaiLayeredHeadEnabled();
    this.headLayerManager =
        layeredHeadEnabled
            ? new BonsaiHeadLayerManager(
                worldStateKeyValueStorage,
                pathBasedExtraStorageConfiguration
                    .getUnstable()
                    .getBonsaiLayeredHeadCheckpointInterval(),
                pathBasedExtraStorageConfiguration
                    .getUnstable()
                    .getBonsaiLayeredHeadMemoryBudgetBytes())
            : null;
    provideWorldStateCacheManager(
        new BonsaiWorldStateCacheManager(
            this, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache));
    initializeHeadWorldState(
        new BonsaiWorldState(
            this, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache));
    if (headLayerManager != null) {
      headLayerManager.setOnCheckpoint(this::onLayeredHeadCheckpoint);
      recoverLayeredHeadIfNeeded();
    }
  }

  @VisibleForTesting
  BonsaiWorldStateProvider(
      final BonsaiWorldStateCacheManager bonsaiWorldStateCacheManager,
      final PathBasedExtraStorageConfiguration pathBasedExtraStorageConfiguration,
      final TrieLogManager trieLogManager,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final EvmConfiguration evmConfiguration,
      final PathBasedCodeCache codeCache) {
    super(
        worldStateKeyValueStorage, blockchain, pathBasedExtraStorageConfiguration, trieLogManager);
    this.bonsaiCachedMerkleTrieLoader = bonsaiCachedMerkleTrieLoader;
    this.amsterdamMilestone = Optional.empty();
    this.evmConfiguration = evmConfiguration;
    this.layeredHeadEnabled =
        pathBasedExtraStorageConfiguration.getUnstable().getBonsaiLayeredHeadEnabled();
    this.headLayerManager =
        layeredHeadEnabled
            ? new BonsaiHeadLayerManager(
                worldStateKeyValueStorage,
                pathBasedExtraStorageConfiguration
                    .getUnstable()
                    .getBonsaiLayeredHeadCheckpointInterval(),
                pathBasedExtraStorageConfiguration
                    .getUnstable()
                    .getBonsaiLayeredHeadMemoryBudgetBytes())
            : null;
    provideWorldStateCacheManager(bonsaiWorldStateCacheManager);
    initializeHeadWorldState(
        new BonsaiWorldState(
            this, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache));
    if (headLayerManager != null) {
      headLayerManager.setOnCheckpoint(this::onLayeredHeadCheckpoint);
      recoverLayeredHeadIfNeeded();
    }
  }

  public BonsaiCachedMerkleTrieLoader getCachedMerkleTrieLoader() {
    return bonsaiCachedMerkleTrieLoader;
  }

  public Optional<BonsaiHeadLayerManager> getHeadLayerManager() {
    return Optional.ofNullable(headLayerManager);
  }

  public void registerPayloadLayerCandidate(
      final BlockHeader blockHeader, final BonsaiWorldStateLayerStorage layerStorage) {
    if (headLayerManager != null) {
      headLayerManager.registerCandidate(blockHeader, layerStorage);
    }
  }

  @Override
  public boolean isLayeredHeadEnabled() {
    return layeredHeadEnabled;
  }

  @Override
  public Optional<Long> getLayeredHeadCheckpointNumber() {
    return headLayerManager == null
        ? Optional.empty()
        : Optional.of(headLayerManager.getCheckpointNumber());
  }

  @Override
  public boolean promoteCachedWorldState(final BlockHeader blockHeader) {
    if (headLayerManager == null) {
      return false;
    }
    final Optional<BonsaiWorldStateLayerStorage> promoted = headLayerManager.promote(blockHeader);
    if (promoted.isEmpty()) {
      return false;
    }
    // If a checkpoint just flushed the window, head should read from durable root storage.
    if (headLayerManager.getCheckpointHash().equals(blockHeader.getBlockHash())
        && headLayerManager.getCanonicalWindowDepth() == 0) {
      headWorldState.replaceWorldStateStorage(worldStateKeyValueStorage, blockHeader);
    } else {
      final BonsaiWorldStateKeyValueStorage root =
          (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage;
      // Candidates are already reparented at registration; reparentOnto is a cheap clone then.
      headWorldState.replaceWorldStateStorage(promoted.get().reparentOnto(root), blockHeader);
    }
    worldStateCacheManager.addCachedLayer(
        blockHeader, blockHeader.getStateRoot(), headWorldState);
    LOG.debug("Layered-head promotion succeeded for {}", blockHeader.toLogString());
    return true;
  }

  private void onLayeredHeadCheckpoint(final BlockHeader checkpointHeader) {
    headWorldState.replaceWorldStateStorage(worldStateKeyValueStorage, checkpointHeader);
    worldStateCacheManager.addCachedLayer(
        checkpointHeader, checkpointHeader.getStateRoot(), headWorldState);
  }

  private void initializeHeadWorldState(final BonsaiWorldState headWorldState) {
    blockchain
        .getBlockHeader(headWorldState.getWorldStateBlockHash())
        .ifPresentOrElse(
            header -> loadHeadWorldState(header, headWorldState),
            () -> {
              this.headWorldState = headWorldState;
              this.headWorldState.markAsHeadWorldState();
            });
  }

  /**
   * After restart, RocksDB holds the last checkpoint. Replay canonical trie logs from that
   * checkpoint up to the blockchain head so the in-memory layered head matches chain head.
   */
  private void recoverLayeredHeadIfNeeded() {
    final Optional<Hash> checkpointHash =
        worldStateKeyValueStorage.getWorldStateCheckpointHash()
            .or(worldStateKeyValueStorage::getWorldStateBlockHash);
    final long checkpointNumber =
        worldStateKeyValueStorage
            .getWorldStateCheckpointNumber()
            .or(() -> worldStateKeyValueStorage.getWorldStateBlockNumber())
            .orElse(0L);
    final BlockHeader chainHead = blockchain.getChainHeadHeader();
    if (checkpointHash.isEmpty()
        || chainHead.getBlockHash().equals(checkpointHash.get())
        || chainHead.getNumber() <= checkpointNumber) {
      if (headLayerManager != null && checkpointHash.isPresent()) {
        // Align manager metadata with durable checkpoint.
        LOG.info(
            "Layered-head recovery: durable checkpoint already at chain head {}",
            chainHead.toLogString());
      }
      return;
    }

    LOG.info(
        "Layered-head recovery: replaying trie logs from checkpoint #{} ({}) to chain head {}",
        checkpointNumber,
        checkpointHash.get().toHexString(),
        chainHead.toLogString());

    final List<TrieLog> rollForwards = new ArrayList<>();
    BlockHeader cursor = chainHead;
    while (cursor.getNumber() > checkpointNumber
        && !cursor.getBlockHash().equals(checkpointHash.get())) {
      final Optional<TrieLog> trieLog = trieLogManager.getTrieLogLayer(cursor.getBlockHash());
      if (trieLog.isEmpty()) {
        LOG.error(
            "Layered-head recovery failed: missing trie log for {}. Durable checkpoint remains at #{}. Resync may be required.",
            cursor.toLogString(),
            checkpointNumber);
        return;
      }
      rollForwards.add(trieLog.get());
      final Hash parentHash = cursor.getParentHash();
      cursor =
          blockchain
              .getBlockHeader(parentHash)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Missing parent during layered-head recovery for " + parentHash));
    }
    Collections.reverse(rollForwards);

    // Rebuild layered head by rolling from checkpoint storage.
    headWorldState.resetWorldStateTo(
        blockchain
            .getBlockHeader(checkpointHash.get())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Missing checkpoint header " + checkpointHash.get())));
    final PathBasedWorldStateUpdateAccumulator<?> updater =
        (PathBasedWorldStateUpdateAccumulator<?>) headWorldState.updater();
    try {
      for (final TrieLog forward : rollForwards) {
        updater.rollForward(forward);
      }
      updater.commit();
      // Persist through layered path: apply to head (root RocksDB for recovered window is
      // acceptable as a one-shot rebuild; subsequent FCUs use promotion again).
      headWorldState.persist(chainHead);
      LOG.info(
          "Layered-head recovery complete at {}", chainHead.toLogString());
    } catch (final Exception e) {
      updater.reset();
      LOG.error(
          "Layered-head recovery failed while replaying to {}; durable checkpoint retained",
          chainHead.toLogString(),
          e);
    }
  }

  @Override
  protected void loadHeadWorldState(
      final BlockHeader blockHeader, final PathBasedWorldState headWorldState) {
    super.loadHeadWorldState(blockHeader, headWorldState);
    prepareWorldStateForBlock(blockHeader, headWorldState);
  }

  @Override
  public void prepareWorldStateForBlock(
      final BlockHeader blockHeader, final MutableWorldState worldState) {
    if (isAmsterdamActive(blockHeader)) {
      if (worldState instanceof BonsaiWorldState bonsaiWorldState) {
        bonsaiWorldState.disableCacheMerkleTrieLoader();
      }
    }
  }

  private boolean isAmsterdamActive(final BlockHeader blockHeader) {
    return amsterdamMilestone
        .map(milestone -> Long.compareUnsigned(blockHeader.getTimestamp(), milestone) >= 0)
        .orElse(false);
  }
}
