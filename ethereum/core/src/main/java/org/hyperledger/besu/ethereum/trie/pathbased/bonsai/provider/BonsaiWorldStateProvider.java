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

import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BinaryTrieForkSupport;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.MigrationScopedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.BonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldState;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiWorldStateProvider implements WorldStateArchive {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiWorldStateProvider.class);

  protected final Blockchain blockchain;

  protected final TrieLogManager trieLogManager;
  protected BonsaiWorldStateCacheManager worldStateCacheManager;
  protected BonsaiWorldState headWorldState;
  protected final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage;
  protected EvmConfiguration evmConfiguration;
  // Configuration that will be shared by all instances of world state at their creation
  protected final WorldStateConfig worldStateConfig;

  private final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader;
  private final Supplier<WorldStateHealer> worldStateHealerSupplier;

  /** Amsterdam fork milestone: BAL execution rules and {@link #prepareWorldStateForBlock} only. */
  private final Optional<Long> amsterdamMilestone;

  /**
   * Binary-trie fork milestone: trie branch selection and trie-log encoding (independent of
   * Amsterdam).
   */
  private final Optional<Long> binaryTrieMilestone;

  public BonsaiWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final PathBasedExtraStorageConfiguration pathBasedExtraStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final Supplier<WorldStateHealer> worldStateHealerSupplier,
      final BonsaiCodeCache codeCache) {
    this(
        worldStateKeyValueStorage,
        blockchain,
        pathBasedExtraStorageConfiguration,
        bonsaiCachedMerkleTrieLoader,
        pluginContext,
        evmConfiguration,
        worldStateHealerSupplier,
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
      final Supplier<WorldStateHealer> worldStateHealerSupplier,
      final BonsaiCodeCache codeCache,
      final Optional<Long> amsterdamMilestone) {
    this(
        worldStateKeyValueStorage,
        blockchain,
        pathBasedExtraStorageConfiguration,
        bonsaiCachedMerkleTrieLoader,
        pluginContext,
        evmConfiguration,
        worldStateHealerSupplier,
        codeCache,
        amsterdamMilestone,
        Optional.empty());
  }

  public BonsaiWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final PathBasedExtraStorageConfiguration pathBasedExtraStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final Supplier<WorldStateHealer> worldStateHealerSupplier,
      final BonsaiCodeCache codeCache,
      final Optional<Long> amsterdamMilestone,
      final Optional<Long> binaryTrieMilestone) {
    this.worldStateKeyValueStorage = worldStateKeyValueStorage;
    this.trieLogManager =
        new TrieLogManager(
            blockchain,
            worldStateKeyValueStorage,
            pathBasedExtraStorageConfiguration.getMaxLayersToLoad(),
            pluginContext,
            binaryTrieMilestone);
    this.blockchain = blockchain;
    this.worldStateConfig =
        WorldStateConfig.newBuilder()
            .parallelStateRootComputationEnabled(
                pathBasedExtraStorageConfiguration.getParallelStateRootComputationEnabled())
            .build();
    this.bonsaiCachedMerkleTrieLoader = bonsaiCachedMerkleTrieLoader;
    this.worldStateHealerSupplier = worldStateHealerSupplier;
    this.amsterdamMilestone = amsterdamMilestone;
    this.binaryTrieMilestone = binaryTrieMilestone;
    this.evmConfiguration = evmConfiguration;
    provideWorldStateCacheManager(
        new BonsaiWorldStateCacheManager(
            this, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache));
    initializeHeadWorldState(
        new BonsaiWorldState(
            this,
            worldStateKeyValueStorage,
            evmConfiguration,
            worldStateConfig,
            codeCache,
            blockchain.getChainHeadHeader()));
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
      final Supplier<WorldStateHealer> worldStateHealerSupplier,
      final BonsaiCodeCache codeCache) {
    this.worldStateKeyValueStorage = worldStateKeyValueStorage;
    this.trieLogManager = trieLogManager;
    this.blockchain = blockchain;
    this.worldStateConfig =
        WorldStateConfig.newBuilder()
            .parallelStateRootComputationEnabled(
                pathBasedExtraStorageConfiguration.getParallelStateRootComputationEnabled())
            .build();
    this.bonsaiCachedMerkleTrieLoader = bonsaiCachedMerkleTrieLoader;
    this.worldStateHealerSupplier = worldStateHealerSupplier;
    this.amsterdamMilestone = Optional.empty();
    this.binaryTrieMilestone = Optional.empty();
    this.evmConfiguration = evmConfiguration;
    provideWorldStateCacheManager(bonsaiWorldStateCacheManager);
    initializeHeadWorldState(
        new BonsaiWorldState(
            this,
            worldStateKeyValueStorage,
            evmConfiguration,
            worldStateConfig,
            codeCache,
            blockchain.getChainHeadHeader()));
  }

  public BonsaiCachedMerkleTrieLoader getCachedMerkleTrieLoader() {
    return bonsaiCachedMerkleTrieLoader;
  }

  public Blockchain getBlockchain() {
    return blockchain;
  }

  protected void provideWorldStateCacheManager(
      final BonsaiWorldStateCacheManager worldStateCacheManager) {
    this.worldStateCacheManager = worldStateCacheManager;
  }

  @Override
  public Optional<WorldState> get(final Hash rootHash, final Hash blockHash) {
    return worldStateCacheManager
        .getWorldState(blockHash)
        .or(
            () -> {
              if (blockHash.equals(headWorldState.blockHash())) {
                return Optional.of(headWorldState);
              } else {
                return Optional.empty();
              }
            })
        .map(WorldState.class::cast);
  }

  @Override
  public boolean isWorldStateAvailable(final Hash rootHash, final Hash blockHash) {
    final TrieBranchType trieBranchType = headWorldState.getTrieBranchType();
    return worldStateCacheManager.contains(blockHash)
        || headWorldState.blockHash().equals(blockHash)
        || worldStateKeyValueStorage.isWorldStateAvailable(
            trieBranchType, Bytes32.wrap(rootHash.getBytes()), blockHash);
  }

  @Override
  public Optional<MutableWorldState> getWorldState(final WorldStateQueryParams queryParams) {
    if (worldStateConfig.isStateful()) {
      return getFullWorldState(queryParams);
    } else {
      throw new RuntimeException("stateless mode is not yet available");
    }
  }

  @Override
  public MutableWorldState getWorldState() {
    return headWorldState;
  }

  /**
   * Loads a world state for {@code queryParams}, selecting MPT vs PBT by the target block and (when
   * provided) the next-block timestamp:
   *
   * <ul>
   *   <li><b>Case 1 (pre-PBT):</b> roll/load in MPT; no format conversion.
   *   <li><b>Case 2 (transition):</b> parent pre-PBT, next block PBT — reset cache, roll in MPT,
   *       then switch the returned view to BINARY (binary root from migrator column when present).
   *   <li><b>Case 3 (post-PBT):</b> target is already PBT — ensure BINARY before rolling so persist
   *       verifies against the PBT header state root.
   * </ul>
   */
  protected Optional<MutableWorldState> getFullWorldState(final WorldStateQueryParams queryParams) {
    final BlockHeader targetHeader =
        Optional.ofNullable(queryParams.getBlockHeader())
            .orElseGet(() -> headerOrThrow(queryParams.getBlockHash()));
    final Optional<Long> nextTimestamp =
        queryParams.getTimeStamp().or(() -> Optional.of(targetHeader.getTimestamp()));
    final boolean isPbtTransition =
        BinaryTrieForkSupport.isBinaryTrieTransition(
            targetHeader.getTimestamp(), binaryTrieMilestone, nextTimestamp);
    final boolean targetIsPbt =
        BinaryTrieForkSupport.isBinaryTrieActive(targetHeader.getTimestamp(), binaryTrieMilestone);

    if (isPbtTransition) {
      // Case 2: drop cached MPT snapshots so we do not serve a stale pre-transition view.
      worldStateCacheManager.reset();
    }

    // Case 1 rolls in MPT; Case 2 (transition) and Case 3 roll in BINARY.
    final TrieBranchType rollBranchType =
        targetIsPbt || isPbtTransition ? TrieBranchType.BINARY : TrieBranchType.PATRICIA;
    return queryParams.shouldWorldStateUpdateHead()
        ? getFullWorldStateFromHead(queryParams.getBlockHash(), rollBranchType, isPbtTransition)
        : getFullWorldStateFromCache(
            targetHeader, queryParams.getBlockAccessListOverlay(), rollBranchType, isPbtTransition);
  }

  /**
   * Migration-driven world state retrieval. The migrator calls this with {@code isMigration ==
   * true} to reuse the provider's rolling/reorg mechanism (the same code path PMT uses) while
   * keeping every non-binary-trie column untouched.
   *
   * <p>The returned world state is backed by a {@link MigrationScopedWorldStateKeyValueStorage}
   * (writes scoped to the binary-trie branch column), its accumulator trusts trie-log priors (no
   * flat-DB prior reads — the flat DB is PMT's), and state-root verification is skipped for
   * pre-{@code binaryTime} blocks (whose headers carry a PMT root). The snapshot cache is neither
   * read nor written: migration builds PBT state asynchronously and a cached snapshot taken before
   * the binary trie is materialised would be stale.
   *
   * <p>Progress is durable: the binary-trie branch column records the last migrated block hash, so
   * successive calls seed from that point and roll forward incrementally.
   */
  public BonsaiWorldState getMigrationWorldState() {
    return new BonsaiWorldState(
        worldStateKeyValueStorage,
        new NoOpBonsaiCachedMerkleTrieLoader(),
        worldStateCacheManager,
        trieLogManager,
        evmConfiguration,
        worldStateConfig,
        worldStateCacheManager.getCodeCache(),
        TrieBranchType.BINARY);
  }

  private Optional<MutableWorldState> getFullWorldStateFromHead(
      final Hash blockHash,
      final TrieBranchType rollBranchType,
      final boolean skipStateRootVerificationOnRollPersist) {
    ensureTrieBranchType(headWorldState, rollBranchType);
    return rollFullWorldStateToBlockHash(
            headWorldState, blockHash, skipStateRootVerificationOnRollPersist)
        .map(MutableWorldState.class::cast);
  }

  private Optional<MutableWorldState> getFullWorldStateFromCache(
      final BlockHeader blockHeader,
      final Optional<BlockAccessListOverlay> maybeBlockAccessListOverlay,
      final TrieBranchType rollBranchType,
      final boolean skipStateRootVerificationOnRollPersist) {
    final BlockHeader chainHeadBlockHeader = blockchain.getChainHeadHeader();
    if (chainHeadBlockHeader.getNumber() - blockHeader.getNumber()
        >= trieLogManager.getMaxLayersToLoad()) {
      LOG.warn(
          "Exceeded the limit of historical blocks that can be loaded ({}). If you need to make older historical queries, configure your `--bonsai-historical-block-limit`.",
          trieLogManager.getMaxLayersToLoad());
      return Optional.empty();
    }
    return worldStateCacheManager
        .getWorldState(blockHeader.getBlockHash())
        .or(() -> worldStateCacheManager.getNearestWorldState(blockHeader))
        .or(
            () ->
                worldStateCacheManager.getHeadWorldState(
                    blockHeaderHash ->
                        blockchain.getBlockHeader(blockHeaderHash).map(BlockHeader.class::cast)))
        .map(
            worldState -> {
              ensureTrieBranchType(worldState, rollBranchType);
              return worldState;
            })
        .flatMap(
            worldState ->
                rollFullWorldStateToBlockHash(
                    worldState, blockHeader.getBlockHash(), skipStateRootVerificationOnRollPersist))
        .map(
            worldState -> {
              maybeBlockAccessListOverlay.ifPresent(worldState::applyBlockAccessListOverlay);
              return worldState;
            })
        .map(MutableWorldState::freezeStorage);
  }

  /** Aligns in-memory trie branch with the format required for the upcoming roll/persist. */
  private void ensureTrieBranchType(
      final BonsaiWorldState worldState, final TrieBranchType rollBranchType) {
    if (worldState.getTrieBranchType() != rollBranchType) {
      final Hash stateroot =
          Hash.wrap(
              Bytes32.wrap(
                  worldStateKeyValueStorage
                      .getWorldStateRootHash(rollBranchType)
                      .orElse(Bytes32.ZERO)));
      final Hash blockhash =
          worldStateKeyValueStorage.getWorldStateBlockHash(rollBranchType).orElseThrow();
      worldState.resetWorldStateTo(blockhash, stateroot, rollBranchType);
    }
  }

  private BlockHeader headerOrThrow(final Hash blockHash) {
    return blockchain
        .getBlockHeader(blockHash)
        .orElseThrow(
            () -> new IllegalStateException("Missing block header for block hash " + blockHash));
  }

  private TrieLog trieLogOrThrow(final Hash blockHash) {
    return trieLogManager
        .getTrieLogLayer(blockHash)
        .orElseThrow(
            () -> new IllegalStateException("Missing trie log for block hash " + blockHash));
  }

  private synchronized Optional<BonsaiWorldState> rollFullWorldStateToBlockHash(
      final BonsaiWorldState mutableState,
      final Hash blockHash,
      final boolean skipStateRootVerificationOnRollPersist) {
    try {
      rollFlatDbToBlockHash(mutableState, blockHash);
    } catch (final RuntimeException re) {
      LOG.warn("Flat DB rolling failed for block hash " + blockHash, re);
      if (re instanceof MerkleTrieException) {
        throw re;
      }
      throw new MerkleTrieException(
          "Flat DB rolling failed for block hash " + blockHash + ": " + re.getMessage(),
          re,
          Optional.of(Address.ZERO),
          Bytes32.wrap(Hash.EMPTY.getBytes()),
          Bytes.EMPTY);
    }
    if (blockHash.equals(mutableState.blockHash())) {
      return Optional.of(mutableState);
    } else {
      try {
        final Optional<BlockHeader> maybePersistedHeader =
            blockchain.getBlockHeader(mutableState.blockHash()).map(BlockHeader.class::cast);

        final List<TrieLog> rollBacks = new ArrayList<>();
        final List<TrieLog> rollForwards = new ArrayList<>();
        if (maybePersistedHeader.isEmpty()) {
          trieLogManager.getTrieLogLayer(mutableState.blockHash()).ifPresent(rollBacks::add);
        } else {
          BlockHeader targetHeader = headerOrThrow(blockHash);
          BlockHeader persistedHeader = maybePersistedHeader.get();
          Hash persistedBlockHash = persistedHeader.getBlockHash();
          while (persistedHeader.getNumber() > targetHeader.getNumber()) {
            LOG.debug("Rollback {}", persistedBlockHash);
            rollBacks.add(trieLogOrThrow(persistedBlockHash));
            persistedHeader = headerOrThrow(persistedHeader.getParentHash());
            persistedBlockHash = persistedHeader.getBlockHash();
          }
          Hash targetBlockHash = targetHeader.getBlockHash();
          while (persistedHeader.getNumber() < targetHeader.getNumber()) {
            LOG.debug("Rollforward {}", targetBlockHash);
            rollForwards.add(trieLogOrThrow(targetBlockHash));
            targetHeader = headerOrThrow(targetHeader.getParentHash());
            targetBlockHash = targetHeader.getBlockHash();
          }
          while (!persistedBlockHash.equals(targetBlockHash)) {
            LOG.debug("Paired Rollback {}", persistedBlockHash);
            LOG.debug("Paired Rollforward {}", targetBlockHash);
            rollForwards.add(trieLogOrThrow(targetBlockHash));
            targetHeader = headerOrThrow(targetHeader.getParentHash());
            rollBacks.add(trieLogOrThrow(persistedBlockHash));
            persistedHeader = headerOrThrow(persistedHeader.getParentHash());
            targetBlockHash = targetHeader.getBlockHash();
            persistedBlockHash = persistedHeader.getBlockHash();
          }
        }

        final BonsaiWorldStateUpdateAccumulator pathBasedUpdater = mutableState.updater();
        try {
          for (final TrieLog rollBack : rollBacks) {
            LOG.debug("Attempting Rollback of {}", rollBack.getBlockHash());
            pathBasedUpdater.rollBack(rollBack);
          }
          for (int i = rollForwards.size() - 1; i >= 0; i--) {
            final TrieLog forward = rollForwards.get(i);
            LOG.debug("Attempting Rollforward of {}", forward.getBlockHash());
            pathBasedUpdater.rollForward(forward);
          }
          pathBasedUpdater.commit();
          mutableState.setSkipStateRootVerification(skipStateRootVerificationOnRollPersist);
          try {
            mutableState.persist(headerOrThrow(blockHash));
          } finally {
            mutableState.setSkipStateRootVerification(false);
          }
          LOG.debug(
              "Archive rolling finished, {} now at {}",
              mutableState.getWorldStateStorage().getClass().getSimpleName(),
              blockHash);
          return Optional.of(mutableState);
        } catch (final MerkleTrieException re) {
          throw re;
        } catch (final Exception e) {
          pathBasedUpdater.reset();
          LOG.atDebug()
              .setMessage("State rolling failed on {} for block hash {}")
              .addArgument(mutableState.getWorldStateStorage().getClass().getSimpleName())
              .addArgument(blockHash)
              .addArgument(e)
              .log();
          return Optional.empty();
        }
      } catch (final RuntimeException re) {
        LOG.warn("Archive rolling failed for block hash " + blockHash, re);
        if (re instanceof MerkleTrieException) {
          throw re;
        }
        throw new MerkleTrieException(
            "Archive rolling failed for block hash " + blockHash + ": " + re.getMessage(),
            re,
            Optional.of(Address.ZERO),
            Bytes32.wrap(Hash.EMPTY.getBytes()),
            Bytes.EMPTY);
      }
    }
  }

  private void rollFlatDbToBlockHash(
      final BonsaiWorldState mutableState, final Hash targetBlockHash) {
    if (mutableState.isStorageFrozen()) {
      return;
    }
    final BonsaiWorldStateKeyValueStorage storage = mutableState.getWorldStateStorage();
    // PARTIAL reads fall back to the trie on a flat miss, so flat and trie cannot be rolled in
    // separate commits. ARCHIVE storage is already block-versioned and does not need rolling.
    if (storage.getFlatDbMode() != FlatDbMode.FULL) {
      return;
    }
    final Optional<Hash> persistedFlatDbBlockHash = storage.getFlatDbBlockHash();
    final Hash currentFlatDbBlockHash = persistedFlatDbBlockHash.orElseGet(mutableState::blockHash);

    // Existing databases have no flat cursor. Their flat DB and active trie were committed
    // together, so let the next normal persist establish the cursor.
    if (persistedFlatDbBlockHash.isEmpty()) {
      return;
    }

    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    try {
      if (!currentFlatDbBlockHash.equals(targetBlockHash)) {
        LOG.debug(
            "Synchronizing flat DB from {} to {} before trie rolling",
            currentFlatDbBlockHash,
            targetBlockHash);
        final List<FlatDbRollStep> steps = planFlatDbRoll(currentFlatDbBlockHash, targetBlockHash);
        for (final FlatDbRollStep step : steps) {
          applyFlatDbRollStep(updater, step);
        }
      }
      if (!currentFlatDbBlockHash.equals(targetBlockHash)) {
        updater.putFlatDbBlockHash(targetBlockHash);
        updater.commitComposedOnly();
      } else {
        updater.rollback();
      }
    } catch (final RuntimeException e) {
      updater.rollback();
      throw e;
    }
  }

  private List<FlatDbRollStep> planFlatDbRoll(
      final Hash currentBlockHash, final Hash targetBlockHash) {
    BlockHeader currentHeader = headerOrThrow(currentBlockHash);
    BlockHeader targetHeader = headerOrThrow(targetBlockHash);
    final List<FlatDbRollStep> rollBacks = new ArrayList<>();
    final List<FlatDbRollStep> rollForwards = new ArrayList<>();

    while (currentHeader.getNumber() > targetHeader.getNumber()) {
      rollBacks.add(new FlatDbRollStep(trieLogOrThrow(currentHeader.getBlockHash()), false));
      currentHeader = headerOrThrow(currentHeader.getParentHash());
    }
    while (currentHeader.getNumber() < targetHeader.getNumber()) {
      rollForwards.add(new FlatDbRollStep(trieLogOrThrow(targetHeader.getBlockHash()), true));
      targetHeader = headerOrThrow(targetHeader.getParentHash());
    }
    while (!currentHeader.getBlockHash().equals(targetHeader.getBlockHash())) {
      rollBacks.add(new FlatDbRollStep(trieLogOrThrow(currentHeader.getBlockHash()), false));
      rollForwards.add(new FlatDbRollStep(trieLogOrThrow(targetHeader.getBlockHash()), true));
      currentHeader = headerOrThrow(currentHeader.getParentHash());
      targetHeader = headerOrThrow(targetHeader.getParentHash());
    }

    for (int i = rollForwards.size() - 1; i >= 0; i--) {
      rollBacks.add(rollForwards.get(i));
    }
    return rollBacks;
  }

  private void applyFlatDbRollStep(
      final BonsaiWorldStateKeyValueStorage.Updater updater, final FlatDbRollStep step) {
    final TrieLog trieLog = step.trieLog();
    trieLog
        .getAccountChanges()
        .forEach(
            (address, change) -> {
              final AccountValue replacement =
                  step.forward() ? change.getUpdated() : change.getPrior();
              if (replacement == null) {
                updater.removeAccountInfoState(address.addressHash());
              } else {
                updater.putAccountInfoState(
                    address.addressHash(), RLP.encode(replacement::writeTo));
              }
            });
    trieLog
        .getCodeChanges()
        .forEach(
            (address, change) -> {
              final Bytes replacement = step.forward() ? change.getUpdated() : change.getPrior();
              if (replacement == null || replacement.isEmpty()) {
                updater.removeCode(address.addressHash());
              } else {
                updater.putCode(address.addressHash(), Hash.hash(replacement), replacement);
              }
            });
    trieLog
        .getStorageChanges()
        .forEach(
            (address, changes) ->
                changes.forEach(
                    (slot, change) -> {
                      final UInt256 replacement =
                          step.forward() ? change.getUpdated() : change.getPrior();
                      if (replacement == null || replacement.isZero()) {
                        updater.removeStorageValueBySlotHash(
                            address.addressHash(), slot.getSlotHash());
                      } else {
                        updater.putStorageValueBySlotHash(
                            address.addressHash(), slot.getSlotHash(), replacement);
                      }
                    }));
  }

  private record FlatDbRollStep(TrieLog trieLog, boolean forward) {}

  public WorldStateConfig getWorldStateSharedSpec() {
    return worldStateConfig;
  }

  public BonsaiWorldStateKeyValueStorage getWorldStateKeyValueStorage() {
    return worldStateKeyValueStorage;
  }

  public TrieLogManager getTrieLogManager() {
    return trieLogManager;
  }

  public BonsaiWorldStateCacheManager getWorldStateCacheManager() {
    return worldStateCacheManager;
  }

  @Override
  public void resetArchiveStateTo(final BlockHeader blockHeader) {
    headWorldState.resetWorldStateTo(
        blockHeader.getBlockHash(), blockHeader.getStateRoot(), resolveTrieBranchType(blockHeader));
    this.worldStateCacheManager.reset();
    this.worldStateCacheManager.addCachedLayer(
        blockHeader, headWorldState.getWorldStateRootHash(), headWorldState);
  }

  @Override
  public <U> Optional<U> getAccountProof(
      final BlockHeader blockHeader,
      final Address accountAddress,
      final List<UInt256> accountStorageKeys,
      final Function<Optional<WorldStateProof>, ? extends Optional<U>> mapper) {
    try (BonsaiWorldState ws =
        (BonsaiWorldState)
            getWorldState(withBlockHeaderAndNoUpdateNodeHead(blockHeader)).orElse(null)) {
      if (ws != null) {
        final WorldStateProofProvider worldStateProofProvider =
            new WorldStateProofProvider(
                new WorldStateStorageCoordinator(ws.getWorldStateStorage()));
        return mapper.apply(
            worldStateProofProvider.getAccountProof(
                ws.getWorldStateRootHash(), accountAddress, accountStorageKeys));
      }
    } catch (Exception ex) {
      LOG.error(
          "failed proof query for " + blockHeader.getBlockHash().getBytes().toShortHexString(), ex);
    }
    return Optional.empty();
  }

  @Override
  public void close() {
    try {
      worldStateKeyValueStorage.close();
    } catch (Exception e) {
      // no op
    }
  }

  private void initializeHeadWorldState(final BonsaiWorldState headWorldState) {
    blockchain
        .getBlockHeader(headWorldState.getWorldStateBlockHash())
        .ifPresentOrElse(
            header -> loadHeadWorldState(header, headWorldState),
            () -> this.headWorldState = headWorldState);
  }

  protected void loadHeadWorldState(
      final BlockHeader blockHeader, final BonsaiWorldState headWorldState) {
    this.headWorldState = headWorldState;
    this.worldStateCacheManager.addCachedLayer(
        blockHeader, headWorldState.getWorldStateRootHash(), headWorldState);
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

  /** Binary-trie fork active at {@code blockHeader} → PBT, otherwise MPT. */
  public TrieBranchType resolveTrieBranchType(final BlockHeader blockHeader) {
    return BinaryTrieForkSupport.isBinaryTrieActive(blockHeader.getTimestamp(), binaryTrieMilestone)
        ? TrieBranchType.BINARY
        : TrieBranchType.PATRICIA;
  }

  private BonsaiWorldStateKeyValueStorage getBonsaiWorldStateKeyValueStorage() {
    return worldStateKeyValueStorage;
  }

  /**
   * Prepares the state healing process for a given address and location. It prepares the state
   * healing, including retrieving data from storage, identifying invalid slots or nodes, removing
   * account and slot from the state trie, and committing the changes. Finally, it downgrades the
   * world state storage to partial flat database mode.
   */
  public void prepareStateHealing(final Address address, final Bytes location) {
    final Set<Bytes> keysToDelete = new HashSet<>();
    final BonsaiWorldStateKeyValueStorage.Updater updater =
        getBonsaiWorldStateKeyValueStorage().updater();
    final Hash accountHash = address.addressHash();
    final StoredMerklePatriciaTrie<Bytes, Bytes> accountTrie =
        new StoredMerklePatriciaTrie<Bytes, Bytes>(
            (l, h) -> {
              final Optional<Bytes> node = getBonsaiWorldStateKeyValueStorage().getTrieNode(l, h);
              if (node.isPresent()) {
                keysToDelete.add(l);
              }
              return node;
            },
            Bytes32.wrap(headWorldState.getWorldStateRootHash().getBytes()),
            Function.identity(),
            Function.identity());
    try {
      accountTrie
          .get(accountHash.getBytes())
          .map(RLP::input)
          .map(PatriciaTrieAccountValue::readFrom)
          .ifPresent(
              account -> {
                final StoredMerklePatriciaTrie<Bytes, Bytes> storageTrie =
                    new StoredMerklePatriciaTrie<Bytes, Bytes>(
                        (l, h) -> {
                          Optional<Bytes> node =
                              getBonsaiWorldStateKeyValueStorage()
                                  .getTrieNode(Bytes.concatenate(accountHash.getBytes(), l), h);
                          if (node.isPresent()) {
                            keysToDelete.add(Bytes.concatenate(accountHash.getBytes(), l));
                          }
                          return node;
                        },
                        Bytes32.wrap(account.getStorageRoot().getBytes()),
                        Function.identity(),
                        Function.identity());
                try {
                  storageTrie.getPath(location);
                } catch (Exception eA) {
                  LOG.warn("Invalid slot found for account {} at location {}", address, location);
                  // ignore
                }
              });
    } catch (Exception eA) {
      LOG.warn("Invalid node for account {} at location {}", address, location);
      // ignore
    }
    keysToDelete.forEach(updater::removeTrieNode);
    updater.commit();

    getBonsaiWorldStateKeyValueStorage().downgradeToPartialFlatDbMode();
  }

  @Override
  public void heal(final Optional<Address> maybeAccountToRepair, final Bytes location) {
    worldStateHealerSupplier.get().heal(maybeAccountToRepair, location);
  }
}
