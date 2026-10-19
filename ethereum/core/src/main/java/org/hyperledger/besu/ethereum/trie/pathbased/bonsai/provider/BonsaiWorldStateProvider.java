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

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BinaryTrieForkSupport;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.PbtColumnOwnership;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.PmtPruner;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.BonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.ExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiWorldStateProvider extends PathBasedWorldStateProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiWorldStateProvider.class);

  private final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader;
  private final Optional<Long> amsterdamMilestone;

  /** Timestamp of the binary trie fork, when the chain schedules it. */
  private final Optional<Long> binaryTrieMilestone;

  /** Who writes the binary-trie column: the PBT migrator before the fork, the chain after. */
  private final PbtColumnOwnership binaryColumnOwnership;

  /** Deletes the PMT once the fork is finalized; pre-fork states cannot be read after that. */
  private final PmtPruner pmtPruner;

  private volatile CodeFetcher codeFetcher = codeHash -> Optional.empty();

  public BonsaiWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final ExtraStorageConfiguration extraStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final BonsaiCodeCache codeCache) {
    this(
        worldStateKeyValueStorage,
        blockchain,
        extraStorageConfiguration,
        bonsaiCachedMerkleTrieLoader,
        pluginContext,
        evmConfiguration,
        codeCache,
        Optional.empty());
  }

  public BonsaiWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final ExtraStorageConfiguration extraStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final BonsaiCodeCache codeCache,
      final Optional<Long> amsterdamMilestone) {
    this(
        worldStateKeyValueStorage,
        blockchain,
        extraStorageConfiguration,
        bonsaiCachedMerkleTrieLoader,
        pluginContext,
        evmConfiguration,
        codeCache,
        amsterdamMilestone,
        Optional.empty());
  }

  public BonsaiWorldStateProvider(
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final ExtraStorageConfiguration extraStorageConfiguration,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final BonsaiCodeCache codeCache,
      final Optional<Long> amsterdamMilestone,
      final Optional<Long> binaryTrieMilestone) {
    super(
        worldStateKeyValueStorage,
        blockchain,
        extraStorageConfiguration,
        pluginContext,
        binaryTrieMilestone);
    this.bonsaiCachedMerkleTrieLoader = bonsaiCachedMerkleTrieLoader;
    this.amsterdamMilestone = amsterdamMilestone;
    this.binaryTrieMilestone = binaryTrieMilestone;
    this.binaryColumnOwnership = new PbtColumnOwnership(worldStateKeyValueStorage);
    this.pmtPruner = new PmtPruner(worldStateKeyValueStorage, blockchain, binaryTrieMilestone);
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
      final ExtraStorageConfiguration extraStorageConfiguration,
      final TrieLogManager trieLogManager,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final EvmConfiguration evmConfiguration,
      final BonsaiCodeCache codeCache) {
    this(
        bonsaiWorldStateCacheManager,
        extraStorageConfiguration,
        trieLogManager,
        worldStateKeyValueStorage,
        blockchain,
        bonsaiCachedMerkleTrieLoader,
        evmConfiguration,
        codeCache,
        Optional.empty());
  }

  @VisibleForTesting
  BonsaiWorldStateProvider(
      final BonsaiWorldStateCacheManager bonsaiWorldStateCacheManager,
      final ExtraStorageConfiguration extraStorageConfiguration,
      final TrieLogManager trieLogManager,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final BonsaiCachedMerkleTrieLoader bonsaiCachedMerkleTrieLoader,
      final EvmConfiguration evmConfiguration,
      final BonsaiCodeCache codeCache,
      final Optional<Long> binaryTrieMilestone) {
    super(worldStateKeyValueStorage, blockchain, extraStorageConfiguration, trieLogManager);
    this.bonsaiCachedMerkleTrieLoader = bonsaiCachedMerkleTrieLoader;
    this.amsterdamMilestone = Optional.empty();
    this.binaryTrieMilestone = binaryTrieMilestone;
    this.binaryColumnOwnership = new PbtColumnOwnership(worldStateKeyValueStorage);
    this.pmtPruner = new PmtPruner(worldStateKeyValueStorage, blockchain, binaryTrieMilestone);
    this.evmConfiguration = evmConfiguration;
    provideWorldStateCacheManager(bonsaiWorldStateCacheManager);
    initializeHeadWorldState(
        new BonsaiWorldState(
            this, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache));
  }

  public BonsaiCachedMerkleTrieLoader getCachedMerkleTrieLoader() {
    return bonsaiCachedMerkleTrieLoader;
  }

  public PbtColumnOwnership getBinaryColumnOwnership() {
    return binaryColumnOwnership;
  }

  public PmtPruner getPmtPruner() {
    return pmtPruner;
  }

  /** The timestamp the binary trie fork starts at, if scheduled. */
  public Optional<Long> getBinaryTrieMilestone() {
    return binaryTrieMilestone;
  }

  /** A world state on the binary trie branch, for the PBT migrator. */
  public BonsaiWorldState getMigrationWorldState() {
    return new BonsaiWorldState(
        worldStateKeyValueStorage,
        new NoOpBonsaiCachedMerkleTrieLoader(),
        worldStateCacheManager,
        trieLogManager,
        evmConfiguration,
        worldStateConfig,
        ((BonsaiWorldStateCacheManager) worldStateCacheManager).getCodeCache(),
        TrieBranchType.BINARY);
  }

  @Override
  public TrieBranchType resolveTrieBranchType(
      final org.hyperledger.besu.plugin.data.BlockHeader blockHeader) {
    // without a binary trie fork the header is not needed, and may be missing
    if (binaryTrieMilestone.isEmpty() || blockHeader == null) {
      return TrieBranchType.PATRICIA;
    }
    return BinaryTrieForkSupport.isBinaryTrieActive(blockHeader.getTimestamp(), binaryTrieMilestone)
        ? TrieBranchType.BINARY
        : TrieBranchType.PATRICIA;
  }

  @Override
  public boolean isWorldStateAvailable(final Hash rootHash, final Hash blockHash) {
    // pre-fork states cannot be read once the PMT is deleted
    if (pmtPruner.isPmtPruned()
        && blockchain
            .getBlockHeader(blockHash)
            .filter(header -> resolveTrieBranchType(header) == TrieBranchType.PATRICIA)
            .isPresent()) {
      return false;
    }
    return super.isWorldStateAvailable(rootHash, blockHash);
  }

  /**
   * Rolls in the trie branch of the target block. Before the binary trie fork the world state rolls
   * in the MPT; across the transition and after it, in the binary trie, without checking the state
   * root at the transition.
   */
  @Override
  protected Optional<MutableWorldState> getFullWorldState(final WorldStateQueryParams queryParams) {
    if (binaryTrieMilestone.isEmpty()) {
      return super.getFullWorldState(queryParams);
    }
    final BlockHeader targetHeader =
        Optional.ofNullable(queryParams.getBlockHeader())
            .orElseGet(
                () ->
                    blockchain
                        .getBlockHeader(queryParams.getBlockHash())
                        .orElseThrow(
                            () ->
                                new IllegalStateException(
                                    "Missing block header for block hash "
                                        + queryParams.getBlockHash())));
    final Optional<Long> nextTimestamp =
        queryParams.getTimeStamp().or(() -> Optional.of(targetHeader.getTimestamp()));
    final boolean isPbtTransition =
        BinaryTrieForkSupport.isBinaryTrieTransition(
            targetHeader.getTimestamp(), binaryTrieMilestone, nextTimestamp);
    final boolean targetIsPbt =
        BinaryTrieForkSupport.isBinaryTrieActive(targetHeader.getTimestamp(), binaryTrieMilestone);

    if (isPbtTransition) {
      // drop cached MPT snapshots so we do not serve a stale pre-transition view
      worldStateCacheManager.reset();
    }
    final TrieBranchType rollBranchType =
        targetIsPbt || isPbtTransition ? TrieBranchType.BINARY : TrieBranchType.PATRICIA;
    if (rollBranchType == TrieBranchType.PATRICIA && pmtPruner.isPmtPruned()) {
      LOG.debug("PMT deleted after the PBT fork: no state for block {}", targetHeader.getNumber());
      return Optional.empty();
    }
    if (rollBranchType == TrieBranchType.BINARY) {
      binaryColumnOwnership.claimForChain();
    }
    return getFullWorldState(queryParams, Optional.of(rollBranchType), isPbtTransition);
  }

  @Override
  protected void beforeRollingWorldState(
      final PathBasedWorldState worldState, final Hash blockHash) {
    try {
      rollFlatDbToBlockHash(worldState, blockHash);
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
  }

  // the flat DB is shared by both trie branches, so it follows the trie logs to the target block
  private void rollFlatDbToBlockHash(
      final PathBasedWorldState mutableState, final Hash targetBlockHash) {
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
    // Existing databases have no flat cursor. Their flat DB and active trie were committed
    // together, so let the next normal persist establish the cursor.
    if (persistedFlatDbBlockHash.isEmpty()) {
      return;
    }
    final Hash currentFlatDbBlockHash = persistedFlatDbBlockHash.get();
    // at the trie's block, or already at the target: the trie roll's persist moves it, as on main
    if (currentFlatDbBlockHash.equals(mutableState.blockHash())
        || currentFlatDbBlockHash.equals(targetBlockHash)) {
      return;
    }
    LOG.debug(
        "Synchronizing flat DB from {} to {} before trie rolling",
        currentFlatDbBlockHash,
        targetBlockHash);
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    try {
      for (final FlatDbRollStep step : planFlatDbRoll(currentFlatDbBlockHash, targetBlockHash)) {
        applyFlatDbRollStep(updater, step);
      }
      updater.putFlatDbBlockHash(targetBlockHash);
      updater.commitComposedOnly();
    } catch (final RuntimeException e) {
      updater.rollback();
      throw e;
    }
  }

  private List<FlatDbRollStep> planFlatDbRoll(
      final Hash currentBlockHash, final Hash targetBlockHash) {
    BlockHeader currentHeader = flatRollHeader(currentBlockHash);
    BlockHeader targetHeader = flatRollHeader(targetBlockHash);
    final List<FlatDbRollStep> rollBacks = new ArrayList<>();
    final List<FlatDbRollStep> rollForwards = new ArrayList<>();

    while (currentHeader.getNumber() > targetHeader.getNumber()) {
      rollBacks.add(new FlatDbRollStep(flatRollTrieLog(currentHeader.getBlockHash()), false));
      currentHeader = flatRollHeader(currentHeader.getParentHash());
    }
    while (currentHeader.getNumber() < targetHeader.getNumber()) {
      rollForwards.add(new FlatDbRollStep(flatRollTrieLog(targetHeader.getBlockHash()), true));
      targetHeader = flatRollHeader(targetHeader.getParentHash());
    }
    while (!currentHeader.getBlockHash().equals(targetHeader.getBlockHash())) {
      rollBacks.add(new FlatDbRollStep(flatRollTrieLog(currentHeader.getBlockHash()), false));
      rollForwards.add(new FlatDbRollStep(flatRollTrieLog(targetHeader.getBlockHash()), true));
      currentHeader = flatRollHeader(currentHeader.getParentHash());
      targetHeader = flatRollHeader(targetHeader.getParentHash());
    }

    for (int i = rollForwards.size() - 1; i >= 0; i--) {
      rollBacks.add(rollForwards.get(i));
    }
    return rollBacks;
  }

  private BlockHeader flatRollHeader(final Hash blockHash) {
    return blockchain
        .getBlockHeader(blockHash)
        .orElseThrow(
            () -> new IllegalStateException("Missing block header for block hash " + blockHash));
  }

  private TrieLog flatRollTrieLog(final Hash blockHash) {
    return trieLogManager
        .getTrieLogLayer(blockHash)
        .orElseThrow(
            () -> new IllegalStateException("Missing trie log for block hash " + blockHash));
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
              final Bytes replaced = step.forward() ? change.getPrior() : change.getUpdated();
              if (replacement == null || replacement.isEmpty()) {
                updater.removeCode(
                    address.addressHash(), replaced == null ? Hash.EMPTY : Hash.hash(replaced));
              } else {
                updater.putCode(address.addressHash(), replacement);
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

  private void initializeHeadWorldState(final BonsaiWorldState headWorldState) {
    blockchain
        .getBlockHeader(headWorldState.getWorldStateBlockHash())
        .ifPresentOrElse(
            header -> loadHeadWorldState(header, headWorldState),
            () -> this.headWorldState = headWorldState);
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

  public void setCodeFetcher(final CodeFetcher codeFetcher) {
    this.codeFetcher = codeFetcher;
  }

  @Override
  public boolean healMissingCode(final Address address, final Hash codeHash) {
    // the block already failed as a local fault, a failed heal must not change that
    try {
      // checked before asking the peers for a code that could not be stored
      if (wouldReplaceHeadCode(address, codeHash)) {
        LOG.warn(
            "Not healing the code {} of account {}, the head has another one", codeHash, address);
        return false;
      }
      // only the code of the stored hash is written, so a peer cannot change what is executed
      final Optional<Bytes> code =
          codeFetcher.fetch(codeHash).filter(fetched -> Hash.hash(fetched).equals(codeHash));
      if (code.isEmpty()) {
        LOG.warn("Unable to heal the missing code {} of account {}", codeHash, address);
        return false;
      }
      return withHeadWorldStateLock(() -> storeHeadAccountCode(address, codeHash, code.get()));
    } catch (final RuntimeException e) {
      LOG.warn("Unable to heal the missing code {} of account {}", codeHash, address, e);
      return false;
    }
  }

  // a code stored by its hash replaces nothing, but stored by account it replaces the head one
  private boolean wouldReplaceHeadCode(final Address address, final Hash codeHash) {
    if (getWorldStateKeyValueStorage().getFlatDbStrategy().isCodeByCodeHash()) {
      return false;
    }
    final Account headAccount = getWorldState().get(address);
    return headAccount == null || !headAccount.getCodeHash().equals(codeHash);
  }

  private boolean storeHeadAccountCode(
      final Address address, final Hash codeHash, final Bytes code) {
    // checked again, the head may have moved during the fetch
    if (wouldReplaceHeadCode(address, codeHash)) {
      LOG.warn(
          "Not healing the code {} of account {}, the head moved to another one",
          codeHash,
          address);
      return false;
    }
    final BonsaiWorldStateKeyValueStorage.Updater updater =
        getWorldStateKeyValueStorage().updater();
    updater.putCode(address.addressHash(), codeHash, code);
    updater.commit();
    // the cached world states are snapshots taken before the code was stored
    worldStateCacheManager.reset();
    LOG.info("Healed the missing code {} of account {}", codeHash, address);
    return true;
  }

  private boolean isAmsterdamActive(final BlockHeader blockHeader) {
    return amsterdamMilestone
        .map(milestone -> Long.compareUnsigned(blockHeader.getTimestamp(), milestone) >= 0)
        .orElse(false);
  }
}
