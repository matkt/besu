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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.BINARY_TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;

import org.hyperledger.besu.config.GenesisAccount;
import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary.DefaultBinaryStateRootCommitter;
import org.hyperledger.besu.ethereum.trie.common.BinaryTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BinaryTrieForkSupport;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.MigrationScopedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.StateRootComputation;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PbtMigrator {
  private static final Logger LOG = LoggerFactory.getLogger(PbtMigrator.class);

  private final BonsaiWorldStateProvider provider;
  private final Blockchain blockchain;
  private final Optional<Long> binaryTrieMilestone;
  private final ScheduledExecutorService scheduler;
  private final Stream<GenesisAccount> genesisAllocations;
  private final long pollIntervalMs;

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean bootstrapped = new AtomicBoolean(false);

  private volatile long lastMigratedBlockNumber = -1L;
  private volatile Hash lastMigratedBlockHash = null;
  private volatile long lastTickTimeMs = 0L;

  public PbtMigrator(
      final BonsaiWorldStateProvider provider,
      final Blockchain blockchain,
      final Optional<Long> binaryTrieMilestone,
      final ScheduledExecutorService scheduler,
      final Stream<GenesisAccount> genesisAllocations,
      final long pollIntervalMs) {
    this.provider = provider;
    this.blockchain = blockchain;
    this.binaryTrieMilestone = binaryTrieMilestone;
    this.scheduler = scheduler;
    this.genesisAllocations = genesisAllocations;
    this.pollIntervalMs = pollIntervalMs;
  }

  public void start() {
    if (binaryTrieMilestone.isEmpty()) {
      LOG.info("PBT migrator disabled: no binaryTrieTime fork configured");
      return;
    }
    if (!started.compareAndSet(false, true)) {
      LOG.warn("PBT migrator already started");
      return;
    }
    LOG.info("PBT migrator starting; binaryTrieTime milestone={}", binaryTrieMilestone.get());
    scheduler.scheduleWithFixedDelay(
        this::tick, pollIntervalMs, pollIntervalMs, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    started.set(false);
  }

  public boolean isCaughtUp() {
    return lastMigratedBlockHash != null
        && lastMigratedBlockHash.equals(blockchain.getChainHeadHash());
  }

  public long getLastTickTimeMs() {
    return lastTickTimeMs;
  }

  private void tick() {
    if (!started.get()) {
      return;
    }
    try {
      final BlockHeader chainHead = blockchain.getChainHeadHeader();
      if (!bootstrapped.get()) {
        bootstrap();
      }
      migrateToward(chainHead);
    } catch (final Throwable t) {
      LOG.atError().setMessage("PBT migrator tick failed (will retry): {}").addArgument(t).log();
    }
  }

  private void bootstrap() {
    final Optional<Hash> existing =
        provider.getWorldStateKeyValueStorage().getWorldStateBlockHash(TrieBranchType.BINARY);

    if (existing.isPresent() && !existing.get().equals(Hash.ZERO)) {
      final Hash baseHash = existing.get();
      blockchain
          .getBlockHeader(baseHash)
          .ifPresent(
              h -> {
                lastMigratedBlockHash = baseHash;
                lastMigratedBlockNumber = h.getNumber();
              });
      bootstrapped.set(true);
      LOG.info(
          "PBT migrator resuming from previously-materialised base block {} ({})",
          lastMigratedBlockNumber,
          baseHash);
      return;
    }

    final Optional<BlockHeader> genesis =
        blockchain.getBlockHeader(0L).map(BlockHeader.class::cast);
    if (genesis.isEmpty()) {
      LOG.warn("PBT migrator cannot bootstrap: genesis block header not found");
      return;
    }

    lastMigratedBlockHash = genesis.get().getBlockHash();
    lastMigratedBlockNumber = genesis.get().getNumber();
    bootstrapped.set(true);

    LOG.info(
        "PBT migrator bootstrap ready; rolling base seeded at genesis {} ({})",
        lastMigratedBlockNumber,
        lastMigratedBlockHash);
    applyTrieLogsAndPersist(
        provider.getMigrationWorldState(),
        genesis.get(),
        Collections.emptyList(),
        List.of(getTrieLogOrGenesis(genesis.get())));
    lastTickTimeMs = System.currentTimeMillis();
  }

  private void migrateToward(final BlockHeader target) {
    if (lastMigratedBlockHash == null) {
      return;
    }

    if (target.getBlockHash().equals(lastMigratedBlockHash)) {
      LOG.atDebug()
          .setMessage("PBT migrator caught up at block {} ({})")
          .addArgument(target.getNumber())
          .addArgument(target.getBlockHash())
          .log();
      return;
    }

    if (BinaryTrieForkSupport.isBinaryTrieActive(target.getTimestamp(), binaryTrieMilestone)) {
      final Optional<BlockHeader> lastPmt = lastPmtAncestor(target);
      if (lastPmt.isPresent() && !lastPmt.get().getBlockHash().equals(lastMigratedBlockHash)) {
        rollTo(lastPmt.get());
      }
      LOG.atInfo()
          .setMessage(
              "PBT migrator reached PBT era at chain head {} ({}); last PMT block {} ({}) is the live path's binary base. Live path takes over; migrator stopping.")
          .addArgument(target.getNumber())
          .addArgument(target.getBlockHash())
          .addArgument(lastPmt.map(BlockHeader::getNumber).orElse(-1L))
          .addArgument(lastPmt.map(BlockHeader::getBlockHash).orElse(Hash.EMPTY))
          .log();
      stop();
      return;
    }

    rollTo(target);
  }

  private void rollTo(final BlockHeader target) {
    final BonsaiWorldState bonsaiWorldState = provider.getMigrationWorldState();
    final Optional<BlockHeader> maybePersistedHeader =
        blockchain.getBlockHeader(lastMigratedBlockHash).map(BlockHeader.class::cast);

    if (maybePersistedHeader.isEmpty()) {
      LOG.warn("PBT migrator missing persisted header for {}", lastMigratedBlockHash);
      return;
    }

    try {
      final List<TrieLog> rollBacks = new ArrayList<>();
      final List<TrieLog> rollForwards = new ArrayList<>();

      BlockHeader persistedHeader = maybePersistedHeader.get();
      BlockHeader targetHeader = target;
      Hash persistedBlockHash = persistedHeader.getBlockHash();
      Hash targetBlockHash = targetHeader.getBlockHash();

      while (persistedHeader.getNumber() > targetHeader.getNumber()) {
        rollBacks.add(getTrieLogOrGenesis(persistedHeader));
        persistedHeader = blockchain.getBlockHeader(persistedHeader.getParentHash()).orElseThrow();
        persistedBlockHash = persistedHeader.getBlockHash();
      }

      while (persistedHeader.getNumber() < targetHeader.getNumber()) {
        rollForwards.add(getTrieLogOrGenesis(targetHeader));
        targetHeader = blockchain.getBlockHeader(targetHeader.getParentHash()).orElseThrow();
        targetBlockHash = targetHeader.getBlockHash();
      }

      while (!persistedBlockHash.equals(targetBlockHash)) {
        rollForwards.add(getTrieLogOrGenesis(targetHeader));
        targetHeader = blockchain.getBlockHeader(targetHeader.getParentHash()).orElseThrow();

        rollBacks.add(getTrieLogOrGenesis(persistedHeader));
        persistedHeader = blockchain.getBlockHeader(persistedHeader.getParentHash()).orElseThrow();

        targetBlockHash = targetHeader.getBlockHash();
        persistedBlockHash = persistedHeader.getBlockHash();
      }

      applyTrieLogsAndPersist(bonsaiWorldState, target, rollBacks, rollForwards);

      lastMigratedBlockHash = target.getBlockHash();
      lastMigratedBlockNumber = target.getNumber();
      lastTickTimeMs = System.currentTimeMillis();

    } catch (final Exception e) {
      LOG.atError()
          .setMessage("PBT migrator failed while rolling to block {} ({}): {}")
          .addArgument(target.getNumber())
          .addArgument(target.getBlockHash())
          .addArgument(e)
          .log();
    }
  }

  private TrieLog getTrieLogOrGenesis(final BlockHeader header) {
    if (header.getNumber() == 0L) {
      return genesisAllocationsToTrieLogLayer(genesisAllocations, header);
    }
    return provider.getTrieLogManager().getTrieLogLayer(header.getBlockHash()).orElseThrow();
  }

  private void applyTrieLogsAndPersist(
      final BonsaiWorldState bonsaiWorldState,
      final BlockHeader blockHeader,
      final List<TrieLog> rollBacks,
      final List<TrieLog> rollForwards) {
    final Hash previousRoot = bonsaiWorldState.getWorldStateRootHash();
    final BonsaiWorldStateUpdateAccumulator accumulator = bonsaiWorldState.updater();

    try {
      applyTrieLogs(accumulator, rollBacks, false);
      applyTrieLogs(accumulator, rollForwards, true);

      final BonsaiWorldStateKeyValueStorage worldStateStorage =
          bonsaiWorldState.getWorldStateStorage();
      final DefaultBinaryStateRootCommitter committer = new DefaultBinaryStateRootCommitter();
      final StateRootComputation computation =
          committer.compute(bonsaiWorldState, blockHeader, bonsaiWorldState.updater());

      final BonsaiWorldStateKeyValueStorage.Updater stateUpdater =
          new MigrationScopedWorldStateKeyValueStorage(worldStateStorage).updater();

      computation.applyTo(stateUpdater);
      stateUpdater
          .getWorldStateTransaction()
          .put(
              BINARY_TRIE_BRANCH_STORAGE,
              WORLD_ROOT_HASH_KEY,
              computation.root().getBytes().toArrayUnsafe());
      stateUpdater
          .getWorldStateTransaction()
          .put(
              BINARY_TRIE_BRANCH_STORAGE,
              WORLD_BLOCK_HASH_KEY,
              blockHeader.getBlockHash().getBytes().toArrayUnsafe());

      LOG.atInfo()
          .setMessage("PBT migrator rolled to block {} ({}); root from {} to {}")
          .addArgument(blockHeader.getNumber())
          .addArgument(blockHeader.getBlockHash().toShortLogString())
          .addArgument(previousRoot.toShortLogString())
          .addArgument(computation.root().toShortLogString())
          .log();

      stateUpdater.commit();

    } catch (final Exception e) {
      LOG.error(
          "Failed to apply trie logs and persist state for block {}",
          blockHeader.getBlockHash(),
          e);
      accumulator.revert();
      throw e;
    }
  }

  private void applyTrieLogs(
      final BonsaiWorldStateUpdateAccumulator accumulator,
      final List<TrieLog> trieLogs,
      final boolean forward) {
    if (forward) {
      for (final TrieLog trieLog : trieLogs) {
        accumulator.rollForward(trieLog);
        LOG.info("Attempting rollForward of {}", trieLog.getBlockHash());
      }
    } else {
      for (final TrieLog trieLog : trieLogs) {
        accumulator.rollBack(trieLog);
        LOG.info("Attempting Rollback of {}", trieLog.getBlockHash());
      }
    }
  }

  private Optional<BlockHeader> lastPmtAncestor(final BlockHeader head) {
    BlockHeader current = head;
    while (current != null
        && BinaryTrieForkSupport.isBinaryTrieActive(current.getTimestamp(), binaryTrieMilestone)) {
      if (current.getNumber() == 0) {
        return Optional.empty();
      }
      final Optional<BlockHeader> parent =
          blockchain.getBlockHeader(current.getParentHash()).map(BlockHeader.class::cast);
      if (parent.isEmpty()) {
        return Optional.empty();
      }
      current = parent.get();
    }
    return Optional.ofNullable(current);
  }

  private TrieLogLayer genesisAllocationsToTrieLogLayer(
      final Stream<GenesisAccount> genesisAllocations, final BlockHeader genesisHeader) {
    final TrieLogLayer trieLog =
        new TrieLogLayer()
            .setBlockHash(genesisHeader.getBlockHash())
            .setBlockNumber(genesisHeader.getNumber());

    genesisAllocations.forEach(
        ga -> {
          final Address address = ga.address();
          final Bytes code = ga.code();
          final Hash codeHash = (code == null || code.isEmpty()) ? Hash.EMPTY : Hash.hash(code);

          final AccountValue accountValue =
              new BinaryTrieAccountValue(ga.nonce(), ga.balance(), codeHash);

          trieLog.addAccountChange(address, null, accountValue);

          if (code != null && !code.isEmpty()) {
            trieLog.addCodeChange(address, null, code, genesisHeader.getBlockHash());
            trieLog.addIntroducedCodeHash(codeHash);
          }

          if (ga.storage() != null) {
            ga.storage()
                .forEach(
                    (slotKey, value) -> {
                      final StorageSlotKey storageSlotKey =
                          new StorageSlotKey(Hash.hash(slotKey), Optional.of(slotKey));
                      trieLog.addStorageChange(address, storageSlotKey, null, value);
                    });
          }
        });

    trieLog.freeze();
    return trieLog;
  }
}
