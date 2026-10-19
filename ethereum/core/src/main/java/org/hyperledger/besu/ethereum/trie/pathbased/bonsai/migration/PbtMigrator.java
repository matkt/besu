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

import static com.google.common.base.Preconditions.checkArgument;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.BINARY_TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;

import org.hyperledger.besu.config.GenesisAccount;
import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary.BinaryTrieFactory;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary.DefaultBinaryStateRootCommitter;
import org.hyperledger.besu.ethereum.trie.common.BinaryTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BinaryTrieForkSupport;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.verify.Eip8347DualCheckVerifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.MigrationScopedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.MissingCodeException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.StateRootComputation;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the binary trie column in the background until the PBT fork: from a verified EIP-8347
 * snapshot, or genesis when enabled, then along the canonical chain through trie logs, or BALs
 * where the node has none. The chain claims the column at the fork ({@link PbtColumnOwnership}).
 */
public class PbtMigrator {
  private static final Logger LOG = LoggerFactory.getLogger(PbtMigrator.class);

  /** Default most blocks per roll, so a long catch-up never holds its whole range in heap. */
  static final int DEFAULT_BLOCKS_PER_ROLL = 128;

  /** Wait after a failed snapshot load (e.g. a full disk) before trying again. */
  static final Duration SNAPSHOT_RETRY_DELAY = Duration.ofMinutes(5);

  /** Longest wait between two rolls when the pause follows the last roll's duration. */
  private static final long MAX_ROLL_PAUSE_MS = 10_000L;

  /** Longest wait after rolls that failed in a row. */
  private static final long MAX_ROLL_RETRY_DELAY_MS = 60_000L;

  private final BonsaiWorldStateProvider provider;
  private final Blockchain blockchain;
  private final Optional<Long> binaryTrieMilestone;
  private final ScheduledExecutorService scheduler;
  private final Optional<SnapshotBootstrap> snapshotBootstrap;
  private final boolean fromGenesis;
  private final Function<List<BlockHeader>, Map<Hash, BlockAccessList>> balsFromPeers;
  private final long pollIntervalMs;
  private final Duration snapshotRetryDelay;
  private final Tuning tuning;

  /** Every CPU-bound task of the migrator, off the pools block import uses. */
  private final ForkJoinPool pool;

  private final AtomicBoolean started = new AtomicBoolean(false);

  /** Earliest time, in epoch millis, of the next snapshot load after a failed one. */
  private long nextSnapshotLoadMillis;

  /** Rolls that failed in a row, and the earliest time of the next attempt after them. */
  private int failedRolls;

  private long nextRollMillis;

  /** Last waiting state logged, so that a wait is logged once and not on every tick. */
  private String lastWait = "";

  private final LongSupplier clock;

  /**
   * In-memory copy of the binary column's cursor: the block whose state the column holds. {@code
   * null} until bootstrapped.
   */
  private volatile BlockHeader cursor;

  /** BALs fetched from peers ahead of the blocks that need them, consumed one block at a time. */
  private final Map<Hash, BlockAccessList> prefetchedBals = new HashMap<>();

  private final Supplier<Stream<GenesisAccount>> genesisAllocations;

  public PbtMigrator(
      final BonsaiWorldStateProvider provider,
      final Blockchain blockchain,
      final Optional<Long> binaryTrieMilestone,
      final ScheduledExecutorService scheduler,
      final Supplier<Stream<GenesisAccount>> genesisAllocations,
      final Optional<SnapshotBootstrap> snapshotBootstrap,
      final boolean fromGenesis,
      final Function<List<BlockHeader>, Map<Hash, BlockAccessList>> balsFromPeers,
      final long pollIntervalMs,
      final Tuning tuning) {
    this(
        provider,
        blockchain,
        binaryTrieMilestone,
        scheduler,
        genesisAllocations,
        snapshotBootstrap,
        fromGenesis,
        balsFromPeers,
        pollIntervalMs,
        tuning,
        SNAPSHOT_RETRY_DELAY);
  }

  /** Same, with the wait after a failed snapshot load (tests retry at once). */
  PbtMigrator(
      final BonsaiWorldStateProvider provider,
      final Blockchain blockchain,
      final Optional<Long> binaryTrieMilestone,
      final ScheduledExecutorService scheduler,
      final Supplier<Stream<GenesisAccount>> genesisAllocations,
      final Optional<SnapshotBootstrap> snapshotBootstrap,
      final boolean fromGenesis,
      final Function<List<BlockHeader>, Map<Hash, BlockAccessList>> balsFromPeers,
      final long pollIntervalMs,
      final Tuning tuning,
      final Duration snapshotRetryDelay) {
    this(
        provider,
        blockchain,
        binaryTrieMilestone,
        scheduler,
        genesisAllocations,
        snapshotBootstrap,
        fromGenesis,
        balsFromPeers,
        pollIntervalMs,
        tuning,
        snapshotRetryDelay,
        System::currentTimeMillis);
  }

  /** Same, on {@code clock} (epoch millis). */
  PbtMigrator(
      final BonsaiWorldStateProvider provider,
      final Blockchain blockchain,
      final Optional<Long> binaryTrieMilestone,
      final ScheduledExecutorService scheduler,
      final Supplier<Stream<GenesisAccount>> genesisAllocations,
      final Optional<SnapshotBootstrap> snapshotBootstrap,
      final boolean fromGenesis,
      final Function<List<BlockHeader>, Map<Hash, BlockAccessList>> balsFromPeers,
      final long pollIntervalMs,
      final Tuning tuning,
      final Duration snapshotRetryDelay,
      final LongSupplier clock) {
    this.provider = provider;
    this.blockchain = blockchain;
    this.binaryTrieMilestone = binaryTrieMilestone;
    this.scheduler = scheduler;
    this.snapshotBootstrap = snapshotBootstrap;
    this.fromGenesis = fromGenesis;
    this.balsFromPeers = balsFromPeers;
    this.pollIntervalMs = pollIntervalMs;
    this.snapshotRetryDelay = snapshotRetryDelay;
    this.tuning = tuning;
    this.pool = newPool(tuning.threads());
    this.genesisAllocations = genesisAllocations;
    this.clock = clock;
  }

  public void start() {
    final BonsaiWorldStateKeyValueStorage storage = provider.getWorldStateKeyValueStorage();
    // Once the fork has been crossed, the migrator is permanently retired — even if a reorg
    // brings the chain head back to a PMT-era block. FCU owns the binary column from then on.
    if (storage.isPbtMigratorRetired()) {
      LOG.info(
          "PBT migrator not starting: fork already crossed (migrator retired); FCU owns the binary column");
      return;
    }
    if (!started.compareAndSet(false, true)) {
      LOG.warn("PBT migrator already started");
      return;
    }
    LOG.info(
        "PBT migrator starting; binaryTrieTime milestone={}, {}",
        binaryTrieMilestone.get(),
        tuning);
    scheduler.scheduleWithFixedDelay(
        this::tick, pollIntervalMs, pollIntervalMs, TimeUnit.MILLISECONDS);
  }

  /** Stops ticking; a tick in progress ends on its own. */
  public void stop() {
    started.set(false);
    scheduler.shutdown();
  }

  /**
   * Stops when the node shuts down: interrupts a tick in progress (a snapshot load) and waits for
   * it, so that it never runs on a closed database.
   */
  public void close() {
    started.set(false);
    scheduler.shutdownNow();
    try {
      if (!scheduler.awaitTermination(1, TimeUnit.MINUTES)) {
        LOG.warn("PBT migrator still running a minute after it was stopped");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    pool.shutdownNow();
  }

  private void tick() {
    if (clock.getAsLong() < nextRollMillis) {
      return;
    }
    try {
      // Steps until caught up; each writes under the binary-column lock only, see writeColumn
      boolean progressed = true;
      while (started.get() && progressed) {
        if (provider.getBinaryColumnOwnership().isClaimedByChain()) {
          LOG.info("PBT migrator stopping: the chain owns the binary column");
          prefetchedBals.clear();
          stop();
          return;
        }
        final long stepStart = clock.getAsLong();
        progressed = step();
        if (progressed) {
          // block import has the cores and the disk to itself meanwhile
          final Duration pause =
              tuning
                  .rollPause()
                  .orElseGet(
                      () ->
                          Duration.ofMillis(
                              Math.min(MAX_ROLL_PAUSE_MS, clock.getAsLong() - stepStart)));
          if (!pause.isZero()) {
            pauseBetweenRolls(pause);
          }
        }
      }
    } catch (final InterruptedException e) {
      // The node is closing.
      Thread.currentThread().interrupt();
    } catch (final Throwable t) {
      LOG.atError().setMessage("PBT migrator tick failed (will retry): {}").addArgument(t).log();
    }
  }

  /** Waits between two migration steps. */
  @VisibleForTesting
  void pauseBetweenRolls(final Duration pause) throws InterruptedException {
    Thread.sleep(pause);
  }

  private void bootstrap() {
    final BonsaiWorldStateKeyValueStorage storage = provider.getWorldStateKeyValueStorage();
    final Optional<Hash> existing = storage.getWorldStateBlockHash(TrieBranchType.BINARY);

    if (existing.isPresent()) {
      final Hash baseHash = existing.get();
      final Optional<BlockHeader> base = blockchain.getBlockHeader(baseHash);
      if (base.isEmpty()) {
        logWait("PBT migrator waiting: binary column block " + baseHash + " not in the chain yet");
        return;
      }
      cursor = base.get();
      // a crash after the snapshot cursor left the work dir behind
      snapshotBootstrap
          .map(SnapshotBootstrap::workDir)
          .filter(Files::exists)
          .ifPresent(Eip8347ExternalSorter::deleteRecursively);
      LOG.info(
          "PBT migrator resuming from previously-materialised base block {} ({})",
          cursor.getNumber(),
          baseHash);
      return;
    }

    // No cursor: the column was never completed. Anything it holds (nodes, code reference counts)
    // is an attempt interrupted by a restart, and each bootstrap below empties it before writing.
    if (snapshotBootstrap.isPresent()) {
      bootstrapFromSnapshot(snapshotBootstrap.get());
      return;
    }
    // A replay from genesis needs a trie log or a BAL for every block: only when asked for.
    if (!fromGenesis) {
      LOG.info(
          "PBT migrator stopped: no snapshot (--Xpbt-snapshot-file) and no start from genesis"
              + " (--Xpbt-migration-from-genesis)");
      stop();
      return;
    }

    final Optional<BlockHeader> genesis = blockchain.getBlockHeader(0L);
    if (genesis.isEmpty()) {
      LOG.warn("PBT migrator cannot bootstrap: genesis block header not found");
      return;
    }
    if (!writeColumn(storage::clearBinaryTrie)
        || !applyAndPersist(genesis.get(), List.of(), List.of(genesisTrieLog()))) {
      return;
    }
    // Only once genesis is durably in the column: a failed first write is retried from scratch.
    cursor = genesis.get();
    LOG.info("PBT migrator bootstrapped at genesis {}", cursor.getBlockHash());
  }

  /**
   * Dual-checks the configured EIP-8347 artifacts against their anchor's {@code stateRoot} and, in
   * the same read of the snapshot, writes the PBT into sorted files, added to the binary column
   * once the snapshot is accepted; the cursor is written last. Waits (returns, retried next tick)
   * until the anchor is known, canonical and finalized. On a rejection the migrator stops, as
   * retrying would re-read the same bad artifact; on any other failure the column is emptied and
   * the load retried.
   */
  private void bootstrapFromSnapshot(final SnapshotBootstrap bootstrap) {
    if (clock.getAsLong() < nextSnapshotLoadMillis) {
      return;
    }
    final Optional<BlockHeader> maybeAnchor =
        blockchain.getBlockHeader(bootstrap.anchorBlockHash());
    if (maybeAnchor.isEmpty()) {
      logWait("PBT migrator waiting for snapshot anchor " + bootstrap.anchorBlockHash());
      return;
    }
    final BlockHeader anchor = maybeAnchor.get();
    if (BinaryTrieForkSupport.isBinaryTrieActive(anchor.getTimestamp(), binaryTrieMilestone)) {
      LOG.error(
          "PBT snapshot anchor {} is past the PBT fork; migrator stopped", anchor.getNumber());
      stop();
      return;
    }
    final boolean canonical =
        blockchain
            .getBlockHashByNumber(anchor.getNumber())
            .filter(anchor.getBlockHash()::equals)
            .isPresent();
    final boolean finalized =
        blockchain
            .getFinalized()
            .flatMap(blockchain::getBlockHeader)
            .filter(f -> f.getNumber() >= anchor.getNumber())
            .isPresent();
    if (!canonical || !finalized) {
      logWait(
          "PBT migrator waiting for snapshot anchor "
              + anchor.getNumber()
              + " to be canonical and finalized");
      return;
    }
    if (!Files.isReadable(bootstrap.snapshotPath())
        || !Files.isReadable(bootstrap.preimagesPath())) {
      LOG.error(
          "PBT snapshot {} or preimages {} unreadable, migrator stopped",
          bootstrap.snapshotPath(),
          bootstrap.preimagesPath());
      stop();
      return;
    }
    // a failed load, whatever failed, waits before the next one
    nextSnapshotLoadMillis = clock.getAsLong() + snapshotRetryDelay.toMillis();
    if (!hasRoomForLoad(bootstrap)) {
      return;
    }

    final BonsaiWorldStateKeyValueStorage storage = provider.getWorldStateKeyValueStorage();
    // No cursor yet: whatever the column and the work dir hold is a previous, interrupted attempt.
    if (!writeColumn(storage::clearBinaryTrie)) {
      return;
    }
    Eip8347ExternalSorter.deleteRecursively(bootstrap.workDir());
    // The writer is closed, and its files deleted, before a catch below runs.
    try (final SnapshotColumnWriter writer =
        new SnapshotColumnWriter(storage, bootstrap.workDir())) {
      LOG.info(
          "PBT migrator verifying and loading snapshot {} against anchor {} ({}, stateRoot={})",
          bootstrap.snapshotPath(),
          anchor.getNumber(),
          anchor.getBlockHash(),
          anchor.getStateRoot());
      final Eip8347DualCheckVerifier.Loaded loaded =
          Eip8347DualCheckVerifier.verifyAndLoad(
              bootstrap.snapshotPath(),
              bootstrap.preimagesPath(),
              Bytes32.wrap(anchor.getStateRoot().getBytes()),
              bootstrap.workDir(),
              writer,
              tuning.sortBufferBytes(),
              pool);
      if (!writeColumn(() -> writer.finish(loaded.pbtRoot(), anchor.getBlockHash()))) {
        return;
      }
      cursor = anchor;
      LOG.info(
          "PBT migrator bootstrapped from snapshot at block {} (leaves={}, codes={}, root={})",
          anchor.getNumber(),
          loaded.leafCount(),
          loaded.codeHashes(),
          loaded.pbtRoot());
    } catch (final Eip8347ArtifactVerificationException e) {
      // Nothing reached the column: its files are only added once the snapshot is accepted.
      LOG.error("PBT snapshot rejected, migrator stopped: {}", e.getMessage());
      stop();
    } catch (final UnsupportedOperationException e) {
      LOG.error("PBT snapshot cannot be loaded into this storage, migrator stopped", e);
      stop();
    } catch (final IOException | RuntimeException e) {
      writeColumn(storage::clearBinaryTrie);
      if (!started.get()) {
        LOG.info("PBT snapshot load stopped with the node");
        return;
      }
      LOG.error(
          "PBT snapshot bootstrap failed (will retry in {} min): {}",
          snapshotRetryDelay.toMinutes(),
          e.getMessage(),
          e);
    }
  }

  /** One migration step; {@code true} when it moved the column, so another step may follow. */
  private boolean step() {
    if (cursor == null) {
      bootstrap();
      return cursor != null;
    }
    return migrateToward(blockchain.getChainHeadHeader());
  }

  private boolean migrateToward(final BlockHeader head) {
    if (!BinaryTrieForkSupport.isBinaryTrieActive(head.getTimestamp(), binaryTrieMilestone)) {
      return rollToward(head);
    }
    // The head is past the fork: finish on the last pre-fork block of this canonical chain, then
    // retire. The cursor must equal it by hash, not just by height, or it may sit on a stale fork.
    final Optional<BlockHeader> lastPmt = lastPmtAncestor(head);
    if (lastPmt.isPresent() && !lastPmt.get().getBlockHash().equals(cursor.getBlockHash())) {
      return rollToward(lastPmt.get());
    }
    // On the last pre-fork block, or the fork is active from genesis (no pre-fork state to build).
    LOG.info(
        "PBT migrator retired at block {} ({}); the chain owns the binary column",
        cursor.getNumber(),
        cursor.getBlockHash());
    provider.getWorldStateKeyValueStorage().markPbtMigratorRetired();
    prefetchedBals.clear();
    stop();
    return false;
  }

  /**
   * One roll toward {@code target}, a canonical block (the head or an ancestor), of at most {@code
   * blocksPerRoll} blocks. Returns {@code true} when the column moved.
   */
  private boolean rollToward(final BlockHeader target) {
    if (target.getBlockHash().equals(cursor.getBlockHash())) {
      return false;
    }
    final boolean cursorCanonical =
        blockchain
            .getBlockHashByNumber(cursor.getNumber())
            .filter(cursor.getBlockHash()::equals)
            .isPresent();
    BlockHeader step = target;
    if (!cursorCanonical) {
      // off the canonical chain: back to the common ancestor first, the next roll is capped
      step = canonicalAncestor(cursor);
    } else if (target.getNumber() - cursor.getNumber() > tuning.blocksPerRoll()) {
      step = blockchain.getBlockHeader(cursor.getNumber() + tuning.blocksPerRoll()).orElseThrow();
    }
    return rollOnce(step);
  }

  /**
   * One roll from the cursor toward {@code target}: back to the common ancestor through trie logs,
   * then forward through consecutive blocks that have a trie log, or else through a single block's
   * BAL, built against the PBT the column holds for its parent. Returns {@code false}, leaving the
   * cursor where it was, when the next block cannot be rolled yet.
   */
  private boolean rollOnce(final BlockHeader target) {
    try {
      final List<TrieLog> rollBacks = new ArrayList<>();
      final List<BlockHeader> rollForwards = new ArrayList<>();
      BlockHeader persistedHeader = cursor;
      BlockHeader targetHeader = target;
      while (persistedHeader.getNumber() > targetHeader.getNumber()) {
        rollBacks.add(trieLogForRollBack(persistedHeader));
        persistedHeader = parentOf(persistedHeader);
      }
      while (persistedHeader.getNumber() < targetHeader.getNumber()) {
        rollForwards.add(targetHeader);
        targetHeader = parentOf(targetHeader);
      }
      while (!persistedHeader.getBlockHash().equals(targetHeader.getBlockHash())) {
        rollForwards.add(targetHeader);
        targetHeader = parentOf(targetHeader);
        rollBacks.add(trieLogForRollBack(persistedHeader));
        persistedHeader = parentOf(persistedHeader);
      }
      final BlockHeader ancestor = persistedHeader;
      // Collected from target down to the ancestor: roll forward oldest first.
      final List<BlockHeader> forward = rollForwards.reversed();

      final List<TrieLog> forwardTrieLogs = new ArrayList<>();
      BlockHeader reached = ancestor;
      for (final BlockHeader header : forward) {
        final Optional<TrieLog> trieLog = trieLog(header);
        if (trieLog.isEmpty()) {
          break;
        }
        forwardTrieLogs.add(trieLog.get());
        reached = header;
      }
      // No trie log for the next block: roll it through its BAL, which applies on committed parent
      // state only. After rollbacks, this roll lands on the common ancestor and the next one does.
      if (forwardTrieLogs.isEmpty() && !forward.isEmpty() && rollBacks.isEmpty()) {
        reached = forward.getFirst();
        forwardTrieLogs.add(balTrieLog(reached, forward));
      }

      if (!applyAndPersist(reached, rollBacks, forwardTrieLogs)) {
        return false;
      }
      cursor = reached;
      failedRolls = 0;
      return true;
    } catch (final MissingRollData e) {
      rollFailed("PBT migrator cannot roll to block " + target.getNumber() + " yet", e);
      return false;
    } catch (final RuntimeException e) {
      rollFailed(
          "PBT migrator failed while rolling to block "
              + target.getNumber()
              + " ("
              + target.getBlockHash()
              + ")",
          e);
      return false;
    }
  }

  // the first failure in a row is logged, the next ones wait longer each time and log at debug
  private void rollFailed(final String message, final RuntimeException e) {
    failedRolls++;
    nextRollMillis =
        clock.getAsLong() + Math.min(MAX_ROLL_RETRY_DELAY_MS, 1000L << Math.min(failedRolls, 6));
    if (failedRolls == 1) {
      LOG.warn("{}: {}", message, e.getMessage());
    } else {
      LOG.debug("{} ({} failures in a row)", message, failedRolls, e);
    }
  }

  // a wait is logged once, then at debug while it lasts
  private void logWait(final String message) {
    if (message.equals(lastWait)) {
      LOG.debug(message);
    } else {
      LOG.info(message);
      lastWait = message;
    }
  }

  /** The highest ancestor of {@code header} on the canonical chain. */
  private BlockHeader canonicalAncestor(final BlockHeader header) {
    BlockHeader current = header;
    while (!blockchain
        .getBlockHashByNumber(current.getNumber())
        .filter(current.getBlockHash()::equals)
        .isPresent()) {
      current = parentOf(current);
    }
    return current;
  }

  // the load needs about 4 times the snapshot in its work dir, at its peak
  private boolean hasRoomForLoad(final SnapshotBootstrap bootstrap) {
    try {
      final Path dir = bootstrap.workDir().toAbsolutePath().getParent();
      final long needed = 4 * Files.size(bootstrap.snapshotPath());
      final long usable = Files.getFileStore(dir).getUsableSpace();
      if (usable < needed) {
        LOG.error(
            "PBT snapshot load needs {} bytes free under {}, {} left; retrying later",
            needed,
            dir,
            usable);
        return false;
      }
    } catch (final IOException e) {
      LOG.debug("Free space for the PBT snapshot load unknown", e);
    }
    return true;
  }

  // a write of the migrator, refused once the chain has claimed the column, which stops it
  private boolean writeColumn(final Runnable write) {
    if (provider.getBinaryColumnOwnership().writeAsMigrator(write)) {
      return true;
    }
    LOG.info("PBT migrator stopping: the chain owns the binary column");
    stop();
    return false;
  }

  private BlockHeader parentOf(final BlockHeader header) {
    return blockchain
        .getBlockHeader(header.getParentHash())
        .orElseThrow(() -> new MissingRollData("no header for parent of " + header.getNumber()));
  }

  private TrieLog trieLogForRollBack(final BlockHeader header) {
    return trieLog(header)
        .orElseThrow(
            () -> new MissingRollData("no trie log to roll back block " + header.getNumber()));
  }

  private Optional<TrieLog> trieLog(final BlockHeader header) {
    if (header.getNumber() == 0L) {
      return Optional.of(genesisTrieLog());
    }
    // main's trie log format keeps no slot key, which the binary trie keys slots by
    return provider
        .getTrieLogManager()
        .getTrieLogLayer(header.getBlockHash())
        .filter(PbtMigrator::hasSlotKeys);
  }

  private static boolean hasSlotKeys(final TrieLog trieLog) {
    return trieLog.getStorageChanges().values().stream()
        .flatMap(slots -> slots.keySet().stream())
        .allMatch(slot -> slot.getSlotKey().isPresent());
  }

  private TrieLog genesisTrieLog() {
    return genesisAllocationsToTrieLogLayer(
        genesisAllocations.get(), blockchain.getBlockHeader(0L).orElseThrow());
  }

  /**
   * A roll-forward trie log for {@code header} built from its BAL: stored locally, else fetched
   * from peers together with the BALs of the next blocks of {@code upcoming} that lack a trie log,
   * so a catch-up asks the network once per batch rather than once per block.
   */
  private TrieLog balTrieLog(final BlockHeader header, final List<BlockHeader> upcoming) {
    final Hash hash = header.getBlockHash();
    BlockAccessList bal =
        blockchain.getBlockAccessList(hash).orElseGet(() -> prefetchedBals.remove(hash));
    if (bal == null) {
      final List<BlockHeader> batch = new ArrayList<>();
      for (final BlockHeader next : upcoming) {
        if (batch.size() == tuning.blocksPerRoll()) {
          break;
        }
        if (trieLog(next).isEmpty()
            && blockchain.getBlockAccessList(next.getBlockHash()).isEmpty()) {
          batch.add(next);
        }
      }
      prefetchedBals.clear();
      prefetchedBals.putAll(balsFromPeers.apply(batch));
      LOG.info(
          "PBT migrator fetched {} of {} BAL(s) from peers", prefetchedBals.size(), batch.size());
      bal = prefetchedBals.remove(hash);
    }
    if (bal == null) {
      throw new MissingRollData(
          "neither trie log nor BAL to roll forward block " + header.getNumber());
    }
    final BonsaiWorldState worldState = provider.getMigrationWorldState();
    return BalTrieLogs.forward(
        header,
        bal,
        BinaryTrieFactory.createStateTrie(worldState, pool),
        (address, codeHash) ->
            worldState
                .getCode(address, codeHash)
                .map(Code::getBytes)
                .orElseThrow(() -> new MissingCodeException(address, codeHash)));
  }

  /**
   * Applies the rolls to one accumulator, computes the binary root and persists it together with
   * the column cursor, never touching the flat DB. Returns {@code false}, writing nothing, once the
   * chain owns the column.
   */
  private boolean applyAndPersist(
      final BlockHeader blockHeader,
      final List<TrieLog> rollBacks,
      final List<TrieLog> rollForwards) {
    final BonsaiWorldState bonsaiWorldState = provider.getMigrationWorldState();
    final Hash previousRoot = bonsaiWorldState.getWorldStateRootHash();
    final BonsaiWorldStateUpdateAccumulator accumulator =
        (BonsaiWorldStateUpdateAccumulator) bonsaiWorldState.updater();
    final BonsaiWorldStateKeyValueStorage worldStateStorage =
        bonsaiWorldState.getWorldStateStorage();
    final BonsaiWorldStateKeyValueStorage.Updater stateUpdater =
        new MigrationScopedWorldStateKeyValueStorage(worldStateStorage).updater();
    final StateRootComputation computation;
    try {
      rollBacks.forEach(accumulator::rollBack);
      rollForwards.forEach(accumulator::rollForward);
      computation =
          new DefaultBinaryStateRootCommitter(pool)
              .compute(bonsaiWorldState, blockHeader, accumulator);
      computation.applyTo(stateUpdater);
      putCursor(
          stateUpdater, Bytes32.wrap(computation.root().getBytes()), blockHeader.getBlockHash());
    } catch (final RuntimeException e) {
      stateUpdater.rollback();
      accumulator.revert();
      throw e;
    }
    // computed without the lock: the chain waits for the commit at most
    if (!writeColumn(stateUpdater::commit)) {
      stateUpdater.rollback();
      return false;
    }
    // following the head, one block at a time, is logged at debug
    (rollBacks.size() + rollForwards.size() > 1 ? LOG.atInfo() : LOG.atDebug())
        .setMessage("PBT migrator rolled to block {} ({}); root from {} to {}")
        .addArgument(blockHeader.getNumber())
        .addArgument(blockHeader.getBlockHash().toShortLogString())
        .addArgument(previousRoot.toShortLogString())
        .addArgument(computation.root().toShortLogString())
        .log();
    return true;
  }

  /**
   * The last pre-fork block on {@code head}'s chain, or empty when the fork is active from genesis.
   *
   * @throws MissingRollData when a header on the way is missing
   */
  private Optional<BlockHeader> lastPmtAncestor(final BlockHeader head) {
    BlockHeader current = head;
    while (BinaryTrieForkSupport.isBinaryTrieActive(current.getTimestamp(), binaryTrieMilestone)) {
      if (current.getNumber() == 0) {
        return Optional.empty();
      }
      current = parentOf(current);
    }
    return Optional.of(current);
  }

  private static TrieLogLayer genesisAllocationsToTrieLogLayer(
      final Stream<GenesisAccount> genesisAllocations, final BlockHeader genesisHeader) {
    final TrieLogLayer trieLog =
        new TrieLogLayer()
            .setBlockHash(genesisHeader.getBlockHash())
            .setBlockNumber(genesisHeader.getNumber());

    genesisAllocations.forEach(
        ga -> {
          final Address address = ga.address();
          final Bytes code = ga.code();
          final Hash codeHash = code == null ? Hash.EMPTY : Hash.hash(code);

          final AccountValue accountValue =
              new BinaryTrieAccountValue(ga.nonce(), ga.balance(), codeHash);

          trieLog.addAccountChange(address, null, accountValue);

          if (code != null && !code.isEmpty()) {
            trieLog.addCodeChange(address, null, code, genesisHeader.getBlockHash());
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

  /** A block the roll needs has no usable trie log / BAL yet; retried on a later tick. */
  private static final class MissingRollData extends RuntimeException {
    MissingRollData(final String message) {
      super(message);
    }
  }

  /**
   * EIP-8347 artifacts the PBT migrator starts from instead of genesis: a PBT snapshot and its
   * preimages, anchored at a finalized pre-fork block.
   *
   * @param snapshotPath typed PBT snapshot
   * @param preimagesPath preimage file of the same anchor
   * @param anchorBlockHash block whose {@code stateRoot} the artifacts are anchored to
   * @param workDir where verification spills its external sorts, next to the database (see {@link
   *     #workDir(Path)}); emptied before each attempt
   */
  @SuppressWarnings("MethodInputParametersMustBeFinal") // compact record constructor
  public record SnapshotBootstrap(
      Path snapshotPath, Path preimagesPath, Hash anchorBlockHash, Path workDir) {
    public SnapshotBootstrap {
      Objects.requireNonNull(snapshotPath, "snapshotPath");
      Objects.requireNonNull(preimagesPath, "preimagesPath");
      Objects.requireNonNull(anchorBlockHash, "anchorBlockHash");
      Objects.requireNonNull(workDir, "workDir");
    }

    /**
     * Spill directory of the EIP-8347 tools under a node's data directory: a sibling of {@code
     * database}, so it sits on the same disk without mixing files into RocksDB's own directory.
     *
     * @param dataDir the node's {@code --data-path}
     * @return {@code <dataDir>/pbt-migration}
     */
    public static Path workDir(final Path dataDir) {
      return dataDir.resolve("pbt-migration");
    }
  }

  /**
   * How much of the node the migrator takes, so that block import keeps up.
   *
   * @param threads threads of the migrator's pool, for the snapshot load and the binary roots
   * @param blocksPerRoll most blocks applied in one roll
   * @param rollPause wait after each migration step that moved the column; empty to wait as long as
   *     the step took
   * @param sortBufferBytes in-heap buffer of each external sort of the snapshot load
   */
  @SuppressWarnings("MethodInputParametersMustBeFinal") // compact record constructor
  public record Tuning(
      int threads, int blocksPerRoll, Optional<Duration> rollPause, long sortBufferBytes) {

    /** Most blocks per roll: the trie logs a node keeps by default. */
    public static final int MAX_BLOCKS_PER_ROLL = 512;

    /** Largest buffer of an external sort. */
    public static final long MAX_SORT_BUFFER_BYTES = 1L << 30;

    /** Half the cores, 128 blocks per roll, a pause as long as the step and 64 MiB sort buffers. */
    public static final Tuning DEFAULT =
        new Tuning(
            Math.max(1, Runtime.getRuntime().availableProcessors() / 2),
            DEFAULT_BLOCKS_PER_ROLL,
            Optional.empty(),
            Eip8347ExternalSorter.DEFAULT_BUFFER_BYTES);

    public Tuning {
      final int cores = Runtime.getRuntime().availableProcessors();
      checkArgument(
          threads > 0 && threads <= cores,
          "PBT migration threads must be between 1 and %s, got %s",
          cores,
          threads);
      checkArgument(
          blocksPerRoll > 0 && blocksPerRoll <= MAX_BLOCKS_PER_ROLL,
          "PBT migration blocks per roll must be between 1 and %s, got %s",
          MAX_BLOCKS_PER_ROLL,
          blocksPerRoll);
      checkArgument(
          rollPause.map(pause -> !pause.isNegative()).orElse(true),
          "PBT migration roll pause must not be negative, got %s",
          rollPause);
      checkArgument(
          sortBufferBytes > 0 && sortBufferBytes <= MAX_SORT_BUFFER_BYTES,
          "PBT migration sort buffer must be between 1 byte and %s bytes, got %s bytes",
          MAX_SORT_BUFFER_BYTES,
          sortBufferBytes);
    }
  }

  private static ForkJoinPool newPool(final int threads) {
    final AtomicInteger counter = new AtomicInteger();
    return new ForkJoinPool(
        threads,
        pool -> {
          final ForkJoinWorkerThread worker =
              ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
          worker.setName("pbt-migration-" + counter.getAndIncrement());
          worker.setDaemon(true);
          return worker;
        },
        null,
        false);
  }

  /**
   * Writes the binary column's cursor, the block whose state the column holds and its PBT root, in
   * {@code updater}'s transaction, and records that root as the block's shadow root.
   */
  static void putCursor(
      final BonsaiWorldStateKeyValueStorage.Updater updater,
      final Bytes32 root,
      final Hash blockHash) {
    updater
        .getWorldStateTransaction()
        .put(BINARY_TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY, root.toArrayUnsafe());
    updater
        .getWorldStateTransaction()
        .put(
            BINARY_TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY, blockHash.getBytes().toArrayUnsafe());
    updater
        .getWorldStateTransaction()
        .put(
            BINARY_TRIE_BRANCH_STORAGE,
            BonsaiWorldStateKeyValueStorage.shadowStateRootKey(blockHash),
            root.toArrayUnsafe());
  }
}
