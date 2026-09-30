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
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary.BinaryTrieFactory;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary.DefaultBinaryStateRootCommitter;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.common.BinaryTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BinaryTrieForkSupport;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.verify.Eip8347DualCheckVerifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.MigrationScopedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.StateRootComputation;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background builder of the PBT (binary-trie column) until the PBT fork.
 *
 * <p>Start point: a verified EIP-8347 snapshot when one is configured (dual-checked against its
 * anchor's {@code stateRoot}, then bulk-loaded into the binary column without touching the flat
 * DB), otherwise genesis. From there the column follows the canonical chain one bounded roll at a
 * time: back through trie logs, forward through each block's trie log or, when this node has none
 * (e.g. blocks before a snap sync), through the block's BAL, stored locally or fetched from peers.
 * BALs record post-values only, so they roll forward, never back.
 *
 * <p>The migrator writes the column only while it owns it ({@link PbtColumnOwnership}); the chain
 * claims it at the fork transition. It retires, permanently, once the column sits exactly on the
 * last pre-fork block of the canonical chain.
 */
public class PbtMigrator {
  private static final Logger LOG = LoggerFactory.getLogger(PbtMigrator.class);

  private static final int SHADOW_ROOT_CACHE_SIZE = 1024;

  /** Most blocks applied in one roll, so a long catch-up never holds its whole range in heap. */
  static final int MAX_BLOCKS_PER_ROLL = 128;

  private final BonsaiWorldStateProvider provider;
  private final Blockchain blockchain;
  private final Optional<Long> binaryTrieMilestone;
  private final ScheduledExecutorService scheduler;
  private final Optional<SnapshotBootstrap> snapshotBootstrap;
  private final Function<List<BlockHeader>, Map<Hash, BlockAccessList>> balsFromPeers;
  private final long pollIntervalMs;

  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * In-memory copy of the binary column's cursor: the block whose state the column holds. {@code
   * null} until bootstrapped.
   */
  private volatile BlockHeader cursor;

  /** BALs fetched from peers ahead of the blocks that need them, consumed one block at a time. */
  private final Map<Hash, BlockAccessList> prefetchedBals = new HashMap<>();

  /** Genesis allocations as a trie log, built once: the allocation stream can be read only once. */
  private final Supplier<TrieLog> genesisTrieLog;

  /**
   * Binary roots this migrator computed, keyed by block hash. The binary column only records its
   * cursor, so without this {@code debug_shadowStateRoot} could answer for the tip alone — and the
   * tip moves every block. Memory-only and bounded: a missed root is a missing sample, not a
   * correctness problem.
   */
  private final Cache<Hash, Hash> shadowRoots =
      CacheBuilder.newBuilder().maximumSize(SHADOW_ROOT_CACHE_SIZE).build();

  public PbtMigrator(
      final BonsaiWorldStateProvider provider,
      final Blockchain blockchain,
      final Optional<Long> binaryTrieMilestone,
      final ScheduledExecutorService scheduler,
      final Supplier<Stream<GenesisAccount>> genesisAllocations,
      final Optional<SnapshotBootstrap> snapshotBootstrap,
      final Function<List<BlockHeader>, Map<Hash, BlockAccessList>> balsFromPeers,
      final long pollIntervalMs) {
    this.provider = provider;
    this.blockchain = blockchain;
    this.binaryTrieMilestone = binaryTrieMilestone;
    this.scheduler = scheduler;
    this.snapshotBootstrap = snapshotBootstrap;
    this.balsFromPeers = balsFromPeers;
    this.pollIntervalMs = pollIntervalMs;
    this.genesisTrieLog =
        Suppliers.memoize(
            () ->
                genesisAllocationsToTrieLogLayer(
                    genesisAllocations.get(), blockchain.getBlockHeader(0L).orElseThrow()));
  }

  public void start() {
    if (binaryTrieMilestone.isEmpty()) {
      LOG.info("PBT migrator disabled: no binaryTrieTime fork configured");
      return;
    }
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
    LOG.info("PBT migrator starting; binaryTrieTime milestone={}", binaryTrieMilestone.get());
    scheduler.scheduleWithFixedDelay(
        this::tick, pollIntervalMs, pollIntervalMs, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    started.set(false);
    scheduler.shutdown();
  }

  /** The binary root this migrator computed for a block, while it is still cached. */
  public Optional<Hash> shadowRootFor(final Hash blockHash) {
    return Optional.ofNullable(shadowRoots.getIfPresent(blockHash));
  }

  private void tick() {
    try {
      // Rolls back to back until caught up, each under its own hold of the binary-column lock, so
      // the chain can claim the column between two rolls (at most MAX_BLOCKS_PER_ROLL blocks).
      boolean progressed = true;
      while (started.get() && progressed) {
        final Optional<Boolean> step =
            provider.getBinaryColumnOwnership().runAsMigrator(this::step);
        if (step.isEmpty()) {
          LOG.info("PBT migrator stopping: the chain owns the binary column");
          stop();
          return;
        }
        progressed = step.get();
      }
    } catch (final Throwable t) {
      LOG.atError().setMessage("PBT migrator tick failed (will retry): {}").addArgument(t).log();
    }
  }

  private void bootstrap() {
    final BonsaiWorldStateKeyValueStorage storage = provider.getWorldStateKeyValueStorage();
    final Optional<Hash> existing = storage.getWorldStateBlockHash(TrieBranchType.BINARY);

    if (existing.isPresent()) {
      final Hash baseHash = existing.get();
      final Optional<BlockHeader> base = blockchain.getBlockHeader(baseHash);
      if (base.isEmpty()) {
        LOG.warn(
            "PBT migrator waiting: binary column block {} not in the blockchain yet", baseHash);
        return;
      }
      cursor = base.get();
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

    final Optional<BlockHeader> genesis = blockchain.getBlockHeader(0L);
    if (genesis.isEmpty()) {
      LOG.warn("PBT migrator cannot bootstrap: genesis block header not found");
      return;
    }
    storage.clearBinaryTrie();
    applyAndPersist(genesis.get(), List.of(), List.of(genesisTrieLog.get()));
    // Only once genesis is durably in the column: a failed first write is retried from scratch.
    cursor = genesis.get();
    LOG.info("PBT migrator bootstrapped at genesis {}", cursor.getBlockHash());
  }

  /**
   * Dual-checks the configured EIP-8347 artifacts against their anchor's {@code stateRoot} and, in
   * the same read of the snapshot, writes the PBT into the binary column; the cursor is written
   * last, once the snapshot is accepted. Waits (returns, retried next tick) until the anchor is
   * known, canonical and finalized. On a rejection the column is emptied and the migrator stops, as
   * retrying would re-read the same bad artifact; on any other failure it is emptied and the load
   * retried.
   */
  private void bootstrapFromSnapshot(final SnapshotBootstrap bootstrap) {
    final Optional<BlockHeader> maybeAnchor =
        blockchain.getBlockHeader(bootstrap.anchorBlockHash());
    if (maybeAnchor.isEmpty()) {
      LOG.info("PBT migrator waiting for snapshot anchor {}", bootstrap.anchorBlockHash());
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
      LOG.info(
          "PBT migrator waiting for snapshot anchor {} to be canonical and finalized",
          anchor.getNumber());
      return;
    }

    final BonsaiWorldStateKeyValueStorage storage = provider.getWorldStateKeyValueStorage();
    // No cursor yet: whatever the column and the work dir hold is a previous, interrupted attempt.
    storage.clearBinaryTrie();
    Eip8347ExternalSorter.deleteRecursively(bootstrap.workDir());
    try {
      LOG.info(
          "PBT migrator verifying and loading snapshot {} against anchor {} ({}, stateRoot={})",
          bootstrap.snapshotPath(),
          anchor.getNumber(),
          anchor.getBlockHash(),
          anchor.getStateRoot());
      final ColumnWriter writer = new ColumnWriter(storage);
      final Eip8347DualCheckVerifier.Loaded loaded =
          Eip8347DualCheckVerifier.verifyAndLoad(
              bootstrap.snapshotPath(),
              bootstrap.preimagesPath(),
              Bytes32.wrap(anchor.getStateRoot().getBytes()),
              bootstrap.workDir(),
              writer);
      writer.finish(loaded.pbtRoot(), anchor.getBlockHash());
      cursor = anchor;
      LOG.info(
          "PBT migrator bootstrapped from snapshot at block {} (leaves={}, codes={}, root={})",
          anchor.getNumber(),
          loaded.leafCount(),
          loaded.codeHashes(),
          loaded.pbtRoot());
    } catch (final Eip8347ArtifactVerificationException e) {
      storage.clearBinaryTrie();
      LOG.error("PBT snapshot rejected, migrator stopped: {}", e.getMessage());
      stop();
    } catch (final IOException | RuntimeException e) {
      storage.clearBinaryTrie();
      LOG.error("PBT snapshot bootstrap failed (will retry): {}", e.getMessage(), e);
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
    stop();
    return false;
  }

  /**
   * One roll toward {@code target}, a canonical block (the head or an ancestor), of at most {@link
   * #MAX_BLOCKS_PER_ROLL} blocks. Returns {@code true} when the column moved.
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
    if (cursorCanonical && target.getNumber() - cursor.getNumber() > MAX_BLOCKS_PER_ROLL) {
      step = blockchain.getBlockHeader(cursor.getNumber() + MAX_BLOCKS_PER_ROLL).orElseThrow();
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

      applyAndPersist(reached, rollBacks, forwardTrieLogs);
      cursor = reached;
      return true;
    } catch (final MissingRollData e) {
      LOG.warn("PBT migrator cannot roll to block {} yet: {}", target.getNumber(), e.getMessage());
      return false;
    } catch (final RuntimeException e) {
      LOG.atError()
          .setMessage("PBT migrator failed while rolling to block {} ({}): {}")
          .addArgument(target.getNumber())
          .addArgument(target.getBlockHash())
          .addArgument(e)
          .log();
      return false;
    }
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
      return Optional.of(genesisTrieLog.get());
    }
    return provider.getTrieLogManager().getTrieLogLayer(header.getBlockHash());
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
        if (batch.size() == MAX_BLOCKS_PER_ROLL) {
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
        BinaryTrieFactory.createStateTrie(worldState),
        (address, codeHash) -> worldState.getCode(address, codeHash).orElse(Bytes.EMPTY));
  }

  /**
   * Applies the rolls to one accumulator, computes the binary root and persists it together with
   * the column cursor, never touching the flat DB.
   */
  private void applyAndPersist(
      final BlockHeader blockHeader,
      final List<TrieLog> rollBacks,
      final List<TrieLog> rollForwards) {
    final BonsaiWorldState bonsaiWorldState = provider.getMigrationWorldState();
    final Hash previousRoot = bonsaiWorldState.getWorldStateRootHash();
    final BonsaiWorldStateUpdateAccumulator accumulator = bonsaiWorldState.updater();
    final BonsaiWorldStateKeyValueStorage worldStateStorage =
        bonsaiWorldState.getWorldStateStorage();
    try {
      rollBacks.forEach(accumulator::rollBack);
      rollForwards.forEach(accumulator::rollForward);

      final BonsaiWorldStateKeyValueStorage.Updater stateUpdater =
          new MigrationScopedWorldStateKeyValueStorage(worldStateStorage).updater();
      final StateRootComputation computation =
          new DefaultBinaryStateRootCommitter().compute(bonsaiWorldState, blockHeader, accumulator);
      computation.applyTo(stateUpdater);
      ColumnWriter.putCursor(
          stateUpdater, Bytes32.wrap(computation.root().getBytes()), blockHeader.getBlockHash());
      stateUpdater.commit();
      shadowRoots.put(blockHeader.getBlockHash(), computation.root());

      LOG.atInfo()
          .setMessage("PBT migrator rolled to block {} ({}); root from {} to {}")
          .addArgument(blockHeader.getNumber())
          .addArgument(blockHeader.getBlockHash().toShortLogString())
          .addArgument(previousRoot.toShortLogString())
          .addArgument(computation.root().toShortLogString())
          .log();
    } catch (final RuntimeException e) {
      accumulator.revert();
      throw e;
    }
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
   * {@link NodeUpdater} onto the binary-trie column, for bulk loads that do not fit one
   * transaction.
   *
   * <p>Writes go through {@link MigrationScopedWorldStateKeyValueStorage}, so nothing but
   * binary-trie nodes is written (the flat DB belongs to the MPT). The transaction is committed
   * every {@link #NODES_PER_TRANSACTION} nodes; {@link #finish} writes the column cursor last, so
   * an interrupted load leaves no cursor and is simply redone.
   */
  private static final class ColumnWriter implements NodeUpdater {

    static final int NODES_PER_TRANSACTION = 100_000;

    private final MigrationScopedWorldStateKeyValueStorage storage;
    private BonsaiWorldStateKeyValueStorage.Updater updater;
    private int pending;

    ColumnWriter(final BonsaiWorldStateKeyValueStorage worldStateStorage) {
      this.storage = new MigrationScopedWorldStateKeyValueStorage(worldStateStorage);
      this.updater = storage.updater();
    }

    @Override
    public void store(final Bytes location, final Bytes32 hash, final Bytes value) {
      if (value == null) {
        updater.removeTrieNode(TrieBranchType.BINARY, location);
      } else {
        updater.putTrieNode(TrieBranchType.BINARY, location, hash, value);
      }
      if (++pending >= NODES_PER_TRANSACTION) {
        updater.commit();
        updater = storage.updater();
        pending = 0;
      }
    }

    /** Commits the remaining nodes, then points the binary column at {@code (root, blockHash)}. */
    void finish(final Bytes32 root, final Hash blockHash) {
      updater.commit();
      updater = storage.updater();
      putCursor(updater, root, blockHash);
      updater.commit();
    }

    /**
     * Writes the binary column's cursor, the block whose state the column holds and its PBT root,
     * in {@code updater}'s transaction.
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
              BINARY_TRIE_BRANCH_STORAGE,
              WORLD_BLOCK_HASH_KEY,
              blockHash.getBytes().toArrayUnsafe());
    }
  }
}
