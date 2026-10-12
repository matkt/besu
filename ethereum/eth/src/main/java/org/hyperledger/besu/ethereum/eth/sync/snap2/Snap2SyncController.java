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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.DefaultBlockchain;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlockAccessList;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.TrailingPeerRequirements;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BalFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BlockImporter;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.ChainHeadSource;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderBackfill;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.ReorgRecovery;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateDiff;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.WorldStateTrieBuilder;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncController;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SNAP2 sync: one process downloading the chain and the state together, sharing one request
 * scheduler and one header chain anchored on the trusted head.
 *
 * <ul>
 *   <li>the pivot trails the head; headers between the pivot and the head prove its ancestry;
 *   <li>the state is downloaded flat at the pivot, and each pivot move applies the block access
 *       lists of the blocks skipped to what is already persisted;
 *   <li>headers below the first pivot are back-filled while bodies and receipts are imported up to
 *       the pivot;
 *   <li>once the state is complete, the pivot is moved next to the head, the tries are built from
 *       the flat state and the root checked against that pivot, which becomes the chain head.
 * </ul>
 *
 * <p>When the state cannot be recovered, the sync starts over with a new {@link Attempt}; an
 * abandoned attempt stops all its components and never touches the next one.
 */
public final class Snap2SyncController implements SnapSyncController {

  private static final Logger LOG = LoggerFactory.getLogger(Snap2SyncController.class);
  private static final int MAX_BLOCKS_PER_MOVE = 128;
  private static final int MAX_REORG_DEPTH = 1024;
  private static final int MAX_RESTARTS = 3;
  // orphaned lists are optional for peers: when none has them, start over instead of waiting
  private static final int ORPHANED_BAL_ROUNDS = 4;
  private static final long PROGRESS_LOG_SECONDS = 30;

  private final Snap2Configuration config;
  private final ProtocolSchedule protocolSchedule;
  private final MutableBlockchain blockchain;
  private final EthPeers ethPeers;
  private final SyncState syncState;
  private final SyncStateStorage storage;
  private final ChainHeadSource heads;
  private final MetricsSystem metricsSystem;
  private final Snap2Marker marker;
  private final boolean headersToCheckpointOnly;
  private final boolean indexTransactions;
  private final SyncDurationMetrics syncDurationMetrics;
  private final Supplier<RequestSender> senders;

  private final ExecutorService executor =
      Executors.newCachedThreadPool(Thread.ofPlatform().name("snap2-sync-", 0).daemon().factory());
  private final ScheduledExecutorService control =
      Executors.newSingleThreadScheduledExecutor(
          Thread.ofPlatform().name("snap2-control").daemon().factory());
  private final CompletableFuture<SnapSyncProcessState> result = new CompletableFuture<>();

  private RequestSender scheduler;
  private HeaderFetcher fetcher;
  private BalFetcher balFetcher;
  private volatile Attempt current;
  private volatile boolean stopped;
  private int restarts;
  private long moves;

  public Snap2SyncController(
      final Snap2Configuration config,
      final ProtocolSchedule protocolSchedule,
      final MutableBlockchain blockchain,
      final EthPeers ethPeers,
      final SyncState syncState,
      final SyncStateStorage storage,
      final ChainHeadSource heads,
      final MetricsSystem metricsSystem,
      final Snap2Marker marker,
      final boolean headersToCheckpointOnly,
      final boolean indexTransactions,
      final SyncDurationMetrics syncDurationMetrics,
      final Supplier<RequestSender> senders) {
    this.config = config;
    this.protocolSchedule = protocolSchedule;
    this.blockchain = blockchain;
    this.ethPeers = ethPeers;
    this.syncState = syncState;
    this.storage = storage;
    this.heads = heads;
    this.metricsSystem = metricsSystem;
    this.marker = marker;
    this.headersToCheckpointOnly = headersToCheckpointOnly;
    this.indexTransactions = indexTransactions;
    this.syncDurationMetrics = syncDurationMetrics;
    this.senders = senders;
  }

  @Override
  public CompletableFuture<SnapSyncProcessState> start() {
    LOG.info("Starting SNAP2 sync");
    syncDurationMetrics.startTimer(
        SyncDurationMetrics.Labels.SNAP_INITIAL_WORLD_STATE_DOWNLOAD_DURATION);
    syncDurationMetrics.startTimer(SyncDurationMetrics.Labels.CHAIN_DOWNLOAD_DURATION);
    CompletableFuture.runAsync(this::prepare, executor)
        .thenCompose(unused -> run(Resume.NONE))
        .whenComplete(
            (pivot, error) -> {
              if (error != null) {
                if (!stopped) {
                  LOG.error("SNAP2 sync failed", error);
                }
                result.completeExceptionally(error);
              } else {
                result.complete(new SnapSyncProcessState(pivot));
              }
              shutdown();
            });
    return result;
  }

  @Override
  public void stop() {
    if (!stopped) {
      stopped = true;
      LOG.info("Stopping SNAP2 sync");
      result.cancel(false);
      shutdown();
    }
  }

  @Override
  public void deletePivotSyncState() {
    marker.delete();
  }

  @Override
  public Optional<TrailingPeerRequirements> calculateTrailingPeerRequirements() {
    return Optional.empty();
  }

  private void prepare() {
    marker.create();
    scheduler = senders.get();
    fetcher = new HeaderFetcher(scheduler, protocolSchedule, heads, config.headersPerRequest());
    balFetcher = new BalFetcher(scheduler, config.balBatchSize(), config.balResponseBytes(), 60);
    control.scheduleWithFixedDelay(
        this::logProgress, PROGRESS_LOG_SECONDS, PROGRESS_LOG_SECONDS, TimeUnit.SECONDS);
    control.scheduleWithFixedDelay(
        this::maybeMoveSafely,
        config.pollInterval().toMillis(),
        config.pollInterval().toMillis(),
        TimeUnit.MILLISECONDS);
  }

  // one attempt of the whole sync; the state starts over if it cannot be recovered
  private CompletableFuture<BlockHeader> run(final Resume resume) {
    storage.prepareForSync();
    return waitForPeers()
        .thenCompose(unused -> selectPivot())
        .thenComposeAsync(pivot -> new Attempt(pivot, resume).start(), executor)
        .exceptionallyComposeAsync(
            error -> {
              final Attempt failed = current;
              if (failed != null) {
                failed.cancel();
              }
              if (stopped
                  || restarts >= MAX_RESTARTS
                  || !(unwrap(error) instanceof RestartException restart)) {
                return CompletableFuture.failedFuture(error);
              }
              restarts++;
              LOG.warn("Restarting the SNAP2 sync from scratch: {}", restart.getMessage());
              return run(failed == null ? resume : failed.resume(restart.canonicalUpTo()));
            },
            executor);
  }

  private CompletableFuture<Void> waitForPeers() {
    if (stopped) {
      return CompletableFuture.failedFuture(new CancellationException("stopped"));
    }
    if (scheduler.peerCount(Protocol.SNAP) >= config.minPeers()) {
      return CompletableFuture.completedFuture(null);
    }
    LOG.info(
        "Waiting for {} snap/2 peers, {} connected peers so far",
        config.minPeers(),
        ethPeers.peerCount());
    return delay(5).thenCompose(unused -> waitForPeers());
  }

  // the pivot trails the trusted head, and the blocks after it must carry block access lists
  private CompletableFuture<BlockHeader> selectPivot() {
    if (stopped) {
      return CompletableFuture.failedFuture(new CancellationException("stopped"));
    }
    final Optional<Hash> head = heads.headHash();
    if (head.isEmpty()) {
      LOG.info("Waiting for the consensus client to send a head");
      return delay(5).thenCompose(unused -> selectPivot());
    }
    return fetcher
        .byHash(head.get(), Priority.PIVOT)
        .thenCompose(
            headHeader -> {
              final long target = Math.max(1, headHeader.getNumber() - config.pivotDistance());
              return fetcher.chainDown(headHeader, target, Priority.PIVOT);
            })
        .thenCompose(
            chain -> {
              final BlockHeader pivot = chain.getFirst();
              final BlockHeader next = chain.size() > 1 ? chain.get(1) : pivot;
              if (next.getBalHash().isEmpty()) {
                LOG.info(
                    "Block {} has no block access list yet, SNAP2 waits for the fork that adds them",
                    next.getNumber());
                return delay(30).thenCompose(unused -> selectPivot());
              }
              LOG.info("Selected pivot block {} ({})", pivot.getNumber(), pivot.getHash());
              return CompletableFuture.completedFuture(pivot);
            })
        .exceptionallyCompose(
            error -> {
              if (stopped) {
                return CompletableFuture.failedFuture(error);
              }
              LOG.info("Pivot selection failed, retrying: {}", unwrap(error).getMessage());
              return delay(5).thenCompose(unused -> selectPivot());
            });
  }

  private void maybeMoveSafely() {
    try {
      final Attempt attempt = current;
      if (attempt != null) {
        attempt.maybeMove();
      }
    } catch (final RuntimeException e) {
      LOG.warn("Pivot move check failed", e);
    }
  }

  // what a new attempt keeps from a failed one: the canonical blocks imported, the headers indexed
  private record Resume(long importFrom, long indexedUpTo) {
    private static final Resume NONE = new Resume(0, -1);
  }

  /** One run of the sync from a pivot, owning its download components. */
  private final class Attempt {
    private final BlockHeader firstPivot;
    private final Resume resume;
    private final StateSync stateSync = new StateSync(storage, scheduler, config);
    private final CompletableFuture<BlockHeader> restart = new CompletableFuture<>();
    private final CompletableFuture<Void> headersDone = new CompletableFuture<>();
    private BlockHeader floor;
    private long importStart;
    private volatile long indexedUpTo = -1;
    private volatile HeaderBackfill backfill;
    private volatile BlockImporter importer;
    private volatile WorldStateTrieBuilder trieBuilder;
    private volatile boolean cancelled;
    private volatile boolean moving;
    private volatile boolean finalizing;
    private volatile long finalTarget = -1;

    private Attempt(final BlockHeader firstPivot, final Resume resume) {
      this.firstPivot = firstPivot;
      this.resume = resume;
    }

    private CompletableFuture<BlockHeader> start() {
      current = this;
      floor = chainFloor();
      prepareChain();
      storeHeaders(List.of(firstPivot));
      final CompletableFuture<Void> state = stateSync.start(firstPivot);
      startBackfill(firstPivot);
      importer =
          new BlockImporter(
              scheduler,
              protocolSchedule,
              blockchain,
              importStart,
              config.bodiesPerRequest(),
              config.chainParallelism(),
              indexTransactions,
              metricsSystem);
      final CompletableFuture<Void> blocks = importLoop(importer);

      final CompletableFuture<BlockHeader> main =
          state
              .thenCompose(unused -> settleMoves())
              .thenCompose(unused -> finalCatchUp())
              .thenApplyAsync(unused -> buildTries(), executor)
              .thenCompose(
                  finalPivot -> {
                    finalTarget = finalPivot.getNumber();
                    syncDurationMetrics.stopTimer(
                        SyncDurationMetrics.Labels.SNAP_INITIAL_WORLD_STATE_DOWNLOAD_DURATION);
                    LOG.info("Waiting for the chain download to reach block {}", finalTarget);
                    return CompletableFuture.allOf(headersDone, blocks)
                        .thenApply(unused -> finalPivot);
                  })
              .thenApply(
                  finalPivot -> {
                    syncDurationMetrics.stopTimer(
                        SyncDurationMetrics.Labels.CHAIN_DOWNLOAD_DURATION);
                    LOG.info("SNAP2 sync complete at block {}", finalPivot.getNumber());
                    return finalPivot;
                  });
      // either the sync completes, or an unrecoverable reorg asks for a restart
      final CompletableFuture<BlockHeader> outcome = new CompletableFuture<>();
      main.whenComplete(
          (finalPivot, error) -> {
            if (error != null) {
              outcome.completeExceptionally(error);
            } else {
              outcome.complete(finalPivot);
            }
          });
      restart.whenComplete(
          (unused, error) -> {
            if (error != null) {
              outcome.completeExceptionally(error);
            }
          });
      return outcome;
    }

    private void cancel() {
      cancelled = true;
      stateSync.stop();
      final HeaderBackfill headers = backfill;
      if (headers != null) {
        headers.stop();
      }
      final BlockImporter blocks = importer;
      if (blocks != null) {
        blocks.close();
      }
      final WorldStateTrieBuilder builder = trieBuilder;
      if (builder != null) {
        builder.cancel();
      }
    }

    private boolean inactive() {
      return stopped || cancelled;
    }

    // the imported blocks up to a canonical one are kept for the next attempt
    private Resume resume(final long canonicalUpTo) {
      final BlockImporter blocks = importer;
      final long imported = blocks == null ? importStart : blocks.nextBlock();
      return new Resume(Math.min(imported, canonicalUpTo + 1), indexedUpTo);
    }

    private <T> CompletableFuture<T> abandoned() {
      return CompletableFuture.failedFuture(new CancellationException("attempt abandoned"));
    }

    // genesis, or the checkpoint stored with its total difficulty so imports can start after it
    private BlockHeader chainFloor() {
      final Optional<Checkpoint> checkpoint = syncState.getCheckpoint();
      if (checkpoint.isEmpty() || checkpoint.get().blockNumber() <= 0) {
        final BlockHeader genesis = blockchain.getGenesisBlockHeader();
        importStart = genesis.getNumber() + 1;
        return genesis;
      }
      final BlockHeader header =
          blockchain
              .getBlockHeader(checkpoint.get().blockHash())
              .orElseGet(() -> fetcher.byHash(checkpoint.get().blockHash(), Priority.CHAIN).join());
      if (header.getNumber() != checkpoint.get().blockNumber()) {
        throw new IllegalStateException(
            "checkpoint "
                + checkpoint.get().blockHash()
                + " is not at block "
                + checkpoint.get().blockNumber());
      }
      blockchain.unsafeStoreHeader(header, checkpoint.get().totalDifficulty());
      importStart = header.getNumber() + 1;
      return header;
    }

    // drops what a failed attempt left above this pivot, and resumes after its canonical blocks
    private void prepareChain() {
      if (resume.indexedUpTo() > firstPivot.getNumber()) {
        blockchain.unsafeRemoveCanonicalIndexRange(firstPivot.getNumber(), resume.indexedUpTo());
      }
      importStart =
          Math.max(importStart, Math.min(resume.importFrom(), firstPivot.getNumber() + 1));
      final BlockHeader last =
          importStart - 1 == floor.getNumber()
              ? floor
              : blockchain
                  .getBlockHeader(importStart - 1)
                  .orElseThrow(() -> new IllegalStateException("lost an imported header"));
      blockchain.unsafeSetChainHead(
          last,
          blockchain
              .getTotalDifficultyByHash(last.getHash())
              .orElseThrow(() -> new IllegalStateException("lost a total difficulty")));
    }

    private void storeHeaders(final List<BlockHeader> headers) {
      blockchain.storeBlockHeaders(headers);
      indexedUpTo = Math.max(indexedUpTo, headers.getLast().getNumber());
    }

    // downloads the headers below a stored header, replacing any download in progress
    private void startBackfill(final BlockHeader top) {
      final HeaderBackfill previous = backfill;
      if (previous != null) {
        previous.stop();
      }
      final HeaderBackfill next =
          new HeaderBackfill(
              scheduler,
              protocolSchedule,
              blockchain,
              fetcher,
              headersToCheckpointOnly ? floor : blockchain.getGenesisBlockHeader(),
              top,
              config.headersPerRequest(),
              config.chainParallelism(),
              metricsSystem);
      backfill = next;
      next.run(executor)
          .whenComplete(
              (unused, error) -> {
                if (inactive() || next != backfill) {
                  return;
                }
                if (error == null) {
                  headersDone.complete(null);
                  return;
                }
                LOG.warn("Header download failed, resuming: {}", unwrap(error).getMessage());
                delay(5)
                    .thenRun(
                        () -> {
                          if (!inactive() && next == backfill) {
                            startBackfill(
                                blockchain
                                    .getBlockHeader(next.lowestStored())
                                    .orElseThrow(
                                        () ->
                                            new IllegalStateException(
                                                "lost the lowest stored header")));
                          }
                        });
              });
    }

    // bodies and receipts follow the stored headers, up to the pivot
    private CompletableFuture<Void> importLoop(final BlockImporter blocks) {
      if (inactive() || blocks != importer) {
        return abandoned();
      }
      final long next = blocks.nextBlock();
      if (finalTarget >= 0 && next > finalTarget) {
        return CompletableFuture.completedFuture(null);
      }
      final long target = finalTarget >= 0 ? finalTarget : stateSync.pivot().header().getNumber();
      if (backfill.lowestStored() > next || next > target) {
        return poll().thenCompose(unused -> importLoop(blocks));
      }
      return blocks
          .importUpTo(target, executor)
          .handle(
              (unused, error) -> {
                if (error != null && !inactive()) {
                  LOG.debug("Block import interrupted: {}", unwrap(error).getMessage());
                }
                return error == null;
              })
          // an interrupted import waits a little, so that a failing one does not spin
          .thenCompose(
              imported ->
                  imported ? importLoop(blocks) : poll().thenCompose(unused -> importLoop(blocks)));
    }

    private void maybeMove() {
      if (inactive() || finalizing || moving) {
        return;
      }
      final Optional<Hash> head = heads.headHash();
      if (head.isEmpty()) {
        return;
      }
      moving = true;
      fetcher
          .byHash(head.get(), Priority.PIVOT)
          .thenCompose(headHeader -> moveTowards(headHeader, false))
          .whenComplete(
              (unused, error) -> {
                // a restart is logged when it starts
                if (error != null && !inactive() && !(unwrap(error) instanceof RestartException)) {
                  LOG.warn("Pivot move failed, retrying: {}", unwrap(error).getMessage());
                }
                moving = false;
              });
    }

    // forced, it also checks the pivot is still canonical even if it does not need to move
    private CompletableFuture<Void> moveTowards(final BlockHeader head, final boolean force) {
      if (inactive()) {
        return abandoned();
      }
      final BlockHeader pivot = stateSync.pivot().header();
      final long distance = head.getNumber() - pivot.getNumber();
      final boolean due = distance >= config.pivotDistance() + config.pivotMoveThreshold();
      if (distance <= 0 || !(force || due || stateSync.isPivotStale())) {
        return CompletableFuture.completedFuture(null);
      }
      return fetcher
          .chainDown(head, pivot.getNumber() + 1, Priority.PIVOT)
          .thenCompose(
              chain -> {
                if (!chain.getFirst().getParentHash().equals(pivot.getHash())) {
                  return recoverFromReorg(head, chain);
                }
                final long target = head.getNumber() - config.pivotDistance();
                if (target <= pivot.getNumber()) {
                  return CompletableFuture.completedFuture(null);
                }
                return applyMoves(chain.subList(0, (int) (target - pivot.getNumber())));
              });
    }

    // applies the skipped blocks in bounded steps, so that the folded diff stays small
    private CompletableFuture<Void> applyMoves(final List<BlockHeader> headers) {
      final boolean steps = headers.size() > MAX_BLOCKS_PER_MOVE;
      if (steps) {
        stateSync.pause();
      }
      return applyStep(headers, 0)
          .whenComplete(
              (unused, error) -> {
                if (steps) {
                  stateSync.resume();
                }
              });
    }

    private CompletableFuture<Void> applyStep(final List<BlockHeader> headers, final int from) {
      if (from >= headers.size()) {
        return CompletableFuture.completedFuture(null);
      }
      if (inactive()) {
        return abandoned();
      }
      final List<BlockHeader> step =
          headers.subList(from, Math.min(headers.size(), from + MAX_BLOCKS_PER_MOVE));
      final BlockHeader target = step.getLast();
      return balFetcher
          .fetch(step)
          .thenCompose(
              lists -> {
                if (inactive()) {
                  return abandoned();
                }
                final StateDiff diff = fold(lists);
                storeHeaders(step);
                storeBlockAccessLists(step, lists);
                return stateSync.moveTo(target, diff);
              })
          .thenCompose(
              outcome -> {
                moves++;
                LOG.info(
                    "Moved the pivot to block {}: {} accounts updated, {} outside the downloaded ranges, {} removed, {} slots",
                    target.getNumber(),
                    outcome.applied(),
                    outcome.skipped(),
                    outcome.deleted(),
                    outcome.slots());
                return applyStep(headers, from + MAX_BLOCKS_PER_MOVE);
              });
    }

    private CompletableFuture<Void> recoverFromReorg(
        final BlockHeader head, final List<BlockHeader> newChain) {
      final BlockHeader pivot = stateSync.pivot().header();
      final long lowest = Math.max(floor.getNumber() + 1, pivot.getNumber() - MAX_REORG_DEPTH);
      // both branches down to the same height, the orphaned one walked from our pivot by hash
      final CompletableFuture<List<BlockHeader>> canonicalPart =
          fetcher.chainDown(newChain.getFirst(), lowest, Priority.PIVOT);
      final CompletableFuture<List<BlockHeader>> orphanedPart =
          fetcher.chainDown(pivot, lowest, Priority.PIVOT);
      return canonicalPart
          .thenCombine(orphanedPart, (lower, orphanedChain) -> List.of(lower, orphanedChain))
          .thenCompose(
              branches -> {
                final List<BlockHeader> canonical = new ArrayList<>(branches.get(0));
                canonical.addAll(newChain.subList(1, newChain.size()));
                final List<BlockHeader> orphanedChain = branches.get(1);
                final int ancestorIndex = commonAncestor(canonical, orphanedChain);
                if (ancestorIndex < 0) {
                  return CompletableFuture.failedFuture(
                      new RestartException(
                          "the chain reorganised deeper than " + MAX_REORG_DEPTH + " blocks", -1));
                }
                final BlockHeader ancestor = canonical.get(ancestorIndex);
                final long target =
                    Math.max(ancestor.getNumber() + 1, head.getNumber() - config.pivotDistance());
                final List<BlockHeader> newFork =
                    canonical.subList(
                        ancestorIndex + 1,
                        (int) (ancestorIndex + 1 + target - ancestor.getNumber()));
                final List<BlockHeader> orphaned =
                    orphanedChain.stream()
                        .filter(header -> header.getNumber() > ancestor.getNumber())
                        .toList();
                LOG.warn(
                    "Chain reorganised below the pivot {}: common ancestor {}, {} orphaned blocks, new pivot {}",
                    pivot.getNumber(),
                    ancestor.getNumber(),
                    orphaned.size(),
                    target);
                return balFetcher
                    .fetch(orphaned, ORPHANED_BAL_ROUNDS)
                    .exceptionallyCompose(
                        error ->
                            CompletableFuture.failedFuture(
                                new RestartException(
                                    "orphaned block access lists are unavailable",
                                    ancestor.getNumber())))
                    .thenCombine(balFetcher.fetch(newFork), (old, fresh) -> List.of(old, fresh))
                    .thenCompose(
                        lists -> {
                          if (inactive()) {
                            return abandoned();
                          }
                          if (backfill.lowestStored() > ancestor.getNumber() + 1) {
                            // the download in progress follows the orphaned branch
                            startBackfill(newFork.getFirst());
                          }
                          storeHeaders(newFork);
                          if (pivot.getNumber() > target) {
                            blockchain.unsafeRemoveCanonicalIndexRange(target, pivot.getNumber());
                          }
                          storeBlockAccessLists(newFork, lists.get(1));
                          // after the new headers, so that no import reads the orphaned ones
                          importer.rewindTo(ancestor.getNumber() + 1);
                          return new ReorgRecovery(stateSync, storage, scheduler)
                              .recover(newFork.getLast(), fold(lists.get(0)), fold(lists.get(1)));
                        })
                    .exceptionallyCompose(
                        error ->
                            unwrap(error) instanceof RestartException
                                    || unwrap(error) instanceof CancellationException
                                ? CompletableFuture.failedFuture(error)
                                : CompletableFuture.failedFuture(
                                    new RestartException(
                                        "reorg recovery failed: " + unwrap(error).getMessage(),
                                        ancestor.getNumber())));
              })
          .exceptionallyCompose(
              error -> {
                if (unwrap(error) instanceof RestartException restartError) {
                  // the state cannot be recovered: start over
                  restart.completeExceptionally(restartError);
                }
                return CompletableFuture.failedFuture(error);
              });
    }

    // stops moving the pivot and waits for a move in progress
    private CompletableFuture<Void> settleMoves() {
      final CompletableFuture<Void> settled = new CompletableFuture<>();
      control.execute(
          () -> {
            finalizing = true;
            waitIdle(settled);
          });
      return settled;
    }

    private void waitIdle(final CompletableFuture<Void> settled) {
      if (inactive()) {
        settled.completeExceptionally(new CancellationException("attempt abandoned"));
      } else if (moving) {
        control.schedule(() -> waitIdle(settled), 200, TimeUnit.MILLISECONDS);
      } else {
        settled.complete(null);
      }
    }

    // the last pivot must be canonical and recent: move it next to the head before building
    private CompletableFuture<Void> finalCatchUp() {
      if (inactive()) {
        return abandoned();
      }
      final Optional<Hash> head = heads.headHash();
      if (head.isEmpty()) {
        return CompletableFuture.completedFuture(null);
      }
      return fetcher
          .byHash(head.get(), Priority.PIVOT)
          .thenCompose(headHeader -> moveTowards(headHeader, true))
          .exceptionallyCompose(
              error -> {
                final Throwable cause = unwrap(error);
                if (inactive()
                    || cause instanceof RestartException
                    || cause instanceof CancellationException) {
                  return CompletableFuture.failedFuture(error);
                }
                LOG.info("Final pivot check failed, retrying: {}", cause.getMessage());
                return delay(1).thenCompose(unused -> finalCatchUp());
              });
    }

    private BlockHeader buildTries() {
      if (inactive()) {
        throw new CancellationException("attempt abandoned");
      }
      final BlockHeader pivot = stateSync.pivot().header();
      stateSync.stop();
      LOG.info("Building the state tries of block {}", pivot.getNumber());
      trieBuilder = new WorldStateTrieBuilder(storage, config.trieBuildThreads());
      final WorldStateTrieBuilder.Result built = trieBuilder.build();
      if (!built.root().equals(Bytes32.wrap(pivot.getStateRoot().getBytes()))) {
        throw new CompletionException(
            new RestartException(
                "built state root "
                    + built.root()
                    + " does not match block "
                    + pivot.getNumber()
                    + " root "
                    + pivot.getStateRoot(),
                pivot.getNumber()));
      }
      storage.saveWorldState(pivot, built.rootNode());
      storage.completeSync();
      LOG.info(
          "World state of block {} is complete: {} accounts, {} slots",
          pivot.getNumber(),
          built.accounts(),
          built.slots());
      trieBuilder = null;
      return pivot;
    }

    private String progress() {
      final WorldStateTrieBuilder builder = trieBuilder;
      final HeaderBackfill headers = backfill;
      final BlockImporter blocks = importer;
      final String phase =
          builder != null
              ? String.format(
                  "building tries: %d accounts, %d slots",
                  builder.accountsProcessed(), builder.slotsProcessed())
              : "state " + stateSync.progress();
      return String.format(
          "pivot %d, %s, headers stored down to %s, blocks imported up to %s",
          stateSync.pivot().header().getNumber(),
          phase,
          headers == null ? "-" : headers.lowestStored(),
          blocks == null ? "-" : blocks.nextBlock() - 1);
    }
  }

  // index of the highest header of the canonical branch that is also on the orphaned one
  private static int commonAncestor(
      final List<BlockHeader> canonical, final List<BlockHeader> orphaned) {
    final Map<Long, Hash> orphanedHashes = new HashMap<>();
    orphaned.forEach(header -> orphanedHashes.put(header.getNumber(), header.getHash()));
    for (int i = canonical.size() - 1; i >= 0; i--) {
      final BlockHeader header = canonical.get(i);
      if (header.getHash().equals(orphanedHashes.get(header.getNumber()))) {
        return i;
      }
    }
    return -1;
  }

  private StateDiff fold(final List<Bytes> lists) {
    final StateDiff diff = new StateDiff();
    lists.forEach(list -> diff.add(BlockAccessList.fromBytes(list)));
    return diff;
  }

  // keeps the lists of the blocks the node will hold, to serve them once synced
  private void storeBlockAccessLists(final List<BlockHeader> headers, final List<Bytes> lists) {
    if (blockchain instanceof DefaultBlockchain defaultBlockchain) {
      final var updater = defaultBlockchain.getBlockchainStorage().updater();
      for (int i = 0; i < headers.size(); i++) {
        updater.putSyncBlockAccessList(
            headers.get(i).getHash(), new SyncBlockAccessList(lists.get(i)));
      }
      updater.commit();
    }
  }

  private void logProgress() {
    try {
      final Attempt attempt = current;
      if (attempt == null) {
        return;
      }
      LOG.info(
          "SNAP2 sync progress: {}, {} pivot moves, {} snap/2 peers",
          attempt.progress(),
          moves,
          scheduler.peerCount(Protocol.SNAP));
    } catch (final RuntimeException e) {
      LOG.debug("Failed to log the SNAP2 progress", e);
    }
  }

  private void shutdown() {
    final Attempt attempt = current;
    if (attempt != null) {
      attempt.cancel();
    }
    if (balFetcher != null) {
      balFetcher.stop();
    }
    if (scheduler != null) {
      scheduler.stop();
    }
    control.shutdownNow();
    executor.shutdownNow();
  }

  private CompletableFuture<Void> delay(final long seconds) {
    return CompletableFuture.runAsync(
        () -> {}, CompletableFuture.delayedExecutor(seconds, TimeUnit.SECONDS, executor));
  }

  private CompletableFuture<Void> poll() {
    return CompletableFuture.runAsync(
        () -> {},
        CompletableFuture.delayedExecutor(
            config.pollInterval().toMillis(), TimeUnit.MILLISECONDS, executor));
  }

  private static Throwable unwrap(final Throwable error) {
    Throwable cause = error;
    while (cause instanceof CompletionException && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }

  /** The state cannot be recovered and the sync must start over. */
  static final class RestartException extends RuntimeException {
    private final long canonicalUpTo;

    /**
     * Creates the exception.
     *
     * @param canonicalUpTo the highest block known to be canonical, -1 if none
     */
    RestartException(final String message, final long canonicalUpTo) {
      super(message);
      this.canonicalUpTo = canonicalUpTo;
    }

    long canonicalUpTo() {
      return canonicalUpTo;
    }
  }
}
