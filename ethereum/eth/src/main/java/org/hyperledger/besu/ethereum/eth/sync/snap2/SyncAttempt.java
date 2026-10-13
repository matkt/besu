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
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BalFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BodyDownloader;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.ChainHeadSource;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderBackfill;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.ReorgRecovery;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateDiff;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.StorageRetry;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.WorldStateTrieBuilder;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.util.FutureUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One run of the SNAP2 sync from a pivot, owning its download components: the state download, the
 * header back-fill and the body download, the pivot moves and reorg recoveries, and the final trie
 * build. An abandoned attempt stops all its components and never touches the next one.
 */
final class SyncAttempt {

  private static final Logger LOG = LoggerFactory.getLogger(SyncAttempt.class);
  private static final int MAX_BLOCKS_PER_MOVE = 128;
  private static final int MAX_REORG_DEPTH = 1024;
  // orphaned lists are optional for peers: when none has them, start over instead of waiting
  private static final int ORPHANED_BAL_ROUNDS = 4;

  /** What a new attempt keeps from a failed one: the highest header it indexed. */
  record Resume(long indexedUpTo) {
    static final Resume NONE = new Resume(-1);
  }

  private final Snap2Configuration config;
  private final ProtocolSchedule protocolSchedule;
  private final MutableBlockchain blockchain;
  private final SyncState syncState;
  private final SyncStateStorage storage;
  private final ChainHeadSource heads;
  private final MetricsSystem metricsSystem;
  private final SyncDurationMetrics syncDurationMetrics;
  private final boolean headersToCheckpointOnly;
  private final boolean indexTransactions;
  private final RequestSender scheduler;
  private final HeaderFetcher fetcher;
  private final BalFetcher balFetcher;
  private final ExecutorService executor;
  private final ScheduledExecutorService control;
  private final BooleanSupplier stopped;
  private final Snap2Metrics metrics;

  private final BlockHeader firstPivot;
  private final Resume resume;
  private final StateSync stateSync;
  private final CompletableFuture<BlockHeader> restart = new CompletableFuture<>();
  private final CompletableFuture<Void> headersDone = new CompletableFuture<>();
  private BlockHeader floor;
  private volatile long indexedUpTo = -1;
  private volatile HeaderBackfill backfill;
  private volatile BodyDownloader bodies;
  private volatile WorldStateTrieBuilder trieBuilder;
  private volatile boolean cancelled;
  private volatile boolean moving;
  private volatile boolean finalizing;
  private volatile long finalTarget = -1;

  SyncAttempt(final SyncContext context, final BlockHeader firstPivot, final Resume resume) {
    this.config = context.config();
    this.protocolSchedule = context.protocolSchedule();
    this.blockchain = context.blockchain();
    this.syncState = context.syncState();
    this.storage = context.storage();
    this.heads = context.heads();
    this.metricsSystem = context.metricsSystem();
    this.syncDurationMetrics = context.syncDurationMetrics();
    this.headersToCheckpointOnly = context.headersToCheckpointOnly();
    this.indexTransactions = context.indexTransactions();
    this.scheduler = context.scheduler();
    this.fetcher = context.fetcher();
    this.balFetcher = context.balFetcher();
    this.executor = context.executor();
    this.control = context.control();
    this.stopped = context.stopped();
    this.metrics = context.metrics();
    this.stateSync = new StateSync(storage, scheduler, config);
    this.firstPivot = firstPivot;
    this.resume = resume;
  }

  /** Starts the attempt; it completes with the final pivot, or fails to ask for a restart. */
  CompletableFuture<BlockHeader> start() {
    floor = chainFloor();
    prepareChain();
    storeHeaders(List.of(firstPivot));
    final CompletableFuture<Void> state = stateSync.start(firstPivot);
    startBackfill(firstPivot);
    bodies =
        new BodyDownloader(
            scheduler,
            protocolSchedule,
            blockchain,
            floor.getNumber(),
            config.bodiesPerRequest(),
            config.maxBodyRanges(),
            indexTransactions,
            executor);
    final CompletableFuture<Void> blocks = bodyLoop();

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
                  syncDurationMetrics.stopTimer(SyncDurationMetrics.Labels.CHAIN_DOWNLOAD_DURATION);
                  LOG.info("SNAP2 sync complete at block {}", finalPivot.getNumber());
                  return finalPivot;
                });
    // either the sync completes, or an unrecoverable reorg asks for a restart
    final CompletableFuture<BlockHeader> outcome = new CompletableFuture<>();
    FutureUtils.propagateResult(main, outcome);
    restart.whenComplete(
        (unused, error) -> {
          if (error != null) {
            outcome.completeExceptionally(error);
          }
        });
    return outcome;
  }

  /** Stops every component of the attempt, which never writes anything afterwards. */
  void cancel() {
    cancelled = true;
    stateSync.stop();
    final HeaderBackfill headers = backfill;
    if (headers != null) {
      headers.stop();
    }
    final BodyDownloader downloader = bodies;
    if (downloader != null) {
      downloader.close();
    }
    final WorldStateTrieBuilder builder = trieBuilder;
    if (builder != null) {
      builder.cancel();
    }
  }

  private boolean inactive() {
    return stopped.getAsBoolean() || cancelled;
  }

  Resume resume() {
    return new Resume(indexedUpTo);
  }

  private <T> CompletableFuture<T> abandoned() {
    return CompletableFuture.failedFuture(new CancellationException("attempt abandoned"));
  }

  // genesis, or the checkpoint stored with its total difficulty, below which no body is needed
  private BlockHeader chainFloor() {
    final Optional<Checkpoint> checkpoint = syncState.getCheckpoint();
    if (checkpoint.isEmpty() || checkpoint.get().blockNumber() <= 0) {
      return blockchain.getGenesisBlockHeader();
    }
    final BlockHeader header =
        blockchain
            .getBlockHeader(checkpoint.get().blockHash())
            .orElseGet(() -> fetcher.byHash(checkpoint.get().blockHash(), Priority.HEADERS).join());
    if (header.getNumber() != checkpoint.get().blockNumber()) {
      throw new IllegalStateException(
          "checkpoint "
              + checkpoint.get().blockHash()
              + " is not at block "
              + checkpoint.get().blockNumber());
    }
    StorageRetry.run(
        () -> blockchain.unsafeStoreHeader(header, checkpoint.get().totalDifficulty()));
    return header;
  }

  // the chain head stays on the floor until every body is stored; headers a failed attempt
  // indexed above this pivot are dropped
  private void prepareChain() {
    if (resume.indexedUpTo() > firstPivot.getNumber()) {
      StorageRetry.run(
          () ->
              blockchain.unsafeRemoveCanonicalIndexRange(
                  firstPivot.getNumber(), resume.indexedUpTo()));
    }
    final var floorDifficulty =
        blockchain
            .getTotalDifficultyByHash(floor.getHash())
            .orElseThrow(() -> new IllegalStateException("lost a total difficulty"));
    StorageRetry.run(() -> blockchain.unsafeSetChainHead(floor, floorDifficulty));
  }

  private void storeHeaders(final List<BlockHeader> headers) {
    StorageRetry.run(() -> blockchain.storeBlockHeaders(headers));
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
              LOG.warn("Header download failed, resuming: {}", Futures.unwrap(error).getMessage());
              delay(Duration.ofSeconds(5))
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
                      })
                  .exceptionally(
                      resumeError -> {
                        headersDone.completeExceptionally(resumeError);
                        return null;
                      });
            });
  }

  // bodies and receipts of every stored header up to the pivot, then the chain head once the
  // final pivot is reached
  private CompletableFuture<Void> bodyLoop() {
    if (inactive()) {
      return abandoned();
    }
    final long target = finalTarget >= 0 ? finalTarget : pivotNumber();
    bodies.download(lowestStoredHeader(), target);
    if (finalTarget >= 0 && bodies.storedUpTo(finalTarget)) {
      return CompletableFuture.runAsync(() -> setChainHead(finalTarget), executor);
    }
    return poll().thenCompose(unused -> bodyLoop());
  }

  private void setChainHead(final long target) {
    final BlockHeader head =
        blockchain
            .getBlockHeader(target)
            .orElseThrow(() -> new IllegalStateException("lost the header of block " + target));
    LOG.info(
        "Bodies and receipts of blocks {} to {} stored, writing their total difficulties",
        floor.getNumber() + 1,
        target);
    StorageRetry.run(() -> blockchain.unsafeSetSyncChainHead(floor, head));
  }

  /** Moves the pivot if the head went far enough or peers stopped serving it. */
  void maybeMove() {
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
              if (error != null
                  && !inactive()
                  && !(Futures.unwrap(error) instanceof RestartException)) {
                LOG.warn("Pivot move failed, retrying: {}", Futures.unwrap(error).getMessage());
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
              metrics.pivotMoved();
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
                        "the chain reorganised deeper than " + MAX_REORG_DEPTH + " blocks"));
              }
              final BlockHeader ancestor = canonical.get(ancestorIndex);
              final long target =
                  Math.max(ancestor.getNumber() + 1, head.getNumber() - config.pivotDistance());
              final List<BlockHeader> newFork =
                  canonical.subList(
                      ancestorIndex + 1, (int) (ancestorIndex + 1 + target - ancestor.getNumber()));
              final List<BlockHeader> orphaned =
                  orphanedChain.stream()
                      .filter(header -> header.getNumber() > ancestor.getNumber())
                      .toList();
              metrics.reorged();
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
                              new RestartException("orphaned block access lists are unavailable")))
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
                          StorageRetry.run(
                              () ->
                                  blockchain.unsafeRemoveCanonicalIndexRange(
                                      target, pivot.getNumber()));
                        }
                        storeBlockAccessLists(newFork, lists.get(1));
                        // after the new headers, so that no import reads the orphaned ones
                        bodies.rewind(ancestor.getNumber() + 1);
                        return new ReorgRecovery(stateSync, storage, scheduler)
                            .recover(newFork.getLast(), fold(lists.get(0)), fold(lists.get(1)));
                      })
                  .exceptionallyCompose(
                      error ->
                          Futures.unwrap(error) instanceof RestartException
                                  || Futures.unwrap(error) instanceof CancellationException
                              ? CompletableFuture.failedFuture(error)
                              : CompletableFuture.failedFuture(
                                  new RestartException(
                                      "reorg recovery failed: "
                                          + Futures.unwrap(error).getMessage())));
            })
        .exceptionallyCompose(
            error -> {
              if (Futures.unwrap(error) instanceof RestartException restartError) {
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
              final Throwable cause = Futures.unwrap(error);
              if (inactive()
                  || cause instanceof RestartException
                  || cause instanceof CancellationException) {
                return CompletableFuture.failedFuture(error);
              }
              LOG.info("Final pivot check failed, retrying: {}", cause.getMessage());
              return delay(Duration.ofSeconds(1)).thenCompose(unused -> finalCatchUp());
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
                  + pivot.getStateRoot()));
    }
    storage.saveWorldState(pivot, built.rootNode());
    storage.completeSync();
    // the state skipped the write-ahead log
    storage.flush();
    LOG.info(
        "World state of block {} is complete: {} accounts, {} storage tries kept from the download, {} slots read for the others",
        pivot.getNumber(),
        built.accounts(),
        built.keptStorageTries(),
        built.slots());
    trieBuilder = null;
    return pivot;
  }

  long pivotNumber() {
    final StateSync.Pivot pivot = stateSync.pivot();
    return pivot == null ? 0 : pivot.header().getNumber();
  }

  long lowestStoredHeader() {
    final HeaderBackfill headers = backfill;
    return headers == null ? 0 : headers.lowestStored();
  }

  long bodiesStored() {
    final BodyDownloader downloader = bodies;
    return downloader == null ? 0 : downloader.storedBlocks();
  }

  long bodiesNeeded() {
    final BlockHeader base = floor;
    return base == null ? 0 : Math.max(0, pivotNumber() - base.getNumber());
  }

  long trieProgress(final boolean accounts) {
    final WorldStateTrieBuilder builder = trieBuilder;
    if (builder == null) {
      return 0;
    }
    return accounts ? builder.accountsProcessed() : builder.slotsProcessed();
  }

  String progress() {
    final WorldStateTrieBuilder builder = trieBuilder;
    final HeaderBackfill headers = backfill;
    final String phase =
        builder != null
            ? String.format(
                "building tries: %d accounts, %d slots",
                builder.accountsProcessed(), builder.slotsProcessed())
            : "state " + stateSync.progress();
    return String.format(
        "pivot %d, %s, headers stored down to %s, bodies and receipts of %d of %d blocks",
        pivotNumber(),
        phase,
        headers == null ? "-" : headers.lowestStored(),
        bodiesStored(),
        bodiesNeeded());
  }

  StateSync stateSync() {
    return stateSync;
  }

  /** Returns what the attempt is busy with: the state, the tries or the chain. */
  String stage() {
    if (finalTarget >= 0) {
      return "chain";
    }
    return trieBuilder != null ? "tries" : "state";
  }

  private CompletableFuture<Void> delay(final Duration delay) {
    return Futures.delay(delay, executor);
  }

  private CompletableFuture<Void> poll() {
    return Futures.delay(config.pollInterval(), executor);
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

  private static StateDiff fold(final List<Bytes> lists) {
    final StateDiff diff = new StateDiff();
    lists.forEach(list -> diff.add(BlockAccessList.fromBytes(list)));
    return diff;
  }

  // keeps the lists of the blocks the node will hold, to serve them once synced
  private void storeBlockAccessLists(final List<BlockHeader> headers, final List<Bytes> lists) {
    if (blockchain instanceof DefaultBlockchain defaultBlockchain) {
      StorageRetry.run(
          () -> {
            final var updater = defaultBlockchain.getBlockchainStorage().unloggedUpdater();
            for (int i = 0; i < headers.size(); i++) {
              updater.putSyncBlockAccessList(
                  headers.get(i).getHash(), new SyncBlockAccessList(lists.get(i)));
            }
            updater.commit();
          });
    }
  }
}
