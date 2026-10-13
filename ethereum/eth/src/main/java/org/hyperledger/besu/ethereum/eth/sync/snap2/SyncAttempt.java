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
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.SyncBlockAccessList;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BodyDownloader;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderBackfill;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.ReorgRecovery;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateDiff;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.WorldStateTrieBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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

  /** The blocks a reorg orphaned above the common ancestor, and the new ones up to the pivot. */
  private record Reorg(
      BlockHeader ancestor, List<BlockHeader> orphaned, List<BlockHeader> newFork) {}

  private final SyncContext context;
  private final BlockHeader firstPivot;
  private final Resume resume;
  private final StateSync stateSync;
  // fails when the attempt must end early, to start over or to stop the sync
  private final CompletableFuture<BlockHeader> aborted = new CompletableFuture<>();
  private final CompletableFuture<Void> headersDone = new CompletableFuture<>();
  private BlockHeader floor;
  // the total difficulty stored with every header above a proof-of-stake floor
  private Optional<Difficulty> headerTotalDifficulty = Optional.empty();
  private volatile long indexedUpTo = -1;
  private volatile HeaderBackfill backfill;
  private volatile BodyDownloader bodies;
  private volatile WorldStateTrieBuilder trieBuilder;
  private volatile boolean cancelled;
  private volatile boolean moving;
  private volatile boolean finalizing;
  private volatile long finalTarget = -1;

  SyncAttempt(final SyncContext context, final BlockHeader firstPivot, final Resume resume) {
    this.context = context;
    this.stateSync = new StateSync(context.storage(), context.scheduler(), context.config());
    this.firstPivot = firstPivot;
    this.resume = resume;
  }

  /** Starts the attempt; it completes with the final pivot, or fails with a restart or an error. */
  CompletableFuture<BlockHeader> start() {
    floor = chainFloor();
    prepareChain();
    storeHeaders(List.of(firstPivot));
    final CompletableFuture<Void> state = stateSync.start(firstPivot);
    startBackfill(firstPivot);
    bodies =
        new BodyDownloader(
            context.scheduler(),
            context.protocolSchedule(),
            context.blockchain(),
            floor.getNumber(),
            context.config().bodiesPerRequest(),
            context.config().maxBodyRanges(),
            context.indexTransactions(),
            context.executor(),
            context.writer());
    final CompletableFuture<Void> blocks = bodyLoop();

    final CompletableFuture<BlockHeader> main =
        state
            .thenCompose(unused -> settleMoves())
            .thenCompose(unused -> finalCatchUp())
            .thenApplyAsync(unused -> buildTries(), context.executor())
            .thenCompose(
                finalPivot -> {
                  finalTarget = finalPivot.getNumber();
                  context
                      .syncDurationMetrics()
                      .stopTimer(
                          SyncDurationMetrics.Labels.SNAP_INITIAL_WORLD_STATE_DOWNLOAD_DURATION);
                  LOG.info("Waiting for the chain download to reach block {}", finalTarget);
                  return CompletableFuture.allOf(headersDone, blocks)
                      .thenApply(unused -> finalPivot);
                })
            .thenApply(
                finalPivot -> {
                  context
                      .syncDurationMetrics()
                      .stopTimer(SyncDurationMetrics.Labels.CHAIN_DOWNLOAD_DURATION);
                  LOG.info("SNAP2 sync complete at block {}", finalPivot.getNumber());
                  return finalPivot;
                });
    // either the sync completes, or the attempt ends early
    final CompletableFuture<BlockHeader> outcome = new CompletableFuture<>();
    FutureUtils.propagateResult(main, outcome);
    aborted.whenComplete(
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
    return context.stopped().getAsBoolean() || cancelled;
  }

  Resume resume() {
    return new Resume(indexedUpTo);
  }

  private <T> CompletableFuture<T> abandoned() {
    return CompletableFuture.failedFuture(new CancellationException("attempt abandoned"));
  }

  // genesis, or the checkpoint stored with its total difficulty, below which no body is needed
  private BlockHeader chainFloor() {
    final MutableBlockchain blockchain = context.blockchain();
    final Optional<Checkpoint> checkpoint = context.syncState().getCheckpoint();
    if (checkpoint.isEmpty() || checkpoint.get().blockNumber() <= 0) {
      return blockchain.getGenesisBlockHeader();
    }
    final Hash checkpointHash = checkpoint.get().blockHash();
    final BlockHeader header =
        blockchain
            .getBlockHeader(checkpointHash)
            .orElseGet(() -> context.fetcher().byHash(checkpointHash, Priority.HEADERS).join());
    if (header.getNumber() != checkpoint.get().blockNumber()) {
      throw new IllegalStateException(
          "checkpoint " + checkpointHash + " is not at block " + checkpoint.get().blockNumber());
    }
    StorageRetry.run(
        () -> blockchain.unsafeStoreHeader(header, checkpoint.get().totalDifficulty()));
    return header;
  }

  // the chain head stays on the floor until every body is stored; headers a failed attempt
  // indexed above this pivot are dropped
  private void prepareChain() {
    final MutableBlockchain blockchain = context.blockchain();
    if (resume.indexedUpTo() > firstPivot.getNumber()) {
      StorageRetry.run(
          () ->
              blockchain.unsafeRemoveCanonicalIndexRange(
                  firstPivot.getNumber(), resume.indexedUpTo()));
    }
    final Difficulty floorDifficulty =
        blockchain
            .getTotalDifficultyByHash(floor.getHash())
            .orElseThrow(() -> new IllegalStateException("lost a total difficulty"));
    StorageRetry.run(() -> blockchain.unsafeSetChainHead(floor, floorDifficulty));
    if (floor.getDifficulty().isZero()) {
      headerTotalDifficulty = Optional.of(floorDifficulty);
    }
  }

  private void storeHeaders(final List<BlockHeader> headers) {
    writeHeaders(headers);
    indexedUpTo = Math.max(indexedUpTo, headers.getLast().getNumber());
  }

  // above a proof-of-stake floor, with their total difficulty, which the chain head then reuses
  private void writeHeaders(final List<BlockHeader> headers) {
    final MutableBlockchain blockchain = context.blockchain();
    headerTotalDifficulty.ifPresentOrElse(
        total -> blockchain.storeBlockHeaders(headers, total),
        () -> blockchain.storeBlockHeaders(headers));
  }

  // downloads the headers below a stored header, replacing any download in progress
  private void startBackfill(final BlockHeader top) {
    final HeaderBackfill previous = backfill;
    if (previous != null) {
      previous.stop();
    }
    final HeaderBackfill next =
        new HeaderBackfill(
            context.scheduler(),
            context.protocolSchedule(),
            this::writeHeaders,
            context.fetcher(),
            context.headersToCheckpointOnly()
                ? floor
                : context.blockchain().getGenesisBlockHeader(),
            top,
            context.config().headersPerRequest(),
            context.config().chainParallelism(),
            context.metricsSystem());
    backfill = next;
    next.run(context.executor())
        .whenComplete(
            (unused, error) -> {
              if (inactive() || next != backfill) {
                return;
              }
              if (error == null) {
                headersDone.complete(null);
              } else if (Futures.unwrap(error)
                  instanceof HeaderBackfill.WrongChainException wrong) {
                aborted.completeExceptionally(wrong);
              } else {
                LOG.warn(
                    "Header download failed, resuming: {}", Futures.unwrap(error).getMessage());
                resumeBackfill(next);
              }
            });
  }

  // starts again below the lowest header the failed download stored
  private void resumeBackfill(final HeaderBackfill failed) {
    delay(Duration.ofSeconds(5))
        .thenRun(
            () -> {
              if (!inactive() && failed == backfill) {
                startBackfill(
                    context
                        .blockchain()
                        .getBlockHeader(failed.lowestStored())
                        .orElseThrow(
                            () -> new IllegalStateException("lost the lowest stored header")));
              }
            })
        .exceptionally(
            error -> {
              headersDone.completeExceptionally(error);
              return null;
            });
  }

  // bodies and receipts of a window of stored headers below the pivot, then the chain head once
  // the final pivot is reached; the older blocks are back-filled once the node follows the head
  private CompletableFuture<Void> bodyLoop() {
    if (inactive()) {
      return abandoned();
    }
    final long target = finalTarget >= 0 ? finalTarget : pivotNumber();
    bodies.download(Math.max(lowestStoredHeader(), windowStart(target)), target);
    if (finalTarget >= 0 && bodies.stored(windowStart(finalTarget), finalTarget)) {
      return CompletableFuture.runAsync(() -> setChainHead(finalTarget), context.executor());
    }
    return poll().thenCompose(unused -> bodyLoop());
  }

  // the lowest block whose body and receipts are needed before the head is set at this pivot
  private long windowStart(final long pivot) {
    return Math.max(floor.getNumber() + 1, pivot - context.config().bodyWindow() + 1);
  }

  private void setChainHead(final long target) {
    final MutableBlockchain blockchain = context.blockchain();
    final BlockHeader head = header(target);
    final long from = bodies.storedFrom(target);
    LOG.info("Bodies and receipts of blocks {} to {} stored, setting the chain head", from, target);
    bodies.verifyChain(header(from - 1), head);
    // recorded before the head, so that a node restarted in between back-fills the rest
    StorageRetry.run(() -> blockchain.unsafeSetBodiesFrom(from));
    StorageRetry.run(() -> blockchain.unsafeSetSyncChainHead(floor, head));
  }

  private BlockHeader header(final long number) {
    return context
        .blockchain()
        .getBlockHeader(number)
        .orElseThrow(() -> new IllegalStateException("lost the header of block " + number));
  }

  /** Moves the pivot if the head went far enough or peers stopped serving it. */
  void maybeMove() {
    // nothing to move before the state download starts at the first pivot
    if (inactive() || finalizing || moving || stateSync.pivot() == null) {
      return;
    }
    final Optional<Hash> head = context.heads().headHash();
    if (head.isEmpty()) {
      return;
    }
    moving = true;
    context
        .fetcher()
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

  // forced, it also checks the pivot is still canonical when the head is above it
  private CompletableFuture<Void> moveTowards(final BlockHeader head, final boolean force) {
    if (inactive()) {
      return abandoned();
    }
    final BlockHeader pivot = stateSync.pivot().header();
    final long distance = head.getNumber() - pivot.getNumber();
    final boolean due =
        distance >= context.config().pivotDistance() + context.config().pivotMoveThreshold();
    if (distance <= 0 || !(force || due || stateSync.isPivotStale())) {
      return CompletableFuture.completedFuture(null);
    }
    return context
        .fetcher()
        .chainDown(head, pivot.getNumber() + 1, Priority.PIVOT)
        .thenCompose(
            chain -> {
              if (!chain.getFirst().getParentHash().equals(pivot.getHash())) {
                return recoverFromReorg(head, chain);
              }
              final long target = head.getNumber() - context.config().pivotDistance();
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
    return context
        .balFetcher()
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
              context.metrics().pivotMoved();
              LOG.info(
                  "Moved the pivot to block {}: {} accounts updated, {} outside the downloaded ranges, {} removed, {} slots, {} storages to refresh",
                  target.getNumber(),
                  outcome.applied(),
                  outcome.skipped(),
                  outcome.deleted(),
                  outcome.slots(),
                  outcome.staleStorage());
              return applyStep(headers, from + MAX_BLOCKS_PER_MOVE);
            });
  }

  private CompletableFuture<Void> recoverFromReorg(
      final BlockHeader head, final List<BlockHeader> newChain) {
    final BlockHeader pivot = stateSync.pivot().header();
    final long lowest = Math.max(floor.getNumber() + 1, pivot.getNumber() - MAX_REORG_DEPTH);
    // both branches down to the same height, the orphaned one walked from our pivot by hash
    final CompletableFuture<List<BlockHeader>> canonicalPart =
        context.fetcher().chainDown(newChain.getFirst(), lowest, Priority.PIVOT);
    final CompletableFuture<List<BlockHeader>> orphanedPart =
        context.fetcher().chainDown(pivot, lowest, Priority.PIVOT);
    return canonicalPart
        .thenCombine(
            orphanedPart,
            (lower, orphaned) -> {
              final List<BlockHeader> canonical = new ArrayList<>(lower);
              canonical.addAll(newChain.subList(1, newChain.size()));
              return reorg(head, canonical, orphaned);
            })
        .thenCompose(reorg -> recover(pivot, reorg))
        .exceptionallyCompose(
            error -> {
              if (Futures.unwrap(error) instanceof RestartException restartError) {
                // the state cannot be recovered: start over
                aborted.completeExceptionally(restartError);
              }
              return CompletableFuture.failedFuture(error);
            });
  }

  // splits both branches at their common ancestor, the new one up to the new pivot
  private Reorg reorg(
      final BlockHeader head, final List<BlockHeader> canonical, final List<BlockHeader> orphaned) {
    final int ancestorIndex = commonAncestor(canonical, orphaned);
    if (ancestorIndex < 0) {
      throw new RestartException(
          "the chain reorganised deeper than " + MAX_REORG_DEPTH + " blocks");
    }
    final BlockHeader ancestor = canonical.get(ancestorIndex);
    final long target =
        Math.max(ancestor.getNumber() + 1, head.getNumber() - context.config().pivotDistance());
    return new Reorg(
        ancestor,
        orphaned.stream().filter(header -> header.getNumber() > ancestor.getNumber()).toList(),
        canonical.subList(
            ancestorIndex + 1, (int) (ancestorIndex + 1 + target - ancestor.getNumber())));
  }

  // fetches the lists of both branches, then moves the chain and the state to the new one
  private CompletableFuture<Void> recover(final BlockHeader pivot, final Reorg reorg) {
    context.metrics().reorged();
    LOG.warn(
        "Chain reorganised below the pivot {}: common ancestor {}, {} orphaned blocks, new pivot {}",
        pivot.getNumber(),
        reorg.ancestor().getNumber(),
        reorg.orphaned().size(),
        reorg.newFork().getLast().getNumber());
    final CompletableFuture<List<Bytes>> orphanedLists =
        context
            .balFetcher()
            .fetch(reorg.orphaned(), ORPHANED_BAL_ROUNDS)
            .exceptionallyCompose(
                error ->
                    CompletableFuture.failedFuture(
                        new RestartException("orphaned block access lists are unavailable")));
    final CompletableFuture<List<Bytes>> newLists = context.balFetcher().fetch(reorg.newFork());
    return orphanedLists
        .thenCombine(newLists, (orphaned, fresh) -> switchBranch(pivot, reorg, orphaned, fresh))
        .thenCompose(Function.identity())
        .exceptionallyCompose(
            error -> {
              final Throwable cause = Futures.unwrap(error);
              if (cause instanceof RestartException || cause instanceof CancellationException) {
                return CompletableFuture.failedFuture(error);
              }
              return CompletableFuture.failedFuture(
                  new RestartException("reorg recovery failed: " + cause.getMessage()));
            });
  }

  private CompletableFuture<Void> switchBranch(
      final BlockHeader pivot,
      final Reorg reorg,
      final List<Bytes> orphanedLists,
      final List<Bytes> newLists) {
    if (inactive()) {
      return abandoned();
    }
    final long ancestor = reorg.ancestor().getNumber();
    final BlockHeader newPivot = reorg.newFork().getLast();
    if (backfill.lowestStored() > ancestor + 1) {
      // the download in progress follows the orphaned branch
      startBackfill(reorg.newFork().getFirst());
    }
    storeHeaders(reorg.newFork());
    if (pivot.getNumber() > newPivot.getNumber()) {
      StorageRetry.run(
          () ->
              context
                  .blockchain()
                  .unsafeRemoveCanonicalIndexRange(newPivot.getNumber(), pivot.getNumber()));
    }
    storeBlockAccessLists(reorg.newFork(), newLists);
    // after the new headers, so that no import reads the orphaned ones
    bodies.rewind(ancestor + 1);
    return new ReorgRecovery(stateSync, context.storage(), context.scheduler())
        .recover(newPivot, fold(orphanedLists), fold(newLists));
  }

  // stops moving the pivot and waits for a move in progress
  private CompletableFuture<Void> settleMoves() {
    final CompletableFuture<Void> settled = new CompletableFuture<>();
    context
        .control()
        .execute(
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
      context.control().schedule(() -> waitIdle(settled), 200, TimeUnit.MILLISECONDS);
    } else {
      settled.complete(null);
    }
  }

  // the last pivot must be canonical and recent: move it next to the head before building
  private CompletableFuture<Void> finalCatchUp() {
    if (inactive()) {
      return abandoned();
    }
    final Optional<Hash> head = context.heads().headHash();
    if (head.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return context
        .fetcher()
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
    trieBuilder = new WorldStateTrieBuilder(context.storage(), context.config().trieBuildThreads());
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
    context.storage().saveWorldState(pivot, built.rootNode());
    context.storage().completeSync();
    // the state skipped the write-ahead log
    context.storage().flush();
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
    final long pivot = pivotNumber();
    return floor == null ? 0 : Math.max(0, pivot - windowStart(pivot) + 1);
  }

  long trieProgress(final boolean accounts) {
    final WorldStateTrieBuilder builder = trieBuilder;
    if (builder == null) {
      return 0;
    }
    return accounts ? builder.accountsProcessed() : builder.slotsProcessed();
  }

  long trieKeptStorageTries() {
    final WorldStateTrieBuilder builder = trieBuilder;
    return builder == null ? 0 : builder.keptStorageTries();
  }

  String progress() {
    final WorldStateTrieBuilder builder = trieBuilder;
    final HeaderBackfill headers = backfill;
    final String phase =
        builder != null
            ? String.format(
                "building tries: %d accounts, %d slots, %d storage tries kept from the download",
                builder.accountsProcessed(), builder.slotsProcessed(), builder.keptStorageTries())
            : "state " + stateSync.progress();
    return String.format(
        "pivot %d, %s, headers stored down to %s, bodies and receipts of %d blocks stored, %d needed below the pivot",
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
    return Futures.delay(delay, context.executor());
  }

  private CompletableFuture<Void> poll() {
    return Futures.delay(context.config().pollInterval(), context.executor());
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
    final Map<Hash, SyncBlockAccessList> accessLists = new HashMap<>();
    for (int i = 0; i < headers.size(); i++) {
      accessLists.put(headers.get(i).getHash(), new SyncBlockAccessList(lists.get(i)));
    }
    context.blockchain().unsafeStoreSyncBlockAccessLists(accessLists);
  }
}
