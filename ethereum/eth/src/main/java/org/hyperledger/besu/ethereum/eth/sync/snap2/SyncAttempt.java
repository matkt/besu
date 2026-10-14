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

import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.block;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.compact;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.count;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.duration;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.eta;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.percent;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.SyncBlockAccessList;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BodyBackfill;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderBackfill;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateDiff;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.WorldStateTrieBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.util.FutureUtils;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One run of the SNAP2 sync from a pivot, owning its download components: the state download, the
 * header back-fill, the pivot moves, the final trie build and the download of the bodies of a
 * window below the final pivot. An abandoned attempt stops all its components and never touches the
 * next one.
 */
final class SyncAttempt {

  private static final Logger LOG = LoggerFactory.getLogger(SyncAttempt.class);
  private static final int MAX_BLOCKS_PER_MOVE = 128;

  /** What a new attempt keeps from a failed one: the highest header it indexed. */
  record Resume(long indexedUpTo) {
    static final Resume NONE = new Resume(-1);
  }

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
  private volatile BodyBackfill window;
  private volatile long windowSize;
  private volatile WorldStateTrieBuilder trieBuilder;
  private volatile Instant trieStarted = Instant.now();
  private volatile boolean cancelled;
  private volatile boolean done;
  private volatile boolean moving;
  private volatile boolean finalizing;

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

    final CompletableFuture<BlockHeader> main =
        state
            .thenCompose(unused -> settleMoves())
            .thenCompose(unused -> finalCatchUp())
            .thenComposeAsync(unused -> fetchWindowAndBuildTries(), context.executor())
            .thenCompose(
                finalPivot -> {
                  context
                      .syncDurationMetrics()
                      .stopTimer(
                          SyncDurationMetrics.Labels.SNAP_INITIAL_WORLD_STATE_DOWNLOAD_DURATION);
                  if (!headersDone.isDone()) {
                    LOG.info(
                        "SNAP2 state and tries done, waiting for the headers down to {}",
                        block(floor.getNumber() + 1));
                  }
                  return headersDone.thenApply(unused -> finalPivot);
                })
            .thenApply(
                finalPivot -> {
                  setChainHead(finalPivot);
                  done = true;
                  context
                      .syncDurationMetrics()
                      .stopTimer(SyncDurationMetrics.Labels.CHAIN_DOWNLOAD_DURATION);
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
    final BodyBackfill bodies = window;
    if (bodies != null) {
      bodies.stop();
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
        blockchain.getBlockHeader(checkpointHash).orElseGet(() -> fetchCheckpoint(checkpointHash));
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
  // peers may not serve it yet: start over rather than fail the sync
  private BlockHeader fetchCheckpoint(final Hash hash) {
    try {
      return context.fetcher().byHash(hash, Priority.HEADERS).join();
    } catch (final CompletionException | CancellationException e) {
      throw new RestartException(
          "checkpoint " + hash + " unavailable: " + Futures.unwrap(e).getMessage());
    }
  }

  private void writeHeaders(final List<BlockHeader> headers) {
    // headers down to genesis pass the checkpoint, which must be on their chain
    for (final BlockHeader header : headers) {
      if (header.getNumber() == floor.getNumber() && !header.getHash().equals(floor.getHash())) {
        throw new WrongChainException(
            "checkpoint " + floor.getHash() + " is not on the chain of the pivot");
      }
    }
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
              } else if (Futures.unwrap(error) instanceof WrongChainException wrong) {
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

  // the bodies and receipts of the window below the final pivot download while the tries build;
  // the older blocks are back-filled once the node follows the head
  private CompletableFuture<BlockHeader> fetchWindowAndBuildTries() {
    if (inactive()) {
      return abandoned();
    }
    final BlockHeader pivot = stateSync.pivot().header();
    final long from = windowStart(pivot.getNumber());
    windowSize = pivot.getNumber() - from + 1;
    final BodyBackfill bodies =
        new BodyBackfill(
            context.blockchain(),
            context.protocolSchedule(),
            context.peers(),
            context.peerTaskExecutor(),
            () -> true,
            pivot.getNumber() + 1,
            head -> from,
            BodyBackfill.Settings.unthrottled(context.indexTransactions()),
            Clock.systemUTC(),
            context.metricsSystem());
    window = bodies;
    bodies.start();
    final BlockHeader built = buildTries();
    return bodies.done().thenApply(unused -> built);
  }

  // the lowest block whose body and receipts are needed before the head is set at this pivot
  private long windowStart(final long pivot) {
    return Math.max(floor.getNumber() + 1, pivot - context.config().bodyWindow() + 1);
  }

  // the window recorded the block the bodies are stored from before the head points above it
  private void setChainHead(final BlockHeader head) {
    StorageRetry.run(() -> context.blockchain().unsafeSetSyncChainHead(floor, head));
    LOG.info(
        "SNAP2 chain head set to {} | bodies and receipts from {}, the older blocks back-filled in the background",
        block(head.getNumber()),
        block(windowStart(head.getNumber())));
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
                // below the pivot, past finality: start over rather than unwind the state
                final RestartException restart =
                    new RestartException("the chain reorganised below the pivot");
                aborted.completeExceptionally(restart);
                return CompletableFuture.failedFuture(restart);
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
                  "SNAP2 pivot moved to {} | {} accounts updated, {} outside the downloaded ranges, {} removed, {} slots, {} storages to refresh",
                  block(target.getNumber()),
                  count(outcome.applied()),
                  count(outcome.skipped()),
                  count(outcome.deleted()),
                  count(outcome.slots()),
                  count(outcome.staleStorage()));
              return applyStep(headers, from + MAX_BLOCKS_PER_MOVE);
            });
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
              return delay(Duration.ofSeconds(5)).thenCompose(unused -> finalCatchUp());
            });
  }

  private BlockHeader buildTries() {
    if (inactive()) {
      throw new CancellationException("attempt abandoned");
    }
    final BlockHeader pivot = stateSync.pivot().header();
    stateSync.stop();
    trieStarted = Instant.now();
    LOG.info(
        "SNAP2 trie build started at {} | accounts {} | storage tries built while downloading {}",
        block(pivot.getNumber()),
        compact(stateSync.downloadedAccounts()),
        compact(stateSync.storageTriesBuilt()));
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
        "SNAP2 trie build complete in {} | root verified | accounts {} | storage tries kept {} | slots read {}",
        duration(Duration.between(trieStarted, Instant.now())),
        compact(built.accounts()),
        compact(built.keptStorageTries()),
        compact(built.slots()));
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

  /** Returns the line of a tick: the phase, its share done and the figures that matter in it. */
  String progress() {
    final WorldStateTrieBuilder builder = trieBuilder;
    final BodyBackfill bodies = window;
    final StringBuilder line = new StringBuilder();
    if (builder != null) {
      // the share of the downloaded accounts added to the tries
      final long processed = builder.accountsProcessed();
      final long total = Math.max(processed, stateSync.downloadedAccounts());
      line.append("SNAP2 tries ")
          .append(percent(processed, total))
          .append(" | accounts ")
          .append(compact(processed))
          .append(" of ")
          .append(compact(total))
          .append(" | slots ")
          .append(compact(builder.slotsProcessed()))
          .append(" | storage tries kept ")
          .append(compact(builder.keptStorageTries()))
          .append(" | elapsed ")
          .append(duration(Duration.between(trieStarted, Instant.now())));
      eta(
              (double) processed,
              (double) (total - processed),
              Duration.between(trieStarted, Instant.now()))
          .ifPresent(left -> line.append(" | ETA ").append(left));
    } else if (bodies != null) {
      line.append("SNAP2 chain | bodies ")
          .append(percent(bodies.blocksStored(), windowSize))
          .append(" (")
          .append(count(bodies.blocksStored()))
          .append(" of ")
          .append(count(windowSize))
          .append(")");
    } else {
      // the share of the account key space covered
      final double coverage = stateSync.accountCoverage();
      line.append("SNAP2 state ")
          .append(percent(coverage))
          .append(" | accounts ")
          .append(count(stateSync.downloadedAccounts()))
          .append(" | slots ")
          .append(compact(stateSync.downloadedSlots()))
          .append(" | bytecodes ")
          .append(compact(stateSync.downloadedCodes()))
          .append(" | storages pending ")
          .append(count(stateSync.pendingStorages()));
      line.append(" | storage tries ")
          .append(compact(stateSync.storageTriesBuilt()))
          .append(" built (")
          .append(compact(stateSync.storageTrieSlots()))
          .append(" slots)");
      if (stateSync.storageTriesLeft() > 0) {
        line.append(", ")
            .append(compact(stateSync.storageTriesLeft()))
            .append(" left to the final build");
      }
      line.append(" | pivot ").append(block(pivotNumber()));
      headNumber()
          .ifPresent(
              head -> line.append(", ").append(count(head - pivotNumber())).append(" behind head"));
      if (stateSync.peersWithoutState() > 0) {
        line.append(" | ").append(stateSync.peersWithoutState()).append(" peers left out");
      }
      eta(coverage, 1 - coverage, stateSync.elapsed())
          .ifPresent(left -> line.append(" | ETA ").append(left));
    }
    // the headers, until they reach the floor
    final HeaderBackfill headers = backfill;
    if (headers != null && !headersDone.isDone()) {
      line.append(" | headers ").append(percent(headers.done(), headers.total()));
      eta((double) headers.done(), (double) (headers.total() - headers.done()), headers.elapsed())
          .ifPresent(left -> line.append(" (ETA ").append(left).append(")"));
    }
    return line.toString();
  }

  private Optional<Long> headNumber() {
    return context
        .heads()
        .headHash()
        .flatMap(context.heads()::knownHeader)
        .map(BlockHeader::getNumber);
  }

  StateSync stateSync() {
    return stateSync;
  }

  /** Returns what the attempt is busy with: the state, the tries or the chain, or done. */
  String stage() {
    if (done) {
      return "done";
    }
    if (trieBuilder != null) {
      return "tries";
    }
    return window != null ? "chain" : "state";
  }

  private CompletableFuture<Void> delay(final Duration delay) {
    return Futures.delay(delay, context.executor());
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
