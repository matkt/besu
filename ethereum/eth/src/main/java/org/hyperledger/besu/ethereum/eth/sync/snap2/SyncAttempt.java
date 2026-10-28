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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.block;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.compact;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.count;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.duration;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.eta;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.percent;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BalFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.ChainHeadSource;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.Pivot;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.WorldStateTrieBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One attempt of the SNAP2 sync from a first pivot: the headers down to the floor, the state at a
 * pivot that follows the chain head, the final build of the tries, and the chain head set at the
 * final pivot once the bodies of the window below it are stored. A cancelled attempt writes nothing
 * once {@link #awaitStop} returns.
 */
final class SyncAttempt {

  private static final Logger LOG = LoggerFactory.getLogger(SyncAttempt.class);
  // a cancelled build ends at its next check, after a few thousand slots at most
  private static final Duration BUILD_TIMEOUT = Duration.ofSeconds(30);

  /**
   * What the attempts of a sync share.
   *
   * @param stateSyncs creates the state download of an attempt
   * @param chains creates the chain of an attempt
   * @param trieBuilders creates the final build, given whether the download left the trie of a
   *     storage to it
   * @param executor runs the final build
   * @param retries runs a final catch-up of the pivot again, some time after it failed
   */
  record Context(
      MutableBlockchain blockchain,
      SyncStateStorage storage,
      ChainHeadSource heads,
      HeaderFetcher fetcher,
      BalFetcher balFetcher,
      PivotMoves.Settings moveSettings,
      Supplier<StateSync> stateSyncs,
      Supplier<SyncChain> chains,
      Function<Predicate<Bytes32>, WorldStateTrieBuilder> trieBuilders,
      Executor executor,
      Executor retries) {}

  private final Context context;
  private final BlockHeader firstPivot;
  private final StateSync stateSync;
  private final SyncChain chain;
  private final PivotMoves moves;
  // fails when the attempt must start over before its end
  private final CompletableFuture<BlockHeader> aborted = new CompletableFuture<>();
  private final CompletableFuture<Void> buildEnded = new CompletableFuture<>();
  private volatile boolean cancelled;
  private volatile boolean triesBuilt;
  private volatile boolean done;
  // guarded by this: the final build, once started
  private WorldStateTrieBuilder trieBuilder;

  SyncAttempt(final Context context, final BlockHeader firstPivot) {
    this.context = context;
    this.firstPivot = firstPivot;
    this.stateSync = context.stateSyncs().get();
    this.chain = context.chains().get();
    this.moves =
        new PivotMoves(
            stateSync,
            chain::storeHeaders,
            context.blockchain(),
            context.heads(),
            context.fetcher(),
            context.balFetcher(),
            context.moveSettings(),
            aborted::completeExceptionally,
            context.retries());
  }

  /**
   * Starts the attempt, on a thread that may wait for the checkpoint. It completes with the final
   * pivot, made the chain head, or fails: with a {@link RestartException} when the sync must start
   * over.
   */
  CompletableFuture<BlockHeader> start() {
    chain.start(firstPivot);
    final CompletableFuture<BlockHeader> synced =
        stateSync
            .start(firstPivot)
            .exceptionallyCompose(this::stateFailed)
            .thenCompose(unused -> moves.settle())
            .thenCompose(unused -> moves.catchUp())
            .thenComposeAsync(unused -> buildTriesWithWindow(), context.executor())
            .thenApply(
                pivot -> {
                  chain.setChainHead();
                  done = true;
                  return pivot;
                });
    return synced.applyToEither(aborted, Function.identity());
  }

  /** Moves the pivot when the head went far enough, or when peers stopped serving its state. */
  void maybeMove() {
    moves.maybeMove();
  }

  /** Stops every part of the attempt; see {@link #awaitStop}. */
  void cancel() {
    cancelled = true;
    moves.stop();
    stateSync.stop();
    chain.cancel();
    final WorldStateTrieBuilder builder;
    synchronized (this) {
      builder = trieBuilder;
    }
    if (builder == null) {
      // none starts once cancelled
      buildEnded.complete(null);
    } else {
      builder.cancel();
    }
  }

  /** Waits for the reads and writes of a cancelled attempt to end, before the storage is wiped. */
  void awaitStop() throws InterruptedException {
    chain.awaitStop();
    try {
      buildEnded.get(BUILD_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    } catch (final ExecutionException e) {
      // the build ended
    } catch (final TimeoutException e) {
      LOG.warn("SNAP2 trie build still running after {}", BUILD_TIMEOUT);
    }
  }

  /** Returns the number of pivot moves applied. */
  long pivotMoves() {
    return moves.moves();
  }

  /**
   * What the attempt did of each phase, from 0 to 1.
   *
   * @param state the share of the account key space downloaded
   * @param tries the share of the accounts the final build added
   * @param headers the share of the headers stored
   * @param bodies the share of the bodies of the window stored
   */
  record Progress(double state, double tries, double headers, double bodies) {}

  /** Returns what the attempt did of each phase. */
  Progress progress() {
    final Optional<WorldStateTrieBuilder> builder = trieBuilder();
    final double tries =
        triesBuilt
            ? 1
            : builder
                .map(
                    build ->
                        share(build.accountsProcessed(), stateSync.downloadedAccounts(), false))
                .orElse(0.0);
    final SyncChain.HeaderProgress headers = chain.headerProgress();
    return new Progress(
        builder.isPresent() ? 1 : stateSync.accountCoverage(),
        tries,
        share(headers.stored(), headers.stored() + headers.left(), chain.headersDone().isDone()),
        done ? 1 : chain.windowProgress());
  }

  /** Returns the line of a progress log: the share done of each phase. */
  String progressLine() {
    final Progress progress = progress();
    final SyncChain.HeaderProgress headers = chain.headerProgress();
    final StringBuilder line =
        new StringBuilder("SNAP2 sync | state ")
            .append(percent(progress.state()))
            .append(" | tries ")
            .append(percent(progress.tries()))
            .append(" | headers ")
            .append(percent(progress.headers()));
    eta(headers.stored(), headers.left(), headers.elapsed())
        .ifPresent(left -> line.append(", ETA ").append(left));
    return line.append(" | bodies ").append(percent(progress.bodies())).toString();
  }

  /** Returns the figures behind the progress, for debugging. */
  String details() {
    return "SNAP2 details | pivot "
        + block(pivotNumber())
        + " | accounts "
        + count(stateSync.downloadedAccounts())
        + " | slots "
        + compact(stateSync.downloadedSlots())
        + " | bytecodes "
        + compact(stateSync.downloadedCodes())
        + " | storages pending "
        + count(stateSync.pendingStorageCount())
        + " | storage tries "
        + compact(stateSync.storageTriesBuilt())
        + " built, "
        + compact(stateSync.storageTriesUpdated())
        + " updated, "
        + compact(stateSync.storageTriesLeft())
        + " left | peers left out "
        + stateSync.peersWithoutState();
  }

  /** Returns the phase the attempt is busy with: the state, the tries, the chain, or done. */
  String stage() {
    if (done) {
      return "done";
    }
    if (triesBuilt) {
      return "chain";
    }
    return trieBuilder().isPresent() ? "tries" : "state";
  }

  /** Returns the current pivot, 0 before the state download starts. */
  long pivotNumber() {
    final Pivot pivot = stateSync.pivot();
    return pivot == null ? 0 : pivot.header().getNumber();
  }

  /** Returns the state download, for its counters. */
  StateSync stateSync() {
    return stateSync;
  }

  /** Returns the final build, once started. */
  synchronized Optional<WorldStateTrieBuilder> trieBuilder() {
    return Optional.ofNullable(trieBuilder);
  }

  // complete once over, none when there is nothing to do
  private static double share(final long done, final long total, final boolean over) {
    if (total <= 0) {
      return over ? 1 : 0;
    }
    return Math.min(1, (double) done / total);
  }

  // a download that failed, with a diff partly applied for one, cannot go on from its state
  private CompletableFuture<Void> stateFailed(final Throwable error) {
    return cancelled
        ? CompletableFuture.failedFuture(error)
        : CompletableFuture.failedFuture(new RestartException("the state download failed", error));
  }

  // the bodies of the window below the final pivot download while the tries build
  private CompletableFuture<BlockHeader> buildTriesWithWindow() {
    final BlockHeader pivot = stateSync.pivot().header();
    final CompletableFuture<Void> window = chain.downloadWindow(pivot);
    buildTries(pivot);
    if (!chain.headersDone().isDone()) {
      LOG.info("SNAP2 tries built, waiting for the header download");
    }
    return window.thenApply(unused -> pivot);
  }

  private void buildTries(final BlockHeader pivot) {
    // the tries the last moves changed are kept by the build
    stateSync.finishStorageTries();
    stateSync.stop();
    final WorldStateTrieBuilder builder = newTrieBuilder();
    final Instant started = Instant.now();
    LOG.info(
        "SNAP2 trie build started at {} | accounts {} | storage tries built while downloading {}",
        block(pivot.getNumber()),
        compact(stateSync.downloadedAccounts()),
        compact(stateSync.storageTriesBuilt()));
    try {
      final WorldStateTrieBuilder.Result built = builder.build();
      if (!built.root().equals(pivot.getStateRoot().getBytes())) {
        throw new RestartException(
            "built state root "
                + built.root()
                + " does not match the root "
                + pivot.getStateRoot()
                + " of block "
                + pivot.getNumber());
      }
      context.storage().saveWorldState(pivot, built.rootNode());
      context.storage().completeSync();
      triesBuilt = true;
      LOG.info(
          "SNAP2 trie build complete in {} | root verified | accounts {} | storage tries kept {}"
              + " | slots read {}",
          duration(Duration.between(started, Instant.now())),
          compact(built.accounts()),
          compact(built.keptStorageTries()),
          compact(built.slots()));
    } finally {
      buildEnded.complete(null);
    }
  }

  // under the lock, so that a cancel either finds it or stops it from starting
  private synchronized WorldStateTrieBuilder newTrieBuilder() {
    if (cancelled) {
      throw new CancellationException("attempt abandoned");
    }
    trieBuilder = context.trieBuilders().apply(stateSync::storageTrieLeft);
    return trieBuilder;
  }
}
