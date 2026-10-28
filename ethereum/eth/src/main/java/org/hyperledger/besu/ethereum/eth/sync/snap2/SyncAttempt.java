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
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.duration;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BalFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.ChainHeadSource;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.WorldStateTrieBuilder;

import java.time.Duration;
import java.time.Instant;
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
