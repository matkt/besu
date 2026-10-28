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

import static com.google.common.base.Preconditions.checkState;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.block;
import static org.hyperledger.besu.util.ExceptionUtils.rootCause;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.common.checkpoint.Checkpoint;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BodyBackfill;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderBackfill;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.WriteRetry;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The chain of an attempt of the SNAP2 sync: the floor below which no body is needed, the headers
 * down to it, the bodies and receipts of a window below the last pivot, and the chain head, set
 * once they are stored.
 */
final class SyncChain {

  private static final Logger LOG = LoggerFactory.getLogger(SyncChain.class);
  private static final Duration RESUME_DELAY = Duration.ofSeconds(5);
  // a stopped window writes nothing, its workers only end the reads they started
  private static final Duration WORKERS_TIMEOUT = Duration.ofSeconds(30);
  private static final int TOTAL_DIFFICULTIES_PER_COMMIT = 10_000;

  /**
   * Sizes of the downloads.
   *
   * @param headersPerRequest block headers per request
   * @param headerParallelism header requests at once
   * @param bodyWindow blocks up to the final pivot whose bodies and receipts are stored before the
   *     chain head is set
   * @param headersToCheckpointOnly whether the headers stop at the checkpoint rather than genesis
   * @param indexTransactions whether the transactions of the window are indexed
   * @param terminalTotalDifficulty every block above one whose total difficulty reaches it is proof
   *     of stake, none on a network that does not merge
   */
  record Settings(
      int headersPerRequest,
      int headerParallelism,
      int bodyWindow,
      boolean headersToCheckpointOnly,
      boolean indexTransactions,
      Optional<Difficulty> terminalTotalDifficulty) {}

  private final MutableBlockchain blockchain;
  private final ProtocolSchedule protocolSchedule;
  private final RequestSender sender;
  private final HeaderFetcher fetcher;
  private final Optional<Checkpoint> checkpoint;
  private final Settings settings;
  private final MetricsSystem metricsSystem;
  private final ExecutorService executor;
  private final ExecutorService windowWorkers;
  private final Function<Duration, Executor> delays;
  private final WriteRetry.Pause pause;
  private final int totalDifficultiesPerCommit;
  private final CompletableFuture<Void> headersDone = new CompletableFuture<>();

  private volatile BlockHeader floor;
  private volatile Difficulty floorTotal;
  // when every block above the floor is proof of stake, the total difficulty of every header
  private volatile Optional<Difficulty> headerTotalDifficulty = Optional.empty();
  private volatile boolean cancelled;
  // guarded by this
  private CompletableFuture<BlockHeader> checkpointFetch;
  private HeaderBackfill headers;
  private BodyBackfill window;
  private BlockHeader windowPivot;

  /**
   * Creates the chain of an attempt.
   *
   * @param checkpoint the trusted block below which no body is needed, genesis without one
   * @param executor runs the header download
   */
  SyncChain(
      final MutableBlockchain blockchain,
      final ProtocolSchedule protocolSchedule,
      final RequestSender sender,
      final HeaderFetcher fetcher,
      final Optional<Checkpoint> checkpoint,
      final Settings settings,
      final MetricsSystem metricsSystem,
      final ExecutorService executor) {
    this(
        blockchain,
        protocolSchedule,
        sender,
        fetcher,
        checkpoint,
        settings,
        metricsSystem,
        executor,
        Executors.newFixedThreadPool(
            BodyBackfill.Settings.forWindow(settings.indexTransactions()).parallelism(),
            Thread.ofPlatform().name("snap2-window-", 0).daemon().factory()),
        delay ->
            CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS, executor),
        Thread::sleep,
        TOTAL_DIFFICULTIES_PER_COMMIT);
  }

  /**
   * Creates the chain of an attempt whose window downloads on {@code windowWorkers}, whose failed
   * header downloads wait on the executors {@code delays} returns, whose writes wait {@code pause}
   * between two tries, and whose total difficulties are committed {@code
   * totalDifficultiesPerCommit} at a time.
   */
  @VisibleForTesting
  SyncChain(
      final MutableBlockchain blockchain,
      final ProtocolSchedule protocolSchedule,
      final RequestSender sender,
      final HeaderFetcher fetcher,
      final Optional<Checkpoint> checkpoint,
      final Settings settings,
      final MetricsSystem metricsSystem,
      final ExecutorService executor,
      final ExecutorService windowWorkers,
      final Function<Duration, Executor> delays,
      final WriteRetry.Pause pause,
      final int totalDifficultiesPerCommit) {
    this.blockchain = blockchain;
    this.protocolSchedule = protocolSchedule;
    this.sender = sender;
    this.fetcher = fetcher;
    this.checkpoint = checkpoint;
    this.settings = settings;
    this.metricsSystem = metricsSystem;
    this.executor = executor;
    this.windowWorkers = windowWorkers;
    this.delays = delays;
    this.pause = pause;
    this.totalDifficultiesPerCommit = totalDifficultiesPerCommit;
  }

  /**
   * Sets the chain head on the floor, removes the canonical entries above the first pivot, stores
   * it, and starts downloading the headers below it; an attempt starts once.
   *
   * @throws RestartException when peers do not serve the checkpoint
   * @throws IllegalStateException when the checkpoint is at another height, or above the pivot
   * @throws WrongChainException when the pivot is at the height of the checkpoint, with another
   *     hash, once the head is on the floor
   * @throws CancellationException once cancelled
   */
  void start(final BlockHeader firstPivot) {
    final BlockHeader floorHeader = floorHeader();
    if (firstPivot.getNumber() < floorHeader.getNumber()) {
      throw new IllegalStateException(
          "pivot " + firstPivot.getNumber() + " is below the floor " + floorHeader.getNumber());
    }
    synchronized (this) {
      throwIfCancelled();
      checkState(floor == null, "an attempt starts once");
      floor = floorHeader;
      // the chain head stays on the floor until every body above it is stored
      final Optional<Checkpoint> trusted = trustedCheckpoint();
      if (trusted.isPresent()) {
        // stored as the chain head
        floorTotal = trusted.get().totalDifficulty();
        retryWrite(() -> blockchain.unsafeStoreHeader(floorHeader, floorTotal));
      } else {
        floorTotal =
            blockchain
                .getTotalDifficultyByHash(floorHeader.getHash())
                .orElseThrow(() -> new IllegalStateException("no total difficulty for genesis"));
        retryWrite(() -> blockchain.unsafeSetChainHead(floorHeader, floorTotal));
      }
      removeIndexAbove(firstPivot.getNumber());
      // only proof of stake follows the terminal total difficulty, which Hoodi's genesis reaches
      if (settings.terminalTotalDifficulty().map(floorTotal::greaterOrEqualThan).orElse(false)) {
        headerTotalDifficulty = Optional.of(floorTotal);
      }
      writeHeaders(List.of(firstPivot));
      downloadHeaders(firstPivot);
    }
  }

  /**
   * Completes once the headers are stored down to the checkpoint, or down to genesis unless they
   * stop at the checkpoint; fails when they lead to another chain or cannot be stored.
   */
  CompletableFuture<Void> headersDone() {
    return headersDone;
  }

  /** Stores headers of the chain of the pivot, which must lead to the floor. */
  synchronized void storeHeaders(final List<BlockHeader> headers) {
    throwIfCancelled();
    writeHeaders(headers);
  }

  /**
   * Downloads the bodies and receipts of the window up to the final pivot once every header is
   * stored; completes once they are stored and recorded.
   */
  CompletableFuture<Void> downloadWindow(final BlockHeader pivot) {
    return headersDone.thenComposeAsync(unused -> startWindow(pivot), executor);
  }

  /**
   * Makes the final pivot the chain head, once the window up to it is stored, and removes the
   * canonical entries above it.
   */
  synchronized void setChainHead(final BlockHeader head) {
    throwIfCancelled();
    checkState(windowStored(head), "the window up to block %s is not stored", head.getNumber());
    checkState(
        blockchain.blockIsOnCanonicalChain(head.getHash()),
        "block %s is not canonical",
        head.getNumber());
    final Difficulty total = headerTotalDifficulty.orElseGet(() -> storeTotalDifficulties(head));
    retryWrite(() -> blockchain.unsafeSetChainHead(head, total));
    removeIndexAbove(head.getNumber());
    LOG.info(
        "SNAP2 chain head set to {} | bodies and receipts from {}",
        block(head.getNumber()),
        block(windowStart(head.getNumber())));
  }

  /** Stops the downloads and fails {@link #headersDone}; {@link #awaitStop} awaits the writes. */
  void cancel() {
    // before the lock, which a long call holds until its next check
    cancelled = true;
    final CompletableFuture<BlockHeader> fetch;
    final HeaderBackfill runningHeaders;
    final BodyBackfill runningWindow;
    synchronized (this) {
      fetch = checkpointFetch;
      runningHeaders = headers;
      runningWindow = window;
    }
    // outside the lock: the downloads run callbacks under their own locks
    if (fetch != null) {
      fetch.cancel(false);
    }
    if (runningHeaders != null) {
      runningHeaders.stop();
    }
    if (runningWindow != null) {
      runningWindow.stop();
    }
    windowWorkers.shutdownNow();
    headersDone.completeExceptionally(new CancellationException("attempt abandoned"));
  }

  /** Waits for the workers of the window to stop, once cancelled, before the storage closes. */
  void awaitStop() throws InterruptedException {
    if (!windowWorkers.awaitTermination(WORKERS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
      LOG.warn("SNAP2 window workers still running after {}", WORKERS_TIMEOUT);
    }
  }

  private void throwIfCancelled() {
    if (cancelled) {
      throw new CancellationException("attempt abandoned");
    }
  }

  private Optional<Checkpoint> trustedCheckpoint() {
    return checkpoint.filter(trusted -> trusted.blockNumber() > 0);
  }

  // genesis, or the checkpoint, downloaded when the node lacks it
  private BlockHeader floorHeader() {
    if (trustedCheckpoint().isEmpty()) {
      return blockchain.getGenesisBlockHeader();
    }
    final Hash hash = checkpoint.get().blockHash();
    final BlockHeader header =
        blockchain.getBlockHeader(hash).orElseGet(() -> fetchCheckpoint(hash));
    if (header.getNumber() != checkpoint.get().blockNumber()) {
      throw new IllegalStateException(
          "checkpoint " + hash + " is not at block " + checkpoint.get().blockNumber());
    }
    return header;
  }

  // peers may not serve it yet: the sync starts over rather than fails
  private BlockHeader fetchCheckpoint(final Hash hash) {
    final CompletableFuture<BlockHeader> fetch;
    synchronized (this) {
      throwIfCancelled();
      fetch = fetcher.byHash(hash, Priority.HEADERS);
      checkpointFetch = fetch;
    }
    try {
      return fetch.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CancellationException("interrupted while downloading the checkpoint");
    } catch (final ExecutionException e) {
      throw new RestartException(
          "checkpoint " + hash + " unavailable: " + rootCause(e).getMessage(), e);
    }
  }

  // left by a failed attempt or an earlier run; those above a gap stay until a pivot passes them
  private void removeIndexAbove(final long number) {
    long highest = number;
    while (blockchain.getBlockHashByNumber(highest + 1).isPresent()) {
      highest++;
    }
    if (highest > number) {
      final long last = highest;
      retryWrite(() -> blockchain.unsafeRemoveCanonicalIndexRange(number, last));
    }
  }

  // with proof of stake above the floor, the headers above it share its total difficulty
  private void writeHeaders(final List<BlockHeader> headers) {
    final List<BlockHeader> below = new ArrayList<>();
    final List<BlockHeader> above = new ArrayList<>();
    for (final BlockHeader header : headers) {
      if (header.getNumber() > floor.getNumber()) {
        above.add(header);
      } else if (header.getNumber() < floor.getNumber()
          || header.getHash().equals(floor.getHash())) {
        below.add(header);
      } else {
        // headers down to genesis pass the checkpoint, which must be on their chain
        throw new WrongChainException(
            "checkpoint " + floor.getHash() + " is not on the chain of the pivot");
      }
    }
    if (headerTotalDifficulty.isPresent()
        && above.stream().anyMatch(header -> !header.getDifficulty().isZero())) {
      throw new WrongChainException(
          "proof-of-work blocks above the terminal total difficulty lead to the pivot");
    }
    // in batches, which have no conflict check to retry
    if (!below.isEmpty()) {
      blockchain.storeBlockHeaders(below);
    }
    if (!above.isEmpty()) {
      headerTotalDifficulty.ifPresentOrElse(
          total -> blockchain.storeBlockHeaders(above, total),
          () -> blockchain.storeBlockHeaders(above));
    }
  }

  // above a floor followed by proof of work, the total difficulty of every header up to the head
  private Difficulty storeTotalDifficulties(final BlockHeader head) {
    BlockHeader parent = floor;
    Difficulty total = floorTotal;
    while (parent.getNumber() < head.getNumber()) {
      // a cancel waits for one commit at most
      throwIfCancelled();
      final long last = Math.min(head.getNumber(), parent.getNumber() + totalDifficultiesPerCommit);
      total = blockchain.unsafeStoreSyncTotalDifficulties(parent, total, last);
      parent =
          blockchain
              .getBlockHeader(last)
              .orElseThrow(() -> new IllegalStateException("no header for block " + last));
    }
    return total;
  }

  // the headers below a stored one, down to the checkpoint or genesis
  private synchronized void downloadHeaders(final BlockHeader top) {
    final HeaderBackfill download =
        new HeaderBackfill(
            sender,
            protocolSchedule,
            this::writeHeaders,
            fetcher,
            settings.headersToCheckpointOnly() ? floor : blockchain.getGenesisBlockHeader(),
            top,
            settings.headersPerRequest(),
            settings.headerParallelism(),
            metricsSystem);
    headers = download;
    download.run(executor).whenComplete((unused, error) -> downloaded(download, error));
  }

  // runs under the lock of the pipeline of the download, so it takes no lock of its own
  private void downloaded(final HeaderBackfill download, final Throwable error) {
    if (error == null) {
      headersDone.complete(null);
      return;
    }
    // its requests in flight are no longer awaited
    download.stop();
    final Throwable cause = rootCause(error);
    if (cause instanceof WrongChainException || cause instanceof StorageException) {
      // another chain, or a storage failure: a resume would end the same way
      headersDone.completeExceptionally(cause);
    } else if (!cancelled) {
      LOG.warn("SNAP2 header download failed, resuming: {}", cause.getMessage());
      final long lowestStored = download.lowestStored();
      CompletableFuture.runAsync(() -> resumeBelow(lowestStored), delays.apply(RESUME_DELAY))
          .exceptionally(
              failure -> {
                headersDone.completeExceptionally(failure);
                return null;
              });
    }
  }

  // starts again below the lowest header the failed download stored
  private synchronized void resumeBelow(final long lowestStored) {
    if (cancelled) {
      return;
    }
    downloadHeaders(
        blockchain
            .getBlockHeader(lowestStored)
            .orElseThrow(() -> new IllegalStateException("lost the lowest stored header")));
  }

  private synchronized CompletableFuture<Void> startWindow(final BlockHeader pivot) {
    throwIfCancelled();
    checkState(window == null, "the window downloads once");
    final long from = windowStart(pivot.getNumber());
    window =
        new BodyBackfill(
            blockchain,
            protocolSchedule,
            sender,
            () -> true,
            pivot.getNumber() + 1,
            head -> from,
            BodyBackfill.Settings.forWindow(settings.indexTransactions()),
            metricsSystem);
    windowPivot = pivot;
    window.start(windowWorkers);
    return window.done();
  }

  // the window downloaded up to this head, with every body stored and recorded
  private boolean windowStored(final BlockHeader head) {
    return head.equals(windowPivot) && window.done().state() == Future.State.SUCCESS;
  }

  // the lowest block whose body and receipts are needed before the head is set at this pivot
  private long windowStart(final long pivot) {
    return Math.max(floor.getNumber() + 1, pivot - settings.bodyWindow() + 1);
  }

  // a write in a transaction that RocksDB checks for conflicts
  private void retryWrite(final Runnable write) {
    WriteRetry.run(write, pause);
  }
}
