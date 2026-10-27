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
  private static final int WRITE_ATTEMPTS = 6;
  private static final Duration RESUME_DELAY = Duration.ofSeconds(5);
  // a stopped window writes nothing, its workers only end the reads they started
  private static final Duration WORKERS_TIMEOUT = Duration.ofSeconds(30);

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

  /** Waits between two tries of a write. */
  interface Pause {
    void sleep(long millis) throws InterruptedException;
  }

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
  private final Pause pause;
  private final CompletableFuture<Void> headersDone = new CompletableFuture<>();

  private volatile BlockHeader floor;
  // when every block above the floor is proof of stake, the total difficulty of every header
  private volatile Optional<Difficulty> headerTotalDifficulty = Optional.empty();
  private volatile boolean cancelled;
  // guarded by this
  private CompletableFuture<BlockHeader> checkpointFetch;
  private HeaderBackfill headers;
  private BodyBackfill window;

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
        Thread::sleep);
  }

  /**
   * Creates the chain of an attempt whose window downloads on {@code windowWorkers}, whose failed
   * header downloads wait on the executors {@code delays} returns, and whose writes wait {@code
   * pause} between two tries.
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
      final Pause pause) {
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
  }

  /**
   * Sets the chain head on the floor, removes the canonical entries above the first pivot, stores
   * it, and starts downloading the headers below it.
   *
   * @throws RestartException when peers do not serve the checkpoint
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
      floor = floorHeader;
      trustedCheckpoint()
          .ifPresent(
              trusted ->
                  retryWrite(
                      () -> blockchain.unsafeStoreHeader(floorHeader, trusted.totalDifficulty())));
      final Difficulty floorTotal =
          blockchain
              .getTotalDifficultyByHash(floorHeader.getHash())
              .orElseThrow(() -> new IllegalStateException("no total difficulty for the floor"));
      // the chain head stays on the floor until every body above it is stored
      retryWrite(() -> blockchain.unsafeSetChainHead(floorHeader, floorTotal));
      // left by a failed attempt, or by an earlier run of the node
      final long highestIndexed = highestIndexedFrom(firstPivot.getNumber());
      if (highestIndexed > firstPivot.getNumber()) {
        retryWrite(
            () ->
                blockchain.unsafeRemoveCanonicalIndexRange(firstPivot.getNumber(), highestIndexed));
      }
      // only proof of stake follows the terminal total difficulty, which Hoodi's genesis reaches
      if (settings.terminalTotalDifficulty().map(floorTotal::greaterOrEqualThan).orElse(false)) {
        headerTotalDifficulty = Optional.of(floorTotal);
      }
      writeHeaders(List.of(firstPivot));
      downloadHeaders(firstPivot);
    }
  }

  /** Completes once the headers reach the floor; fails when they lead to another chain. */
  CompletableFuture<Void> headersDone() {
    return headersDone;
  }

  /** Stores headers of the chain of the pivot, which must lead to the floor. */
  synchronized void storeHeaders(final List<BlockHeader> headers) {
    throwIfCancelled();
    writeHeaders(headers);
  }

  /**
   * Downloads the bodies and receipts of the window below the final pivot once the headers are
   * stored; completes once they are stored and recorded.
   */
  CompletableFuture<Void> downloadWindow(final BlockHeader pivot) {
    return headersDone.thenComposeAsync(unused -> startWindow(pivot), executor);
  }

  /** Makes the final pivot the chain head, once the headers and the window below it are stored. */
  synchronized void setChainHead(final BlockHeader head) {
    throwIfCancelled();
    retryWrite(() -> blockchain.unsafeSetSyncChainHead(floor, head));
    LOG.info(
        "SNAP2 chain head set to {} | bodies and receipts from {}",
        block(head.getNumber()),
        block(windowStart(head.getNumber())));
  }

  /** Stops the downloads and fails {@link #headersDone}; {@link #awaitStop} awaits the writes. */
  void cancel() {
    final CompletableFuture<BlockHeader> fetch;
    final HeaderBackfill runningHeaders;
    final BodyBackfill runningWindow;
    synchronized (this) {
      cancelled = true;
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

  // the highest block of the canonical index that goes on from this one
  private long highestIndexedFrom(final long number) {
    long highest = number;
    while (blockchain.getBlockHashByNumber(highest + 1).isPresent()) {
      highest++;
    }
    return highest;
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
    retryWrite(() -> store(below, above));
  }

  private void store(final List<BlockHeader> below, final List<BlockHeader> above) {
    if (!below.isEmpty()) {
      blockchain.storeBlockHeaders(below);
    }
    if (!above.isEmpty()) {
      headerTotalDifficulty.ifPresentOrElse(
          total -> blockchain.storeBlockHeaders(above, total),
          () -> blockchain.storeBlockHeaders(above));
    }
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
    } else if (rootCause(error) instanceof WrongChainException wrong) {
      headersDone.completeExceptionally(wrong);
    } else if (!cancelled) {
      LOG.warn("SNAP2 header download failed, resuming: {}", rootCause(error).getMessage());
      // its requests in flight are no longer awaited
      download.stop();
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
    window.start(windowWorkers);
    return window.done();
  }

  // the lowest block whose body and receipts are needed before the head is set at this pivot
  private long windowStart(final long pivot) {
    return Math.max(floor.getNumber() + 1, pivot - settings.bodyWindow() + 1);
  }

  // a write that starts its own transaction, which RocksDB may fail to check, is tried again
  private void retryWrite(final Runnable write) {
    for (int attempt = 1; attempt < WRITE_ATTEMPTS; attempt++) {
      try {
        write.run();
        return;
      } catch (final StorageException e) {
        LOG.debug("Write not committed, retrying: {}", e.getMessage());
        try {
          pause.sleep(10L << attempt);
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
    write.run();
  }
}
