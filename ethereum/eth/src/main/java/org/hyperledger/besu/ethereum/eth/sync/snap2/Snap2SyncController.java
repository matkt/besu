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
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerSelector;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.sync.TrailingPeerRequirements;
import org.hyperledger.besu.ethereum.eth.sync.snap2.SyncAttempt.Resume;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BalFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.ChainHeadSource;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncController;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SNAP2 sync: downloads the chain and the state together, the state flat at a pivot that trails the
 * head and follows it through block access lists, then builds the tries once the state is complete.
 * One run is a {@link SyncAttempt}; the sync starts over when the state cannot be recovered.
 */
final class Snap2SyncController implements SnapSyncController {

  private static final Logger LOG = LoggerFactory.getLogger(Snap2SyncController.class);
  private static final int MAX_RESTARTS = 3;
  private static final Duration RETRY_DELAY = Duration.ofSeconds(5);
  private static final long PROGRESS_LOG_SECONDS = 30;
  // rounds of requests before a block access list counts as unavailable
  private static final int BAL_ROUNDS = 60;

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
  private final PeerSelector peers;
  private final PeerTaskExecutor peerTaskExecutor;

  private final ExecutorService executor =
      Executors.newCachedThreadPool(Thread.ofPlatform().name("snap2-sync-", 0).daemon().factory());
  private final ScheduledExecutorService control =
      Executors.newSingleThreadScheduledExecutor(
          Thread.ofPlatform().name("snap2-control").daemon().factory());
  private final CompletableFuture<SnapSyncProcessState> result = new CompletableFuture<>();

  private SyncContext context;
  private Snap2Metrics metrics;
  private volatile SyncAttempt current;
  private volatile boolean stopped;

  Snap2SyncController(
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
      final Supplier<RequestSender> senders,
      final PeerSelector peers,
      final PeerTaskExecutor peerTaskExecutor) {
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
    this.peers = peers;
    this.peerTaskExecutor = peerTaskExecutor;
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

  @VisibleForTesting
  Snap2Metrics metrics() {
    return metrics;
  }

  @Override
  public Optional<TrailingPeerRequirements> calculateTrailingPeerRequirements() {
    return Optional.empty();
  }

  private void prepare() {
    marker.create();
    final RequestSender scheduler = senders.get();
    metrics = new Snap2Metrics(ethPeers, () -> current);
    metrics.register(metricsSystem);
    context =
        new SyncContext(
            config,
            protocolSchedule,
            blockchain,
            syncState,
            storage,
            heads,
            metricsSystem,
            syncDurationMetrics,
            headersToCheckpointOnly,
            indexTransactions,
            scheduler,
            peers,
            peerTaskExecutor,
            new HeaderFetcher(scheduler, protocolSchedule, heads, config.headersPerRequest()),
            new BalFetcher(scheduler, config.balBatchSize(), config.balResponseBytes(), BAL_ROUNDS),
            executor,
            control,
            () -> stopped,
            metrics);
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
        .thenComposeAsync(
            pivot -> {
              final SyncAttempt attempt = new SyncAttempt(context, pivot, resume);
              current = attempt;
              return attempt.start();
            },
            executor)
        .exceptionallyComposeAsync(
            error -> {
              final SyncAttempt failed = current;
              if (failed != null) {
                failed.cancel();
              }
              if (stopped
                  || metrics.restarts() >= MAX_RESTARTS
                  || !(Futures.unwrap(error) instanceof RestartException restart)) {
                return CompletableFuture.failedFuture(error);
              }
              metrics.restarted();
              LOG.warn("Restarting the SNAP2 sync from scratch: {}", restart.getMessage());
              return run(failed == null ? resume : failed.resume());
            },
            executor);
  }

  private CompletableFuture<Void> waitForPeers() {
    if (stopped) {
      return CompletableFuture.failedFuture(new CancellationException("stopped"));
    }
    if (context.scheduler().peerCount(Protocol.SNAP) >= config.minPeers()) {
      return CompletableFuture.completedFuture(null);
    }
    LOG.info("Waiting for {} snap peers, {}", config.minPeers(), metrics.peerSummary());
    return Futures.delay(RETRY_DELAY, executor).thenCompose(unused -> waitForPeers());
  }

  // the pivot trails the trusted head, and the blocks after it must carry block access lists
  private CompletableFuture<BlockHeader> selectPivot() {
    if (stopped) {
      return CompletableFuture.failedFuture(new CancellationException("stopped"));
    }
    final Optional<Hash> head = heads.headHash();
    if (head.isEmpty()) {
      LOG.info("Waiting for the consensus client to send a head");
      return Futures.delay(RETRY_DELAY, executor).thenCompose(unused -> selectPivot());
    }
    final HeaderFetcher fetcher = context.fetcher();
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
                return Futures.delay(Duration.ofSeconds(30), executor)
                    .thenCompose(unused -> selectPivot());
              }
              LOG.info("Selected pivot block {} ({})", pivot.getNumber(), pivot.getHash());
              return CompletableFuture.completedFuture(pivot);
            })
        .exceptionallyCompose(
            error -> {
              if (stopped) {
                return CompletableFuture.failedFuture(error);
              }
              LOG.info("Pivot selection failed, retrying: {}", Futures.unwrap(error).getMessage());
              return Futures.delay(RETRY_DELAY, executor).thenCompose(unused -> selectPivot());
            });
  }

  private void maybeMoveSafely() {
    try {
      final SyncAttempt attempt = current;
      if (attempt != null) {
        attempt.maybeMove();
      }
    } catch (final RuntimeException e) {
      LOG.warn("Pivot move check failed", e);
    }
  }

  private void logProgress() {
    try {
      final SyncAttempt attempt = current;
      if (attempt == null) {
        return;
      }
      LOG.info(
          "SNAP2 sync progress: {}, {} pivot moves, {}",
          attempt.progress(),
          metrics.pivotMoves(),
          metrics.peerSummary());
    } catch (final RuntimeException e) {
      LOG.debug("Failed to log the SNAP2 progress", e);
    }
  }

  private void shutdown() {
    final SyncAttempt attempt = current;
    if (attempt != null) {
      attempt.cancel();
    }
    if (context != null) {
      context.balFetcher().stop();
      context.scheduler().stop();
    }
    control.shutdownNow();
    executor.shutdownNow();
  }
}
