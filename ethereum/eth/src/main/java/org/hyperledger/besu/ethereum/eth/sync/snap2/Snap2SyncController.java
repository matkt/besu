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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
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
  private static final int BAL_BATCH_SIZE = 32;
  private static final int BAL_RESPONSE_BYTES = 2 * 1024 * 1024;
  private static final int MIN_PEERS = 1;

  private final Snap2Configuration config;
  private final ProtocolSchedule protocolSchedule;
  private final MutableBlockchain blockchain;
  private final EthPeers ethPeers;
  private final SyncState syncState;
  private final SyncStateStorage storage;
  private final ChainHeadSource heads;
  private final MetricsSystem metricsSystem;
  // marks a sync in progress: a node restarted while it exists syncs again from scratch
  private final Path marker;
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

  private SyncAttempt.Context context;
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
      final Path marker,
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
    LOG.info("SNAP2 sync started");
    final Instant started = Instant.now();
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
                LOG.info(
                    "SNAP2 sync complete at {} in {} | {} pivot moves, {} restarts",
                    ProgressFormat.block(pivot.getNumber()),
                    ProgressFormat.duration(Duration.between(started, Instant.now())),
                    metrics.pivotMoves(),
                    metrics.restarts());
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
    try {
      Files.deleteIfExists(marker);
    } catch (final IOException e) {
      throw new UncheckedIOException("cannot delete " + marker, e);
    }
  }

  /** Returns the file marking a SNAP2 sync in progress. */
  static Path markerFile(final Path dataDirectory) {
    return dataDirectory.resolve("snap2-sync-in-progress");
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
    try {
      Files.createDirectories(marker.getParent());
      if (!Files.exists(marker)) {
        Files.createFile(marker);
      }
    } catch (final IOException e) {
      throw new UncheckedIOException("cannot create " + marker, e);
    }
    final RequestSender scheduler = senders.get();
    metrics = new Snap2Metrics(ethPeers, () -> current);
    metrics.register(metricsSystem);
    context =
        new SyncAttempt.Context(
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
            new BalFetcher(scheduler, BAL_BATCH_SIZE, BAL_RESPONSE_BYTES, BAL_ROUNDS),
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
                  || !(SyncAttempt.unwrap(error) instanceof SyncAttempt.RestartException restart)) {
                return CompletableFuture.failedFuture(error);
              }
              metrics.restarted();
              LOG.warn(
                  "SNAP2 restart {} of {}: {}",
                  metrics.restarts(),
                  MAX_RESTARTS,
                  restart.getMessage());
              return run(failed == null ? resume : failed.resume());
            },
            executor);
  }

  private CompletableFuture<Void> waitForPeers() {
    if (stopped) {
      return CompletableFuture.failedFuture(new CancellationException("stopped"));
    }
    if (context.scheduler().peerCount(Protocol.SNAP) >= MIN_PEERS) {
      return CompletableFuture.completedFuture(null);
    }
    LOG.info("SNAP2 waiting for {} snap peers | {}", MIN_PEERS, metrics.peerCounts());
    return SyncAttempt.delay(RETRY_DELAY, executor).thenCompose(unused -> waitForPeers());
  }

  // the pivot trails the trusted head, and the blocks after it must carry block access lists
  private CompletableFuture<BlockHeader> selectPivot() {
    if (stopped) {
      return CompletableFuture.failedFuture(new CancellationException("stopped"));
    }
    final Optional<Hash> head = heads.headHash();
    if (head.isEmpty()) {
      LOG.info("SNAP2 waiting for the consensus client to send a head");
      return SyncAttempt.delay(RETRY_DELAY, executor).thenCompose(unused -> selectPivot());
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
                    "SNAP2 waiting for the fork that adds block access lists, {} has none",
                    ProgressFormat.block(next.getNumber()));
                return SyncAttempt.delay(Duration.ofSeconds(30), executor)
                    .thenCompose(unused -> selectPivot());
              }
              LOG.info(
                  "SNAP2 pivot selected: {} ({}), {} blocks behind the head",
                  ProgressFormat.block(pivot.getNumber()),
                  pivot.getHash(),
                  chain.size() - 1);
              return CompletableFuture.completedFuture(pivot);
            })
        .exceptionallyCompose(
            error -> {
              if (stopped) {
                return CompletableFuture.failedFuture(error);
              }
              LOG.info(
                  "SNAP2 pivot selection failed, retrying: {}",
                  SyncAttempt.unwrap(error).getMessage());
              return SyncAttempt.delay(RETRY_DELAY, executor).thenCompose(unused -> selectPivot());
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

  // one line a tick, led by the phase and its share done; the peers only when they are few
  private void logProgress() {
    try {
      final SyncAttempt attempt = current;
      if (attempt == null) {
        return;
      }
      final Snap2Metrics.PeerCounts peers = metrics.peerCounts();
      LOG.info("{}{}", attempt.progress(), peers.few() ? " | " + peers : "");
      if (LOG.isDebugEnabled()) {
        LOG.debug("{}", attempt.details());
      }
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
      context.scheduler().stop();
    }
    control.shutdownNow();
    executor.shutdownNow();
  }
}
