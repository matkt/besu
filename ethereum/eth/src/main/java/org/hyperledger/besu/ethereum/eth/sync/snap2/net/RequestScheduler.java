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
package org.hyperledger.besu.ethereum.eth.sync.snap2.net;

import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.DISCONNECTED;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.EMPTY;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.INVALID;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.NO_PEER;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.STOPPED;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException.Reason.TIMEOUT;

import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.RequestManager.ResponseStream;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.EmptyResponseException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.InvalidResponseException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection.PeerNotConnected;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dispatches every request of the sync, chain and state alike, to the connected peers: most urgent
 * requests first, each to the peer with the best observed throughput that still has capacity.
 *
 * <p>Each peer has one lane per protocol, so chain and state requests never wait for each other.
 * The number of requests a lane accepts adapts to the peer: it grows with each answered request and
 * halves on a timeout, up to a maximum.
 *
 * <p>Dispatching, timeouts and peer bookkeeping run on a single loop thread; responses are decoded
 * on a worker pool. A timed out request keeps its stream open so that a late response is absorbed
 * instead of being treated as unsolicited, and keeps using a slot of the peer until then.
 */
public final class RequestScheduler implements RequestSender {

  private static final Logger LOG = LoggerFactory.getLogger(RequestScheduler.class);
  private static final double INITIAL_THROUGHPUT = 64; // bytes per millisecond
  private static final double INITIAL_LANE_LIMIT = 2;
  private static final int MAX_BACKOFF_SECONDS = 30;
  private static final int CONSECUTIVE_FAILURES_BEFORE_DISCONNECT = 12;
  // announced heights lag, so a peer that looks behind is only picked when no other peer can be
  private static final double BEHIND_PENALTY = 1e-3;

  private static final int PENDING = 0;
  private static final int DONE = 1;
  private static final int TIMED_OUT = 2;
  private static final int RELEASED = 3;

  private final EthPeers ethPeers;
  private final Duration timeout;
  private final Duration queueTimeout;
  private final int maxSnapRequestsPerPeer;
  private final int maxEthRequestsPerPeer;
  private final ScheduledExecutorService loop;
  private final ExecutorService workers;
  private final LabelledMetric<Counter> requestCounter;
  private final LabelledMetric<Counter> responseBytes;
  private final LabelledMetric<Counter> responseMillis;

  private final PriorityQueue<Pending<?>> queue =
      new PriorityQueue<>(
          Comparator.<Pending<?>>comparingInt(p -> p.request.priority().ordinal())
              .thenComparingLong(p -> p.sequence));
  private final Map<EthPeer, PeerState> peers = new HashMap<>();
  private long sequence;
  private final long connectSubscription;
  private volatile int snapCapacity;
  private volatile int ethCapacity;
  private volatile int snapInFlight;
  private volatile int ethInFlight;
  private volatile int queued;
  private volatile boolean stopped;

  public RequestScheduler(
      final EthPeers ethPeers,
      final MetricsSystem metricsSystem,
      final Duration timeout,
      final int maxSnapRequestsPerPeer,
      final int maxEthRequestsPerPeer,
      final int workerThreads) {
    this.ethPeers = ethPeers;
    this.timeout = timeout;
    this.queueTimeout = timeout.multipliedBy(6);
    this.maxSnapRequestsPerPeer = maxSnapRequestsPerPeer;
    this.maxEthRequestsPerPeer = maxEthRequestsPerPeer;
    this.loop =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("snap2-scheduler").daemon().factory());
    this.workers =
        Executors.newFixedThreadPool(
            workerThreads, Thread.ofPlatform().name("snap2-worker-", 0).daemon().factory());
    this.requestCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "snap2_requests_total",
            "Requests sent by the SNAP2 sync, by type and outcome",
            "request",
            "outcome");
    this.responseBytes =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "snap2_response_bytes_total",
            "Bytes of the responses received by the SNAP2 sync, by request type",
            "request");
    this.responseMillis =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "snap2_response_milliseconds_total",
            "Time peers took to answer SNAP2 requests, by request type",
            "request");
    final var inFlight =
        metricsSystem.createLabelledSuppliedGauge(
            BesuMetricCategory.SYNCHRONIZER,
            "snap2_requests_in_flight",
            "Requests of the SNAP2 sync waiting for an answer, by protocol",
            "protocol");
    inFlight.labels(() -> snapInFlight, "snap");
    inFlight.labels(() -> ethInFlight, "eth");
    final var capacity =
        metricsSystem.createLabelledSuppliedGauge(
            BesuMetricCategory.SYNCHRONIZER,
            "snap2_request_capacity",
            "Requests the peers currently accept from the SNAP2 sync, by protocol",
            "protocol");
    capacity.labels(() -> snapCapacity, "snap");
    capacity.labels(() -> ethCapacity, "eth");
    metricsSystem.createIntegerGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "snap2_requests_queued",
        "Requests of the SNAP2 sync waiting for a peer",
        () -> queued);
    ethPeers.subscribeDisconnect(peer -> execute(() -> peers.remove(peer)));
    this.connectSubscription = ethPeers.subscribeConnect(peer -> execute(this::dispatch));
    loop.scheduleWithFixedDelay(this::dispatchSafely, 1, 1, TimeUnit.SECONDS);
  }

  @Override
  public ExecutorService workers() {
    return workers;
  }

  @Override
  public <T> CompletableFuture<PeerResponse<T>> submit(
      final SyncRequest<T> request, final Set<EthPeer> excluded) {
    final Pending<T> pending = new Pending<>(request, Set.copyOf(excluded));
    if (stopped) {
      pending.future.completeExceptionally(failure(STOPPED, null, "scheduler stopped"));
      return pending.future;
    }
    execute(
        () -> {
          pending.sequence = sequence++;
          pending.queuedAt = System.nanoTime();
          queue.add(pending);
          dispatch();
        });
    return pending.future;
  }

  @Override
  public <T> CompletableFuture<PeerResponse<T>> submitWithRetries(
      final SyncRequest<T> request, final int attempts) {
    final CompletableFuture<PeerResponse<T>> result = new CompletableFuture<>();
    attempt(request, new HashSet<>(), attempts, result);
    return result;
  }

  private <T> void attempt(
      final SyncRequest<T> request,
      final Set<EthPeer> excluded,
      final int attemptsLeft,
      final CompletableFuture<PeerResponse<T>> result) {
    submit(request, excluded)
        .whenComplete(
            (response, error) -> {
              if (error == null) {
                result.complete(response);
                return;
              }
              final RequestFailedException failure =
                  RequestFailedException.from(error)
                      .orElseGet(() -> failure(INVALID, null, error.toString()));
              if (attemptsLeft <= 1 || failure.reason() == STOPPED || result.isDone()) {
                result.completeExceptionally(failure);
                return;
              }
              if (failure.reason() == NO_PEER) {
                // every peer may have been tried, start over after a pause
                excluded.clear();
                schedule(
                    () -> attempt(request, excluded, attemptsLeft - 1, result),
                    Duration.ofSeconds(1));
              } else {
                failure.peer().ifPresent(excluded::add);
                attempt(request, excluded, attemptsLeft - 1, result);
              }
            });
  }

  @Override
  public int peerCount(final Protocol protocol) {
    return (int)
        ethPeers
            .streamAvailablePeers()
            .map(EthPeerImmutableAttributes::ethPeer)
            .filter(peer -> speaks(peer, protocol))
            .count();
  }

  @Override
  public long bestPeerHeight() {
    return ethPeers
        .streamAvailablePeers()
        .mapToLong(peer -> peer.ethPeer().chainState().getEstimatedHeight())
        .max()
        .orElse(0);
  }

  @Override
  public int capacity(final Protocol protocol) {
    final int adapted = protocol == Protocol.SNAP ? snapCapacity : ethCapacity;
    return adapted > 0 ? adapted : peerCount(protocol) * (int) INITIAL_LANE_LIMIT;
  }

  private ScheduledFuture<?> schedule(final Runnable task, final Duration delay) {
    return loop.schedule(task, delay.toMillis(), TimeUnit.MILLISECONDS);
  }

  /** Stops dispatching and fails every queued request. */
  @Override
  public void stop() {
    stopped = true;
    ethPeers.unsubscribeConnect(connectSubscription);
    execute(
        () -> {
          Pending<?> pending;
          while ((pending = queue.poll()) != null) {
            pending.future.completeExceptionally(failure(STOPPED, null, "scheduler stopped"));
          }
        });
    loop.shutdown();
    workers.shutdownNow();
  }

  private void execute(final Runnable task) {
    if (!loop.isShutdown()) {
      try {
        loop.execute(task);
      } catch (final RuntimeException e) {
        LOG.trace("Scheduler loop rejected a task", e);
      }
    }
  }

  private void dispatchSafely() {
    try {
      dispatch();
    } catch (final RuntimeException e) {
      LOG.warn("SNAP2 request dispatch failed", e);
    }
  }

  private void dispatch() {
    if (stopped) {
      return;
    }
    final long now = System.nanoTime();
    final List<PeerState> candidates = new ArrayList<>();
    ethPeers
        .streamAvailablePeers()
        .map(EthPeerImmutableAttributes::ethPeer)
        .filter(EthPeer::isFullyValidated)
        .forEach(peer -> candidates.add(peers.computeIfAbsent(peer, this::newPeerState)));
    updateCapacities(candidates);
    if (queue.isEmpty()) {
      return;
    }

    final List<Pending<?>> deferred = new ArrayList<>();
    Pending<?> pending;
    while ((pending = queue.poll()) != null) {
      if (pending.future.isDone()) {
        continue;
      }
      final PeerState peer = select(pending, candidates, now);
      if (peer != null) {
        send(pending, peer);
      } else if (now - pending.queuedAt > queueTimeout.toNanos()) {
        countRequest(pending.request, "no_peer");
        pending.future.completeExceptionally(
            failure(NO_PEER, null, "no peer could serve " + pending.request.name()));
      } else {
        deferred.add(pending);
      }
    }
    queue.addAll(deferred);
  }

  private PeerState newPeerState(final EthPeer peer) {
    return new PeerState(peer, maxSnapRequestsPerPeer, maxEthRequestsPerPeer);
  }

  // requests the state and chain downloads may keep in flight, from the adapted lane limits
  private void updateCapacities(final List<PeerState> candidates) {
    int snap = 0;
    int eth = 0;
    int snapBusy = 0;
    int ethBusy = 0;
    for (final PeerState state : candidates) {
      eth += state.eth.slots();
      ethBusy += state.eth.inFlight;
      snapBusy += state.snap.inFlight;
      if (speaks(state.peer, Protocol.SNAP)) {
        snap += state.snap.slots();
      }
    }
    snapCapacity = snap;
    ethCapacity = eth;
    snapInFlight = snapBusy;
    ethInFlight = ethBusy;
    queued = queue.size();
  }

  private PeerState select(
      final Pending<?> pending, final List<PeerState> candidates, final long now) {
    PeerState best = null;
    double bestScore = -1;
    final long minimumHeight = pending.request.minimumPeerHeight();
    for (final PeerState state : candidates) {
      final Protocol protocol = pending.request.protocolFor(state.peer);
      final Lane lane = state.lane(protocol);
      if (state.peer.isDisconnected()
          || state.backoffUntil > now
          || pending.excluded.contains(state.peer)
          || !lane.hasRoom()
          || !speaks(state.peer, protocol)
          || !pending.request.canServe(state.peer)) {
        continue;
      }
      final double score =
          state.throughput
              / (lane.inFlight + 1)
              * (behind(state.peer, minimumHeight) ? BEHIND_PENALTY : 1);
      if (score > bestScore) {
        best = state;
        bestScore = score;
      }
    }
    return best;
  }

  private <T> void send(final Pending<T> pending, final PeerState state) {
    final SyncRequest<T> request = pending.request;
    final EthPeer peer = state.peer;
    final Protocol protocol = request.protocolFor(peer);
    ResponseStream stream = null;
    try {
      stream = peer.send(request.message(peer), protocolName(protocol));
    } catch (final PeerNotConnected e) {
      LOG.trace("Peer {} disconnected before sending {}", peer.getLoggableId(), request.name());
    } catch (final RuntimeException e) {
      LOG.debug("Failed to send {} to {}", request.name(), peer.getLoggableId(), e);
    }
    if (stream == null) {
      state.backoff(System.nanoTime());
      queue.add(pending.excluding(peer));
      return;
    }
    final Lane lane = state.lane(protocol);
    lane.inFlight++;
    final Attempt<T> attempt = new Attempt<>(pending, state, lane, stream, System.nanoTime());
    attempt.timeoutTask =
        loop.schedule(() -> onTimeout(attempt), timeout.toMillis(), TimeUnit.MILLISECONDS);
    stream.then(
        (closed, message, sender) -> {
          if (message != null) {
            onMessage(attempt, message);
          } else if (closed) {
            onClosed(attempt);
          }
        });
  }

  private <T> void onMessage(final Attempt<T> attempt, final MessageData message) {
    final boolean answered = attempt.state.compareAndSet(PENDING, DONE);
    if (!answered && attempt.state.compareAndSet(TIMED_OUT, RELEASED)) {
      execute(
          () -> {
            attempt.lane.abandoned--;
            dispatch();
          });
    }
    // only once the state is settled: closing calls back synchronously with a closed stream
    attempt.stream.close();
    if (!answered) {
      return;
    }
    final ScheduledFuture<?> timeoutTask = attempt.timeoutTask;
    if (timeoutTask != null) {
      timeoutTask.cancel(false);
    }
    final long elapsedMillis =
        Math.max(1, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - attempt.startedAt));
    final int size = message.getSize();
    final SyncRequest<T> request = attempt.pending.request;
    final EthPeer peer = attempt.peer.peer;
    try {
      workers.execute(
          () -> {
            String outcome;
            try {
              final T value = request.decode(message, peer);
              attempt.pending.future.complete(new PeerResponse<>(value, peer));
              outcome = "success";
            } catch (final EmptyResponseException e) {
              outcome = "empty";
              LOG.debug(
                  "Empty {} ({} bytes) from {} {} at height {}: {}",
                  request.name(),
                  size,
                  peer.getLoggableId(),
                  peer.getConnection().getPeerInfo().getClientId(),
                  peer.chainState().getEstimatedHeight(),
                  e.getMessage());
              attempt.pending.future.completeExceptionally(failure(EMPTY, peer, e.getMessage()));
            } catch (final InvalidResponseException e) {
              outcome = "invalid";
              punish(peer, request, e.getMessage(), e.shouldDisconnect());
              attempt.pending.future.completeExceptionally(failure(INVALID, peer, e.getMessage()));
            } catch (final RLPException | IllegalArgumentException e) {
              outcome = "invalid";
              punish(peer, request, e.getMessage(), true);
              attempt.pending.future.completeExceptionally(failure(INVALID, peer, e.toString()));
            } catch (final RuntimeException e) {
              outcome = "error";
              LOG.warn("Failed to process {} from {}", request.name(), peer.getLoggableId(), e);
              attempt.pending.future.completeExceptionally(failure(INVALID, peer, e.toString()));
            }
            final String result = outcome;
            execute(
                () -> {
                  attempt.lane.inFlight--;
                  if ("success".equals(result)) {
                    attempt.lane.grow();
                  }
                  attempt.peer.record(result, size, elapsedMillis, System.nanoTime());
                  countRequest(request, result);
                  responseBytes.labels(request.name()).inc(size);
                  responseMillis.labels(request.name()).inc(elapsedMillis);
                  dispatch();
                });
          });
    } catch (final RuntimeException rejected) {
      attempt.pending.future.completeExceptionally(failure(STOPPED, peer, "scheduler stopped"));
    }
  }

  private void onClosed(final Attempt<?> attempt) {
    if (attempt.state.compareAndSet(PENDING, DONE)) {
      final ScheduledFuture<?> timeoutTask = attempt.timeoutTask;
      if (timeoutTask != null) {
        timeoutTask.cancel(false);
      }
      attempt.pending.future.completeExceptionally(
          failure(DISCONNECTED, attempt.peer.peer, "stream closed"));
      execute(
          () -> {
            attempt.lane.inFlight--;
            countRequest(attempt.pending.request, "disconnected");
            dispatch();
          });
    } else if (attempt.state.compareAndSet(TIMED_OUT, RELEASED)) {
      execute(() -> attempt.lane.abandoned--);
    }
  }

  private void onTimeout(final Attempt<?> attempt) {
    if (!attempt.state.compareAndSet(PENDING, TIMED_OUT)) {
      return;
    }
    final PeerState state = attempt.peer;
    attempt.lane.inFlight--;
    attempt.lane.abandoned++;
    attempt.lane.shrink();
    state.record("timeout", 0, timeout.toMillis(), System.nanoTime());
    countRequest(attempt.pending.request, "timeout");
    attempt.pending.future.completeExceptionally(
        failure(TIMEOUT, state.peer, attempt.pending.request.name() + " timed out"));
    loop.schedule(() -> abandon(attempt), timeout.toMillis(), TimeUnit.MILLISECONDS);
    if (state.consecutiveFailures >= CONSECUTIVE_FAILURES_BEFORE_DISCONNECT) {
      LOG.debug("Disconnecting {} after repeated timeouts", state.peer.getLoggableId());
      state.peer.disconnect(DisconnectReason.TIMEOUT);
    }
    dispatch();
  }

  // a request still unanswered long after its timeout gives its slot back
  private void abandon(final Attempt<?> attempt) {
    if (attempt.state.compareAndSet(TIMED_OUT, RELEASED)) {
      attempt.lane.abandoned--;
      attempt.stream.close();
      dispatch();
    }
  }

  private void punish(
      final EthPeer peer,
      final SyncRequest<?> request,
      final String reason,
      final boolean disconnect) {
    LOG.debug("Invalid {} from {}: {}", request.name(), peer.getLoggableId(), reason);
    if (disconnect) {
      peer.disconnect(DisconnectReason.BREACH_OF_PROTOCOL_MALFORMED_MESSAGE_RECEIVED);
    } else {
      peer.recordUselessResponse(request.name());
    }
  }

  private void countRequest(final SyncRequest<?> request, final String outcome) {
    requestCounter.labels(request.name(), outcome).inc();
  }

  // a peer that announced a lower head may be syncing, or may just not have announced more
  private static boolean behind(final EthPeer peer, final long minimumHeight) {
    if (minimumHeight <= 0) {
      return false;
    }
    final long height = peer.chainState().getEstimatedHeight();
    return height > 0 && height < minimumHeight;
  }

  // range requests are the same in snap/1 and snap/2, so any snap peer serves them
  private static boolean speaks(final EthPeer peer, final Protocol protocol) {
    return protocol == Protocol.ETH
        || peer.getAgreedCapabilities().stream()
            .anyMatch(capability -> SnapProtocol.NAME.equals(capability.getName()));
  }

  private static String protocolName(final Protocol protocol) {
    return protocol == Protocol.SNAP ? SnapProtocol.NAME : EthProtocol.NAME;
  }

  private static RequestFailedException failure(
      final RequestFailedException.Reason reason, final EthPeer peer, final String message) {
    return new RequestFailedException(reason, peer, message);
  }

  private static final class Pending<T> {
    private final SyncRequest<T> request;
    private final Set<EthPeer> excluded;
    private final CompletableFuture<PeerResponse<T>> future;
    private long sequence;
    private long queuedAt;

    private Pending(final SyncRequest<T> request, final Set<EthPeer> excluded) {
      this(request, excluded, new CompletableFuture<>());
    }

    private Pending(
        final SyncRequest<T> request,
        final Set<EthPeer> excluded,
        final CompletableFuture<PeerResponse<T>> future) {
      this.request = request;
      this.excluded = excluded;
      this.future = future;
    }

    private Pending<T> excluding(final EthPeer peer) {
      final Set<EthPeer> more = new HashSet<>(excluded);
      more.add(peer);
      final Pending<T> copy = new Pending<>(request, more, future);
      copy.sequence = sequence;
      copy.queuedAt = queuedAt;
      return copy;
    }
  }

  private static final class Attempt<T> {
    private final Pending<T> pending;
    private final PeerState peer;
    private final Lane lane;
    private final ResponseStream stream;
    private final long startedAt;
    private final AtomicInteger state = new AtomicInteger(PENDING);
    private volatile ScheduledFuture<?> timeoutTask;

    private Attempt(
        final Pending<T> pending,
        final PeerState peer,
        final Lane lane,
        final ResponseStream stream,
        final long startedAt) {
      this.pending = pending;
      this.peer = peer;
      this.lane = lane;
      this.stream = stream;
      this.startedAt = startedAt;
    }
  }

  /** Requests of one protocol to one peer, with an adaptive limit. */
  private static final class Lane {
    private final int max;
    private double limit;
    private int inFlight;
    private int abandoned;

    private Lane(final int max) {
      this.max = Math.max(1, max);
      this.limit = Math.min(INITIAL_LANE_LIMIT, this.max);
    }

    private int slots() {
      return (int) limit;
    }

    private boolean hasRoom() {
      return inFlight + abandoned < slots();
    }

    // additive increase: about one more slot per window of answered requests
    private void grow() {
      limit = Math.min(max, limit + 1.0 / slots());
    }

    // multiplicative decrease on a timeout
    private void shrink() {
      limit = Math.max(1, limit / 2);
    }
  }

  /** Bookkeeping of one peer, owned by the loop thread. */
  private static final class PeerState {
    private final EthPeer peer;
    private final Lane snap;
    private final Lane eth;
    private int consecutiveFailures;
    private long backoffUntil;
    private double throughput = INITIAL_THROUGHPUT;

    private PeerState(final EthPeer peer, final int maxSnap, final int maxEth) {
      this.peer = peer;
      this.snap = new Lane(maxSnap);
      this.eth = new Lane(maxEth);
    }

    private Lane lane(final Protocol protocol) {
      return protocol == Protocol.SNAP ? snap : eth;
    }

    private void record(
        final String outcome, final int size, final long elapsedMillis, final long now) {
      switch (outcome) {
        case "success" -> {
          consecutiveFailures = 0;
          throughput = 0.8 * throughput + 0.2 * ((double) size / elapsedMillis);
        }
        case "empty" -> {
          throughput *= 0.9;
          backoff(now);
        }
        default -> {
          throughput *= 0.7;
          backoff(now);
        }
      }
    }

    private void backoff(final long now) {
      consecutiveFailures++;
      final long seconds =
          Math.min(MAX_BACKOFF_SECONDS, 1L << Math.min(consecutiveFailures - 1, 5));
      backoffUntil = now + TimeUnit.SECONDS.toNanos(seconds);
    }
  }
}
