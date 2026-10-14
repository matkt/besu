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

import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.EthProtocolVersion;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.MalformedRlpFromPeerException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerSelector;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskRequestSender;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockBodiesMessage;
import org.hyperledger.besu.ethereum.p2p.peers.PeerId;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * Runs peer tasks against a function answering their eth messages, as eth/68 peers would, without a
 * network, and selects among a few such peers. Stands in for the {@link PeerTaskExecutor} and the
 * {@link PeerSelector} of the node.
 */
public final class FakePeerTaskExecutor extends PeerTaskExecutor implements PeerSelector {

  private static final int PEERS = 8;

  private final List<EthPeer> peers =
      Stream.generate(
              () ->
                  FakeSnapNetwork.peer(SnapProtocol.SNAP2, EthProtocolVersion.V68.getCapability()))
          .limit(PEERS)
          .toList();
  private final Function<MessageData, MessageData> answers;
  private final AtomicLong requests = new AtomicLong();
  private final AtomicInteger inFlight = new AtomicInteger();
  private final AtomicInteger maxInFlight = new AtomicInteger();
  private final List<Hash> requestedBodies = new CopyOnWriteArrayList<>();
  private final List<EthPeer> peersUsed = new CopyOnWriteArrayList<>();
  private final Semaphore allowed = new Semaphore(0);
  private volatile boolean held;

  public FakePeerTaskExecutor(final Function<MessageData, MessageData> answers) {
    super(mock(PeerSelector.class), new PeerTaskRequestSender(), new NoOpMetricsSystem());
    this.answers = answers;
  }

  /** Holds every answer until {@link #releaseResponses()} or {@link #allow(int)}. */
  public void holdResponses() {
    held = true;
  }

  public void releaseResponses() {
    held = false;
    allowed.release(1 << 20);
  }

  /** Lets this many held requests be answered. */
  public void allow(final int responses) {
    allowed.release(responses);
  }

  public long requestCount() {
    return requests.get();
  }

  /** Returns the most requests that waited for an answer at the same time. */
  public int maxInFlight() {
    return maxInFlight.get();
  }

  /** Returns the hashes of the blocks whose bodies were asked, in request order. */
  public List<Hash> requestedBodies() {
    return requestedBodies;
  }

  /** Returns the peer each request went to, in request order. */
  public List<EthPeer> peersUsed() {
    return peersUsed;
  }

  @Override
  public Optional<EthPeer> getPeer(final Predicate<EthPeerImmutableAttributes> filter) {
    return peers.stream().filter(peer -> filter.test(attributes(peer))).findFirst();
  }

  @Override
  public CompletableFuture<EthPeer> waitForPeer(
      final Predicate<EthPeerImmutableAttributes> filter) {
    return getPeer(filter)
        .map(CompletableFuture::completedFuture)
        .orElseGet(CompletableFuture::new);
  }

  @Override
  public Optional<EthPeer> getPeerByPeerId(final PeerId peerId) {
    return Optional.empty();
  }

  @Override
  public <T> PeerTaskExecutorResult<T> execute(final PeerTask<T> task) {
    return getPeer(task.getPeerRequirementFilter())
        .map(peer -> executeAgainstPeer(task, peer))
        .orElseGet(
            () ->
                new PeerTaskExecutorResult<>(
                    Optional.empty(), PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE, List.of()));
  }

  @Override
  public <T> PeerTaskExecutorResult<T> executeAgainstPeer(
      final PeerTask<T> task, final EthPeer peer) {
    requests.incrementAndGet();
    maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
    peersUsed.add(peer);
    try {
      final Set<Capability> capabilities = peer.getAgreedCapabilities();
      final MessageData request = task.getRequestMessage(capabilities);
      if (request.getCode() == EthProtocolMessages.GET_BLOCK_BODIES) {
        GetBlockBodiesMessage.readFrom(request).hashes().forEach(requestedBodies::add);
      }
      // a held answer still arrives after an interruption, as a late one does over the network
      if (held) {
        allowed.acquireUninterruptibly();
      }
      final T result;
      try {
        result = task.processResponse(answers.apply(request), capabilities);
      } catch (final InvalidPeerTaskResponseException | MalformedRlpFromPeerException e) {
        return new PeerTaskExecutorResult<>(
            Optional.empty(), PeerTaskExecutorResponseCode.INVALID_RESPONSE, List.of(peer));
      }
      final boolean valid =
          task.validateResult(result) == PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD;
      return new PeerTaskExecutorResult<>(
          Optional.of(result),
          valid
              ? PeerTaskExecutorResponseCode.SUCCESS
              : PeerTaskExecutorResponseCode.INVALID_RESPONSE,
          List.of(peer));
    } finally {
      inFlight.decrementAndGet();
    }
  }

  // synced and validated peers, as the tasks require
  private static EthPeerImmutableAttributes attributes(final EthPeer peer) {
    return new EthPeerImmutableAttributes(
        UInt256.ZERO, true, Long.MAX_VALUE / 2, 0, 0, 0, false, true, true, true, false, peer);
  }
}
