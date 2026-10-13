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
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.google.common.util.concurrent.Uninterruptibles;

/**
 * Runs peer tasks against a function answering their eth messages, as an eth/68 peer would, without
 * a network. Stands in for the {@link PeerTaskExecutor} of the node.
 */
public final class FakePeerTaskExecutor extends PeerTaskExecutor {

  private final EthPeer peer =
      FakeSnapNetwork.peer(SnapProtocol.SNAP2, EthProtocolVersion.V68.getCapability());
  private final Function<MessageData, MessageData> answers;
  private final AtomicLong requests = new AtomicLong();
  private final List<Hash> requestedBodies = new CopyOnWriteArrayList<>();
  private volatile CountDownLatch held = new CountDownLatch(0);

  public FakePeerTaskExecutor(final Function<MessageData, MessageData> answers) {
    super(mock(PeerSelector.class), new PeerTaskRequestSender(), new NoOpMetricsSystem());
    this.answers = answers;
  }

  /** Holds every answer until {@link #releaseResponses()}. */
  public void holdResponses() {
    held = new CountDownLatch(1);
  }

  public void releaseResponses() {
    held.countDown();
  }

  public long requestCount() {
    return requests.get();
  }

  /** Returns the hashes of the blocks whose bodies were asked, in request order. */
  public List<Hash> requestedBodies() {
    return requestedBodies;
  }

  @Override
  public <T> PeerTaskExecutorResult<T> execute(final PeerTask<T> task) {
    requests.incrementAndGet();
    final Set<Capability> capabilities = peer.getAgreedCapabilities();
    final MessageData request = task.getRequestMessage(capabilities);
    if (request.getCode() == EthProtocolMessages.GET_BLOCK_BODIES) {
      GetBlockBodiesMessage.readFrom(request).hashes().forEach(requestedBodies::add);
    }
    // a held answer still arrives after an interruption, as a late one does over the network
    Uninterruptibles.awaitUninterruptibly(held);
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
  }
}
