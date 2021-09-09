/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.manager;

import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.peervalidation.PeerValidator;
import org.hyperledger.besu.ethereum.p2p.network.ProtocolManager;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SnapProtocolManager implements ProtocolManager {
  private static final Logger LOG = LogManager.getLogger();

  private final List<PeerValidator> peerValidators;
  private final List<Capability> supportedCapabilities;
  private final EthPeers ethPeers;
  private final ProtocolMessages snapMessages;

  public SnapProtocolManager(
      final List<PeerValidator> peerValidators,
      final EthPeers ethPeers,
      final ProtocolMessages snapMessages) {
    this.peerValidators = peerValidators;
    this.ethPeers = ethPeers;
    this.snapMessages = snapMessages;
    this.supportedCapabilities = calculateCapabilities();
  }

  private List<Capability> calculateCapabilities() {
    final ImmutableList.Builder<Capability> capabilities = ImmutableList.builder();
    capabilities.add(SnapProtocol.SNAP1);

    return capabilities.build();
  }

  @Override
  public String getSupportedProtocol() {
    return SnapProtocol.NAME;
  }

  @Override
  public List<Capability> getSupportedCapabilities() {
    return supportedCapabilities;
  }

  @Override
  public void stop() {}

  @Override
  public void awaitStop() throws InterruptedException {}

  /**
   * This function is called by the P2P framework when an "IBF" message has been received. This
   * function is responsible for:
   *
   * <ul>
   *   <li>Determining if the message was from a current validator (discard if not)
   *   <li>Determining if the message received was for the 'current round', discarding if old and
   *       buffering for the future if ahead of current state.
   *   <li>If the received message is otherwise valid, it is sent to the state machine which is
   *       responsible for determining how to handle the message given its internal state.
   * </ul>
   *
   * @param cap The capability under which the message was transmitted.
   * @param message The message to be decoded.
   */
  @Override
  public void processMessage(final Capability cap, final Message message) {
    final MessageData messageData = message.getData();
    final int code = messageData.getCode();
    LOG.trace("Process snap message {}, {}", cap, code);
    final EthPeer ethPeer = ethPeers.peer(message.getConnection());
    if (ethPeer == null) {
      LOG.debug(
          "Ignoring message received from unknown peer connection: " + message.getConnection());
      return;
    }
    final EthMessage ethMessage = new EthMessage(ethPeer, messageData);
    if (!ethPeer.validateReceivedMessage(ethMessage, getSupportedProtocol())) {
      LOG.debug("Unsolicited message received from, disconnecting: {}", ethPeer);
      ethPeer.disconnect(DisconnectReason.BREACH_OF_PROTOCOL);
      return;
    }

    // This will handle responses
    ethPeers.dispatchMessage(ethPeer, ethMessage, getSupportedProtocol());

    // This will handle requests
    Optional<MessageData> maybeResponseData = Optional.empty();
    try {
      final Map.Entry<BigInteger, MessageData> requestIdAndEthMessage =
          RequestId.unwrapSnapMessageData(ethMessage.getData());
      maybeResponseData =
          snapMessages
              .dispatch(new EthMessage(ethPeer, requestIdAndEthMessage.getValue()))
              .map(
                  responseData ->
                      RequestId.wrapMessageData(requestIdAndEthMessage.getKey(), responseData));
    } catch (final RLPException e) {
      LOG.debug(
          "Received malformed message {} , disconnecting: {}", messageData.getData(), ethPeer, e);
      ethPeer.disconnect(DisconnectMessage.DisconnectReason.BREACH_OF_PROTOCOL);
    }
    maybeResponseData.ifPresent(
        responseData -> {
          try {
            ethPeer.send(responseData);
          } catch (final PeerConnection.PeerNotConnected __) {
            // Peer disconnected before we could respond - nothing to do
          }
        });
  }

  @Override
  public void handleNewConnection(final PeerConnection connection) {
    ethPeers.registerConnection(connection, peerValidators);
    final EthPeer peer = ethPeers.peer(connection);
    if (peer.statusHasBeenSentToPeer()) {
      return;
    }
  }

  @Override
  public void handleDisconnect(
      final PeerConnection peerConnection,
      final DisconnectReason disconnectReason,
      final boolean initiatedByPeer) {
    System.out.println("handleDisconnect");
  }
}
