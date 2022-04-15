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
package org.hyperledger.besu.ethereum.eth.manager.task;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.LightBlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.BlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.messages.EthPV62;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Retrieves a sequence of headers from a peer. */
public abstract class AbstractGetHeadersFromPeerTaskV2
    extends AbstractPeerRequestTask<List<BlockHeader>> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractGetHeadersFromPeerTaskV2.class);

  protected final int count;
  protected final int skip;
  protected final boolean reverse;

  protected AbstractGetHeadersFromPeerTaskV2(
      final EthContext ethContext,
      final int count,
      final int skip,
      final boolean reverse,
      final MetricsSystem metricsSystem) {
    super(ethContext, EthPV62.GET_BLOCK_HEADERS, metricsSystem);
    checkArgument(count > 0);
    this.count = count;
    this.skip = skip;
    this.reverse = reverse;
  }

  @Override
  protected Optional<List<BlockHeader>> processResponse(
      final boolean streamClosed, final MessageData message, final EthPeer peer) {
    if (streamClosed) {
      // All outstanding requests have been responded to and we still haven't found the response
      // we wanted. It must have been empty or contain data that didn't match.
      peer.recordUselessResponse("headers");
      return Optional.of(Collections.emptyList());
    }

    final BlockHeadersMessage headersMessage = BlockHeadersMessage.readFrom(message);
    final List<LightBlockHeader> headers = headersMessage.getLightHeaders();
    if (headers.isEmpty()) {
      // Message contains no data - nothing to do
      LOG.debug("headers.isEmpty. Peer: {}", peer);
      return Optional.empty();
    }
    if (headers.size() > count) {
      // Too many headers - this isn't our response
      LOG.debug("headers.size()>count. Peer: {}", peer);
      return Optional.empty();
    }

    final LightBlockHeader firstHeader = headers.get(0);
    if (!matchesFirstHeader(firstHeader)) {
      // This isn't our message - nothing to do
      LOG.debug("!matchesFirstHeader. Peer: {}", peer);
      return Optional.empty();
    }

    final List<BlockHeader> headersList = new ArrayList<>();
    headersList.add(firstHeader);
    LightBlockHeader prevBlockHeader = firstHeader;
    final int expectedDelta = reverse ? -(skip + 1) : (skip + 1);
    for (int i = 1; i < headers.size(); i++) {
      final LightBlockHeader header = headers.get(i);
      if (header.getNumber() != prevBlockHeader.getNumber() + expectedDelta) {
        // Skip doesn't match, this isn't our data
        LOG.debug("header not matching the expected number. Peer: {}", peer);
        return Optional.empty();
      }
      // if headers are supposed to be sequential check if a chain is formed
      if (Math.abs(expectedDelta) == 1) {
        final LightBlockHeader parent = reverse ? header : prevBlockHeader;
        final LightBlockHeader child = reverse ? prevBlockHeader : header;
        if (!parent.getHash().equals(child.getParentHash())) {
          LOG.debug(
              "Sequential headers must form a chain through hashes, disconnecting peer: {}", peer);
          peer.disconnect(DisconnectMessage.DisconnectReason.BREACH_OF_PROTOCOL);
          return Optional.empty();
        }
      }
      prevBlockHeader = header;
      headersList.add(header);
    }

    LOG.debug("Received {} of {} headers requested from peer {}", headersList.size(), count, peer);
    return Optional.of(headersList);
  }

  protected abstract boolean matchesFirstHeader(LightBlockHeader firstHeader);
}
