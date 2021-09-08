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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.PendingPeerRequest;
import org.hyperledger.besu.ethereum.eth.messages.SnapV1;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyMap;

public class GetAccountRangeFromPeerTask extends AbstractPeerRequestTask<Bytes> {

  private static final Logger LOG = LogManager.getLogger();

  private final Hash rootHash;
  private final Hash startingHash;
  private final Hash endingHash;
  private final long minimumBlockNumber;

  private GetAccountRangeFromPeerTask(
      final EthContext ethContext,
      final Hash rootHash,
              final Hash startingHash,
              final Hash endingHash,
      final long minimumBlockNumber,
      final MetricsSystem metricsSystem) {
    super(ethContext, SnapV1.GET_ACCOUNT_RANGE, metricsSystem);
    this.rootHash = rootHash;
    this.startingHash = startingHash;
    this.endingHash = endingHash;
    this.minimumBlockNumber = minimumBlockNumber;
  }

  public static GetAccountRangeFromPeerTask forAccountRange(
      final EthContext ethContext,
      final Hash rootHash,
      final Hash startingHash,
      final Hash endingHash,
      final long minimumBlockNumber,
      final MetricsSystem metricsSystem) {
    return new GetAccountRangeFromPeerTask(ethContext, rootHash, startingHash, endingHash, minimumBlockNumber, metricsSystem);
  }

  @Override
  protected PendingPeerRequest sendRequest() {
    return sendRequestToPeer(
        peer -> {
          LOG.debug("Requesting on state root {} account range [{},{}] entries from peer {}.", rootHash, startingHash, endingHash, peer);
          return peer.getAccountRange(System.currentTimeMillis(), rootHash, startingHash, endingHash);
        },
            minimumBlockNumber);
  }

  @Override
  protected Optional<Bytes> processResponse(
      final boolean streamClosed, final MessageData message, final EthPeer peer) {
    if (streamClosed) {
      // We don't record this as a useless response because it's impossible to know if a peer has
      // the data we're requesting.
      return Optional.empty();
    }
    System.out.println("response received");
    return Optional.empty();
  }

}
