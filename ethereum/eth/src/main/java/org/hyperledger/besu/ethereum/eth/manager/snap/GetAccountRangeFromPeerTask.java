/*
 * Copyright contributors to Hyperledger Besu
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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import java.util.Optional;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.PendingPeerRequest;
import org.hyperledger.besu.ethereum.eth.manager.task.AbstractPeerRequestTask;
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetAccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV1;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class GetAccountRangeFromPeerTask extends AbstractPeerRequestTask<AccountRangeMessage> {

  private static final Logger LOG = getLogger(GetAccountRangeFromPeerTask.class);


  private final GetAccountRangeMessage message;
  private final BlockHeader blockHeader;

  private GetAccountRangeFromPeerTask(
          final EthContext ethContext,
          final GetAccountRangeMessage message,
          final BlockHeader blockHeader,
          final MetricsSystem metricsSystem) {
    super(ethContext, SnapV1.ACCOUNT_RANGE, metricsSystem);
    this.message = message;
    this.blockHeader = blockHeader;
  }

  public static GetAccountRangeFromPeerTask forAccountRange(
      final EthContext ethContext,
      final GetAccountRangeMessage message,
      final BlockHeader blockHeader,
      final MetricsSystem metricsSystem) {
    return new GetAccountRangeFromPeerTask(ethContext, message, blockHeader, metricsSystem);
  }

  @Override
  protected PendingPeerRequest sendRequest() {
    return sendRequestToPeer(
        peer -> {
          GetAccountRangeMessage.Range range = message.range(false);
          LOG.trace(
              "Requesting account range [{} ,{}] for state root {} from {} .",
              range.startKeyHash(),
              range.endKeyHash(),
              blockHeader.getStateRoot(),
              peer);
          message.setOverrideStateRoot(Optional.of(blockHeader.getStateRoot()));
          return peer.getSnapAccountRange(message);
        },
        blockHeader.getNumber());
  }

  @Override
  protected Optional<AccountRangeMessage> processResponse(
      final boolean streamClosed, final MessageData message, final EthPeer peer) {
    if (streamClosed) {
      // We don't record this as a useless response because it's impossible to know if a peer has
      // the data we're requesting.
      return Optional.empty();
    }
    return Optional.of(AccountRangeMessage.readFrom(message));
  }
}
