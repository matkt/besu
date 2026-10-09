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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.manager.task.AbstractRetryingSwitchingPeerTask;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.function.Predicate;
import java.util.stream.Stream;

/** Retrying snap world state request, sent to the least busy snap peer. */
public abstract class AbstractRetryingSnapPeerTask<T> extends AbstractRetryingSwitchingPeerTask<T> {

  protected AbstractRetryingSnapPeerTask(
      final EthContext ethContext,
      final MetricsSystem metricsSystem,
      final Predicate<T> isEmptyResponse,
      final int maxRetries) {
    super(ethContext, metricsSystem, isEmptyResponse, maxRetries);
  }

  // Snap peers serve the same recent state, so spread the requests by load: ordering by chain
  // height sends them all to the same peer when the peers are at the same height.
  @Override
  protected Stream<EthPeerImmutableAttributes> peersInSelectionOrder() {
    return getEthContext()
        .getEthPeers()
        .streamAvailablePeers()
        .filter(EthPeerImmutableAttributes::isFullyValidated)
        .sorted(EthPeers.LEAST_TO_MOST_BUSY);
  }

  @Override
  protected boolean isSuitablePeer(final EthPeerImmutableAttributes peer) {
    return peer.isServingSnap();
  }
}
