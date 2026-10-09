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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class AbstractRetryingSnapPeerTaskTest {

  private final EthPeers ethPeers = mock(EthPeers.class);

  @Test
  void triesSnapPeersFromLeastToMostBusyWhateverTheirChainHeight() {
    final EthPeerImmutableAttributes highestBusy = peer(1_000L, 3, 10L, true, true);
    final EthPeerImmutableAttributes lowestIdle = peer(10L, 0, 30L, true, true);
    final EthPeerImmutableAttributes idleUsedEarlier = peer(500L, 0, 20L, true, true);
    final EthPeerImmutableAttributes lessBusy = peer(800L, 1, 40L, true, true);
    final EthPeerImmutableAttributes notValidated = peer(900L, 0, 0L, false, true);
    when(ethPeers.streamAvailablePeers())
        .thenAnswer(
            invocation ->
                Stream.of(highestBusy, lowestIdle, idleUsedEarlier, lessBusy, notValidated));

    assertThat(accountRangeTask().peersInSelectionOrder())
        .containsExactly(idleUsedEarlier, lowestIdle, lessBusy, highestBusy);
  }

  @Test
  void onlySnapServersAreSuitable() {
    final AbstractRetryingSnapPeerTask<?> task = accountRangeTask();

    assertThat(task.isSuitablePeer(peer(1L, 0, 0L, true, true))).isTrue();
    assertThat(task.isSuitablePeer(peer(1L, 0, 0L, true, false))).isFalse();
  }

  private AbstractRetryingSnapPeerTask<?> accountRangeTask() {
    final EthContext ethContext = mock(EthContext.class);
    when(ethContext.getEthPeers()).thenReturn(ethPeers);
    return (AbstractRetryingSnapPeerTask<?>)
        RetryingGetAccountRangeFromPeerTask.forAccountRange(
            ethContext,
            Bytes32.ZERO,
            Bytes32.ZERO,
            new BlockHeaderTestFixture().buildHeader(),
            new NoOpMetricsSystem());
  }

  private static EthPeerImmutableAttributes peer(
      final long chainHeight,
      final int outstandingRequests,
      final long lastRequestTimestamp,
      final boolean fullyValidated,
      final boolean servingSnap) {
    return new EthPeerImmutableAttributes(
        UInt256.ZERO,
        true,
        chainHeight,
        100,
        outstandingRequests,
        lastRequestTimestamp,
        false,
        fullyValidated,
        servingSnap,
        true,
        false,
        mock(EthPeer.class));
  }
}
