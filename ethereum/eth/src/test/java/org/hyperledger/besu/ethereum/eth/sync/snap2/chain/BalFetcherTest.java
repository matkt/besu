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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.FakeSnapNetwork.peer;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.EthProtocolVersion;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.BlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.FakeSnapNetwork;
import org.hyperledger.besu.ethereum.eth.sync.snap2.TestChain;
import org.hyperledger.besu.ethereum.eth.sync.snap2.TestWorld;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class BalFetcherTest {

  // peers of the fake network: snap/2 with eth/71, snap/1 with eth/71, snap/2 with eth/68
  private static final int SNAP2_ETH71 = 0;
  private static final int ETH71_ONLY = 1;
  private static final int SNAP2_ETH68 = 2;

  private final Random random = new Random(5);
  private final FakeSnapNetwork network = new FakeSnapNetwork();
  private final TestChain chain =
      new TestChain(random, TestWorld.random(random, 50, 0, 0), network, 1, 3);
  private final BalFetcher fetcher = new BalFetcher(network, 4, 1 << 20, 5);

  @AfterEach
  void tearDown() {
    fetcher.stop();
    network.stop();
    assertThat(network.unagreedRequests()).isEmpty();
  }

  @Test
  void fetchesTheListsFromSnap2Peers() {
    chain.extend(12);
    network.serveNoBlockAccessLists(ETH71_ONLY);

    final List<BlockHeader> headers = headers(12);

    assertMatch(headers, fetcher.fetch(headers).join());
  }

  @Test
  void fetchesTheListsFromEth71Peers() {
    chain.extend(12);
    network.serveNoBlockAccessLists(SNAP2_ETH71);
    network.serveNoBlockAccessLists(SNAP2_ETH68);

    final List<BlockHeader> headers = headers(12);

    assertMatch(headers, fetcher.fetch(headers).join());
  }

  @Test
  void givesUpWhenNoPeerHasTheLists() {
    chain.extend(4);
    network.serveNoBlockAccessLists(SNAP2_ETH71);
    network.serveNoBlockAccessLists(ETH71_ONLY);
    network.serveNoBlockAccessLists(SNAP2_ETH68);

    assertThatThrownBy(() -> fetcher.fetch(headers(4), 1).join())
        .rootCause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("no peer served the block access list");
  }

  @Test
  void rejectsAListThatDoesNotMatchItsHeader() {
    final TestWorld world = TestWorld.random(random, 20, 0, 0);
    final Bytes expected = world.mutate(random, 2).encode();
    final BlockAccessList other = world.mutate(random, 2);
    final BlockHeader header =
        new BlockHeaderTestFixture().number(1).balHash(Hash.hash(expected)).buildHeader();
    final BalFetcher.BalRequest request = new BalFetcher.BalRequest(List.of(header), 1 << 20);
    final EthPeer peer = peer(SnapProtocol.SNAP1, EthProtocolVersion.V71.getCapability());

    assertThat(
            request.decode(
                BlockAccessListsMessage.create(
                    List.of(Optional.of(BlockAccessList.fromBytes(expected)))),
                peer))
        .containsEntry(header.getHash(), expected);
    assertThatThrownBy(
            () -> request.decode(BlockAccessListsMessage.create(List.of(Optional.of(other))), peer))
        .isInstanceOf(SyncRequest.InvalidResponseException.class)
        .hasMessageContaining("does not match the header of block 1");
  }

  @Test
  void asksOnlyPeersWithSnap2OrEth71() {
    final BalFetcher.BalRequest request =
        new BalFetcher.BalRequest(
            List.of(new BlockHeaderTestFixture().number(1).buildHeader()), 1024);

    assertThat(request.canServe(peer(SnapProtocol.SNAP2, EthProtocolVersion.V68.getCapability())))
        .isTrue();
    assertThat(request.canServe(peer(SnapProtocol.SNAP1, EthProtocolVersion.V71.getCapability())))
        .isTrue();
    assertThat(request.canServe(peer(SnapProtocol.SNAP1, EthProtocolVersion.V68.getCapability())))
        .isFalse();
  }

  private List<BlockHeader> headers(final int count) {
    final List<BlockHeader> headers = new ArrayList<>();
    for (int number = 1; number <= count; number++) {
      headers.add(chain.header(number));
    }
    return headers;
  }

  private static void assertMatch(final List<BlockHeader> headers, final List<Bytes> lists) {
    assertThat(lists).hasSameSizeAs(headers);
    for (int i = 0; i < headers.size(); i++) {
      assertThat(Optional.of(Hash.hash(lists.get(i)))).isEqualTo(headers.get(i).getBalHash());
    }
  }
}
