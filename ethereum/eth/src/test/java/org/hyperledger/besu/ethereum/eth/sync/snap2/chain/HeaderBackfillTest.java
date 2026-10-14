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
import static org.awaitility.Awaitility.await;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.sync.common.WrongChainException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.FakeSnapNetwork;
import org.hyperledger.besu.ethereum.eth.sync.snap2.TestChain;
import org.hyperledger.besu.ethereum.eth.sync.snap2.TestWorld;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class HeaderBackfillTest {

  private final Random random = new Random(19);
  private final FakeSnapNetwork network = new FakeSnapNetwork();
  private final TestChain chain =
      new TestChain(random, TestWorld.random(random, 20, 0, 0), network, 1, 1);
  private final ExecutorService executor = Executors.newFixedThreadPool(4);
  private MutableBlockchain blockchain;

  @AfterEach
  void tearDown() {
    network.stop();
    executor.shutdownNow();
    assertThat(network.unagreedRequests()).isEmpty();
  }

  @Test
  void storesTheHeadersDownToTheFloor() {
    chain.extend(100);
    final HeaderBackfill backfill = backfill(chain.header(0), chain.header(100));

    backfill.run(executor).join();

    assertThat(backfill.lowestStored()).isEqualTo(1);
    for (long number = 1; number < 100; number++) {
      assertThat(blockchain.getBlockHeader(number)).contains(chain.header(number));
    }
  }

  @Test
  void followsTheTrustedHeaderWhenPeersServeAnotherChain() {
    chain.extend(60);
    final List<BlockHeader> trusted = chain.headers(1, 60);
    // the peers moved to a fork from block 31, which they serve by number
    chain.reorg(30, 30);
    final HeaderBackfill backfill = backfill(chain.header(0), trusted.getLast());

    backfill.run(executor).join();

    for (final BlockHeader header : trusted.subList(0, trusted.size() - 1)) {
      assertThat(blockchain.getBlockHeader(header.getNumber())).contains(header);
    }
  }

  @Test
  void failsWhenTheHeadersDoNotLeadToTheFloor() {
    chain.extend(20);
    final BlockHeader otherFloor = new BlockHeaderTestFixture().number(5).buildHeader();
    final HeaderBackfill backfill = backfill(otherFloor, chain.header(20));

    assertThatThrownBy(() -> backfill.run(executor).join())
        .hasCauseInstanceOf(WrongChainException.class)
        .hasMessageContaining("does not lead to block 5");
  }

  @Test
  void storesNothingOnceStopped() {
    chain.extend(100);
    final HeaderBackfill backfill = backfill(chain.header(0), chain.header(100));
    network.holdResponses();

    final CompletableFuture<Void> done = backfill.run(executor);
    await().atMost(Duration.ofSeconds(10)).until(() -> network.requestCount() > 0);
    backfill.stop();
    network.releaseResponses();

    await().atMost(Duration.ofSeconds(10)).until(done::isDone);
    for (long number = 1; number < 100; number++) {
      assertThat(blockchain.getBlockHeader(number)).isEmpty();
    }
  }

  // the top is stored and trusted, as the pivot is
  private HeaderBackfill backfill(final BlockHeader floor, final BlockHeader top) {
    blockchain = InMemoryKeyValueStorageProvider.createInMemoryBlockchain(chain.genesisBlock());
    blockchain.storeBlockHeaders(List.of(top));
    return new HeaderBackfill(
        network,
        ProtocolScheduleFixture.MAINNET,
        blockchain::storeBlockHeaders,
        new HeaderFetcher(network, ProtocolScheduleFixture.MAINNET, chain, 10),
        floor,
        top,
        10,
        3,
        new NoOpMetricsSystem());
  }
}
