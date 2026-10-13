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
import static org.awaitility.Awaitility.await;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.FakeSnapNetwork;
import org.hyperledger.besu.ethereum.eth.sync.snap2.TestChain;
import org.hyperledger.besu.ethereum.eth.sync.snap2.TestWorld;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class BodyDownloaderTest {

  private final Random random = new Random(11);
  private final FakeSnapNetwork network = new FakeSnapNetwork();
  private final TestChain chain =
      new TestChain(random, TestWorld.random(random, 20, 0, 0), network, 1, 1);
  private final ExecutorService writer = Executors.newFixedThreadPool(4);
  private MutableBlockchain blockchain;

  @AfterEach
  void tearDown() {
    network.stop();
    writer.shutdownNow();
  }

  @Test
  void storesTheBodiesOfTheBlocksAboveTheFloor() {
    chain.extend(50);
    final BodyDownloader bodies = downloader(50, 10);

    bodies.download(1, 50);

    awaitStoredUpTo(bodies, 50);
    assertThat(bodies.storedBlocks()).isEqualTo(40);
    for (long number = 1; number <= 50; number++) {
      assertThat(blockchain.getBlockBody(chain.header(number).getHash()).isPresent())
          .isEqualTo(number > 10);
    }
  }

  @Test
  void downloadsAgainARangeWhoseHeadersAReorgReplacedWhileInFlight() {
    chain.extend(40);
    network.latency(300);
    final BodyDownloader bodies = downloader(40, 0);

    bodies.download(1, 40);
    // the two highest ranges are in flight with the headers of the first fork
    await().atMost(Duration.ofSeconds(10)).until(() -> network.requestCount() >= 2);
    chain.reorg(30, 10);
    blockchain.storeBlockHeaders(headers(31, 40));

    awaitStoredUpTo(bodies, 40);
    assertCanonicalBodies(40);
  }

  @Test
  void downloadsAgainTheBlocksARewindForgets() {
    chain.extend(40);
    final BodyDownloader bodies = downloader(40, 0);
    bodies.download(1, 40);
    awaitStoredUpTo(bodies, 40);

    chain.reorg(30, 10);
    blockchain.storeBlockHeaders(headers(31, 40));
    bodies.rewind(31);

    assertThat(bodies.storedUpTo(40)).isFalse();
    bodies.download(1, 40);
    awaitStoredUpTo(bodies, 40);
    assertCanonicalBodies(40);
  }

  @Test
  void asksLargerRangesWhenTheBlocksAreSmall() {
    chain.extend(1000);
    final BodyDownloader bodies = downloader(1000, 0, 128, 2);

    bodies.download(1, 1000);

    awaitStoredUpTo(bodies, 1000);
    // the empty bodies let the ranges grow past the 32 blocks of the first ones
    assertThat(network.requestCount()).isLessThan(1000 / 32);
  }

  // stores the headers up to the head and downloads with ranges of 5 blocks, 2 at a time
  private BodyDownloader downloader(final long head, final long floor) {
    return downloader(head, floor, 5, 2);
  }

  private BodyDownloader downloader(
      final long head, final long floor, final int maxRangeBlocks, final int maxRanges) {
    blockchain = InMemoryKeyValueStorageProvider.createInMemoryBlockchain(chain.genesisBlock());
    blockchain.storeBlockHeaders(headers(1, head));
    return new BodyDownloader(
        network,
        ProtocolScheduleFixture.MAINNET,
        blockchain,
        floor,
        maxRangeBlocks,
        maxRanges,
        false,
        writer);
  }

  private List<BlockHeader> headers(final long from, final long to) {
    final List<BlockHeader> headers = new ArrayList<>();
    for (long number = from; number <= to; number++) {
      headers.add(chain.header(number));
    }
    return headers;
  }

  private void assertCanonicalBodies(final long head) {
    for (long number = 1; number <= head; number++) {
      assertThat(blockchain.getBlockBody(chain.header(number).getHash())).isPresent();
    }
  }

  private static void awaitStoredUpTo(final BodyDownloader bodies, final long target) {
    await().atMost(Duration.ofSeconds(30)).until(() -> bodies.storedUpTo(target));
  }
}
