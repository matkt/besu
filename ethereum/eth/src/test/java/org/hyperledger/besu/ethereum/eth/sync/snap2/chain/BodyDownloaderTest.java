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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.FakeSnapNetwork;
import org.hyperledger.besu.ethereum.eth.sync.snap2.TestChain;
import org.hyperledger.besu.ethereum.eth.sync.snap2.TestWorld;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
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
  private final ExecutorService executor = Executors.newFixedThreadPool(4);
  // one thread, so that a store in progress holds back every other store
  private final ExecutorService writer = Executors.newSingleThreadExecutor();
  private MutableBlockchain blockchain;

  @AfterEach
  void tearDown() {
    network.stop();
    executor.shutdownNow();
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
    network.holdResponses();
    final BodyDownloader bodies = downloader(40, 0);

    bodies.download(1, 40);
    // the two highest ranges are in flight with the headers of the first fork
    await().atMost(Duration.ofSeconds(10)).until(() -> network.requestCount() >= 2);
    chain.reorg(30, 10);
    blockchain.storeBlockHeaders(chain.headers(31, 40));
    network.releaseResponses();

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
    blockchain.storeBlockHeaders(chain.headers(31, 40));
    bodies.rewind(31);

    assertThat(bodies.storedUpTo(40)).isFalse();
    bodies.download(1, 40);
    awaitStoredUpTo(bodies, 40);
    assertCanonicalBodies(40);
  }

  @Test
  void keepsDownloadingWhileEarlierRangesAreStored() {
    chain.extend(40);
    final CountDownLatch storesHeld = new CountDownLatch(1);
    blockchain = spy(blockchainWithHeaders(40));
    doAnswer(
            invocation -> {
              storesHeld.await();
              return invocation.callRealMethod();
            })
        .when(blockchain)
        .unsafeStoreSyncBodiesAndReceipts(any(), anyBoolean());
    final BodyDownloader bodies =
        new BodyDownloader(
            network, ProtocolScheduleFixture.MAINNET, blockchain, 0, 5, 2, false, executor, writer);

    bodies.download(1, 40);

    // the two ranges allowed on the network move on while the first ones wait for the writer
    await().atMost(Duration.ofSeconds(10)).until(() -> network.requestCount() >= 4);
    storesHeld.countDown();
    awaitStoredUpTo(bodies, 40);
    assertCanonicalBodies(40);
  }

  @Test
  void checksThatTheStoredBlocksLinkUpToTheHead() {
    chain.extend(50);
    final BodyDownloader bodies = downloader(50, 10);
    bodies.download(1, 50);
    awaitStoredUpTo(bodies, 50);

    bodies.verifyChain(chain.header(10), chain.header(50));
    assertThatThrownBy(() -> bodies.verifyChain(chain.header(10), chain.header(49)))
        .hasMessageContaining("do not lead to block 49");
    // the blocks a reorg forgot break the chain until downloaded again
    bodies.rewind(31);
    assertThatThrownBy(() -> bodies.verifyChain(chain.header(10), chain.header(50)))
        .isInstanceOf(IllegalStateException.class);
    bodies.download(1, 50);
    awaitStoredUpTo(bodies, 50);
    bodies.verifyChain(chain.header(10), chain.header(50));
  }

  @Test
  void asksLargerRangesWhenTheBlocksAreSmall() {
    chain.extend(1000);
    final BodyDownloader bodies = downloader(1000, 0, 128, 2);

    bodies.download(1, 1000);

    awaitStoredUpTo(bodies, 1000);
    // the empty bodies let the ranges grow from the 32 blocks of the first ones to 128
    assertThat(network.requestCount()).isLessThanOrEqualTo(1000 / 64);
  }

  // stores the headers up to the head and downloads with ranges of 5 blocks, 2 at a time
  private BodyDownloader downloader(final long head, final long floor) {
    return downloader(head, floor, 5, 2);
  }

  private BodyDownloader downloader(
      final long head, final long floor, final int maxRangeBlocks, final int maxRanges) {
    blockchain = blockchainWithHeaders(head);
    return new BodyDownloader(
        network,
        ProtocolScheduleFixture.MAINNET,
        blockchain,
        floor,
        maxRangeBlocks,
        maxRanges,
        false,
        executor,
        writer);
  }

  private MutableBlockchain blockchainWithHeaders(final long head) {
    final MutableBlockchain withHeaders =
        InMemoryKeyValueStorageProvider.createInMemoryBlockchain(chain.genesisBlock());
    withHeaders.storeBlockHeaders(chain.headers(1, head));
    return withHeaders;
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
