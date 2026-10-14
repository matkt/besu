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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator.BlockOptions;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.eth.messages.BlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetReceiptsMessage;
import org.hyperledger.besu.ethereum.eth.messages.ReceiptsMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.FakePeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BodyBackfill.Settings;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BodyBackfill.State;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongUnaryOperator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Timeout(60)
class BodyBackfillTest {

  private static final int HEAD = 60;
  private static final long NO_LIMIT = 0;
  private static final long SLOT_SECONDS = 12;

  private final BlockDataGenerator gen = new BlockDataGenerator(5);
  private final List<Block> chain = new ArrayList<>();
  private final Map<Hash, Block> blocksByHash = new HashMap<>();
  private final Map<Hash, List<TransactionReceipt>> receiptsByHash = new HashMap<>();
  private final FakePeerTaskExecutor executor = new FakePeerTaskExecutor(this::answer);
  private final AtomicBoolean inSync = new AtomicBoolean(true);
  private final SettableClock clock = new SettableClock();
  private MutableBlockchain blockchain;
  private BodyBackfill backfill;

  @AfterEach
  void tearDown() {
    if (backfill != null) {
      backfill.stop();
    }
  }

  @Test
  void storesTheMissingBodiesAndReceiptsHighestFirstAndCompletes() {
    blockchain = chain(51);
    backfill = backfill(settings(NO_LIMIT, Duration.ZERO), head -> 1);

    backfill.start();

    await().atMost(Duration.ofSeconds(30)).until(() -> backfill.state() == State.DONE);
    assertBodiesStored(1, HEAD);
    assertThat(blockchain.getBodiesFrom()).contains(1L);
    assertThat(backfill.lowestStored()).isEqualTo(1);
    assertThat(backfill.blocksRemaining()).isZero();
    assertThat(backfill.blocksStored()).isEqualTo(50);
    assertThat(backfill.done()).isCompleted();
    // the first request asks one of the two highest missing ranges
    assertThat(number(executor.requestedBodies().getFirst())).isGreaterThanOrEqualTo(41);
  }

  @Test
  void honoursTheRateLimit() {
    blockchain = chain(51);
    final long bytesPerSecond = 25_000;
    backfill = backfill(settings(bytesPerSecond, Duration.ZERO), head -> 1);
    final long start = System.nanoTime();

    backfill.start();

    await().atMost(Duration.ofSeconds(30)).until(() -> backfill.state() == State.DONE);
    final double seconds = (System.nanoTime() - start) / 1e9;
    assertThat(backfill.bytesStored()).isGreaterThan(5 * bytesPerSecond / 2);
    assertThat(seconds).isGreaterThan(0.8 * backfill.bytesStored() / bytesPerSecond);
  }

  @ParameterizedTest
  @EnumSource(BackfillMode.class)
  void eachModeCapsItsRateAndItsRequestsInFlight(final BackfillMode mode) throws Exception {
    blockchain = chain(51);
    executor.holdResponses();
    backfill = backfill(settings(mode), head -> 1);
    backfill.start();
    await().atMost(Duration.ofSeconds(10)).until(() -> executor.requestCount() >= mode.requests());
    Thread.sleep(200);

    // every worker waits for its one answer, so no further request goes out
    assertThat(executor.requestCount()).isEqualTo(mode.requests());
    assertThat(executor.maxInFlight()).isEqualTo(mode.requests());
    executor.releaseResponses();
  }

  @Test
  void spreadsTheRequestsOverDifferentPeers() {
    blockchain = chain(51);
    executor.holdResponses();
    backfill = backfill(settings(BackfillMode.MEDIUM), head -> 1);

    backfill.start();

    await().atMost(Duration.ofSeconds(10)).until(() -> executor.requestCount() >= 4);
    assertThat(new HashSet<>(executor.peersUsed().subList(0, 4))).hasSize(4);
    executor.releaseResponses();
  }

  @Test
  void pausesWhileTheNodeIsBehindAndResumes() throws Exception {
    blockchain = chain(51);
    inSync.set(false);
    backfill = backfill(settings(NO_LIMIT, Duration.ZERO), head -> 1);

    backfill.start();
    Thread.sleep(300);
    assertThat(backfill.blocksStored()).isZero();
    assertThat(backfill.state()).isEqualTo(State.PAUSED);

    inSync.set(true);
    await().atMost(Duration.ofSeconds(30)).until(() -> backfill.state() == State.DONE);
    assertBodiesStored(1, HEAD);
  }

  @Test
  void slowModePausesAroundBlockImports() throws Exception {
    blockchain = chain(51);
    backfill = backfill(settings(BackfillMode.SLOW), head -> 1);
    backfill.onBlockAdded(
        BlockAddedEvent.createForHeadAdvancement(chain.get(HEAD), List.of(), List.of()));
    final long imported = System.nanoTime();

    backfill.start();
    Thread.sleep(500);
    assertThat(backfill.blocksStored()).isZero();
    assertThat(backfill.state()).isEqualTo(State.PAUSED);

    await().atMost(Duration.ofSeconds(30)).until(() -> backfill.state() == State.DONE);
    assertThat(System.nanoTime() - imported).isGreaterThan(Duration.ofMillis(1900).toNanos());
    assertBodiesStored(1, HEAD);
  }

  @Test
  void fastModeDoesNotPauseAroundBlockImports() {
    blockchain = chain(51);
    backfill = backfill(settings(BackfillMode.FAST), head -> 1);
    backfill.onBlockAdded(
        BlockAddedEvent.createForHeadAdvancement(chain.get(HEAD), List.of(), List.of()));

    backfill.start();

    await().atMost(Duration.ofMillis(500)).until(() -> backfill.blocksStored() > 0);
  }

  @Test
  void halvesItsRateWhileTheChainHeadLagsAndRecoversOnceItCatchesUp() {
    blockchain = chain(51);
    final long bytesPerSecond = 1 << 20;
    // the head is older than a slot plus the allowance
    clock.advance(Duration.ofSeconds(SLOT_SECONDS + 10));
    executor.holdResponses();
    backfill =
        backfill(
            new Settings(
                "test",
                bytesPerSecond,
                1,
                0,
                Duration.ZERO,
                Duration.ofMillis(20),
                Duration.ofSeconds(4),
                5,
                false,
                true),
            head -> 1);
    backfill.start();
    executor.allow(2);
    await().atMost(Duration.ofSeconds(10)).until(() -> backfill.blocksStored() > 0);
    assertThat(backfill.bytesPerSecond()).isEqualTo(bytesPerSecond / 2);
    assertThat(backfill.backedOff()).isTrue();

    // a fresh head, and a slot later the rate recovers a step
    final Block next =
        gen.block(
            new BlockOptions()
                .setBlockNumber(HEAD + 1)
                .setParentHash(chain.get(HEAD).getHash())
                .setDifficulty(Difficulty.ZERO)
                .hasOmmers(false)
                .hasTransactions(false)
                .setTimestamp(clock.instant().getEpochSecond()));
    blockchain.storeBlockHeaders(List.of(next.getHeader()), Difficulty.ONE);
    blockchain.unsafeSetChainHead(next.getHeader(), Difficulty.ONE);
    clock.advance(Duration.ofSeconds(SLOT_SECONDS + 1));
    executor.allow(2);

    await().atMost(Duration.ofSeconds(10)).until(() -> backfill.blocksStored() > 5);
    assertThat(backfill.bytesPerSecond()).isEqualTo(bytesPerSecond * 5 / 8);
    executor.releaseResponses();
  }

  @Test
  void resumesFromTheRecordedBlockAfterARestart() {
    blockchain = chain(51);
    backfill = backfill(settings(25_000, Duration.ZERO), head -> 1);
    backfill.start();
    await().atMost(Duration.ofSeconds(30)).until(() -> backfill.lowestStored() <= 40);
    backfill.stop();
    final long recorded = blockchain.getBodiesFrom().orElseThrow();
    assertThat(recorded).isBetween(2L, 40L);
    assertBodiesStored(recorded, HEAD);

    final FakePeerTaskExecutor restarted = new FakePeerTaskExecutor(this::answer);
    backfill = backfill(restarted, settings(NO_LIMIT, Duration.ZERO), head -> 1);
    backfill.start();

    await().atMost(Duration.ofSeconds(30)).until(() -> backfill.state() == State.DONE);
    assertBodiesStored(1, HEAD);
    assertThat(blockchain.getBodiesFrom()).contains(1L);
    assertThat(restarted.requestedBodies().stream().map(this::number))
        .allMatch(number -> number < recorded);
  }

  @Test
  void writesNothingAfterItIsStopped() throws Exception {
    blockchain = chain(51);
    executor.holdResponses();
    backfill = backfill(settings(NO_LIMIT, Duration.ZERO), head -> 1);
    backfill.start();
    await().atMost(Duration.ofSeconds(10)).until(() -> executor.requestCount() >= 1);

    backfill.stop();
    executor.releaseResponses();
    Thread.sleep(300);

    assertThat(backfill.blocksStored()).isZero();
    assertThat(blockchain.getBlockBody(chain.get(50).getHash())).isEmpty();
    assertThat(blockchain.getBodiesFrom()).contains(51L);
    assertThat(backfill.done()).isCompletedExceptionally();
  }

  @Test
  void stopsAtTheLowestBlockNeeded() {
    blockchain = chain(51);
    backfill = backfill(settings(NO_LIMIT, Duration.ZERO), head -> head - 29);

    backfill.start();

    await().atMost(Duration.ofSeconds(30)).until(() -> backfill.state() == State.DONE);
    assertBodiesStored(31, HEAD);
    for (long number = 1; number <= 30; number++) {
      assertThat(blockchain.getBlockBody(chain.get((int) number).getHash())).isEmpty();
    }
    assertThat(executor.requestedBodies().stream().map(this::number)).allMatch(n -> n >= 31);
    assertThat(blockchain.getBodiesFrom()).contains(31L);
    assertThat(backfill.blocksRemaining()).isZero();
  }

  @Test
  void hasNothingToDoWhenEveryNeededBodyIsStored() {
    blockchain = chain(1);
    backfill = backfill(settings(NO_LIMIT, Duration.ZERO), head -> 1);

    backfill.start();

    assertThat(backfill.state()).isEqualTo(State.DONE);
    assertThat(executor.requestCount()).isZero();
  }

  // two requests, no flush threshold, and a quick poll
  private static Settings settings(final long bytesPerSecond, final Duration importGrace) {
    return new Settings(
        "test",
        bytesPerSecond,
        2,
        0,
        importGrace,
        Duration.ofMillis(20),
        Duration.ofSeconds(4),
        5,
        false,
        true);
  }

  // the settings of a mode, with small ranges and a quick poll
  private static Settings settings(final BackfillMode mode) {
    return new Settings(
        mode.name(),
        mode.bytesPerSecond(),
        mode.requests(),
        0,
        mode.importGrace(),
        Duration.ofMillis(20),
        mode.headLagAllowance(),
        5,
        false,
        true);
  }

  private BodyBackfill backfill(final Settings settings, final LongUnaryOperator lowestNeeded) {
    return backfill(executor, settings, lowestNeeded);
  }

  private BodyBackfill backfill(
      final FakePeerTaskExecutor executor,
      final Settings settings,
      final LongUnaryOperator lowestNeeded) {
    return new BodyBackfill(
        blockchain,
        ProtocolScheduleFixture.MAINNET,
        executor,
        executor,
        inSync::get,
        blockchain.getBodiesFrom().orElseThrow(),
        lowestNeeded,
        settings,
        clock,
        new NoOpMetricsSystem());
  }

  // a chain of blocks with transactions and receipts, one a slot, the head two seconds old, every
  // header stored, the head set, and the bodies and receipts of the blocks from bodiesFrom only
  private MutableBlockchain chain(final long bodiesFrom) {
    final Block genesis = gen.genesisBlock();
    chain.add(genesis);
    final MutableBlockchain blockchain =
        InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesis);
    final long headTimestamp = clock.instant().getEpochSecond() - 2;
    Block parent = genesis;
    for (long number = 1; number <= HEAD; number++) {
      final List<Transaction> transactions =
          List.of(gen.transaction(), gen.transaction(), gen.transaction());
      final List<TransactionReceipt> receipts =
          transactions.stream().map(transaction -> gen.receipt()).toList();
      parent =
          gen.block(
              new BlockOptions()
                  .setBlockNumber(number)
                  .setParentHash(parent.getHash())
                  .setDifficulty(Difficulty.ZERO)
                  .setTimestamp(headTimestamp - (HEAD - number) * SLOT_SECONDS)
                  .hasOmmers(false)
                  .addTransaction(transactions)
                  .setReceiptsRoot(BodyValidation.receiptsRoot(receipts)));
      chain.add(parent);
      blocksByHash.put(parent.getHash(), parent);
      receiptsByHash.put(parent.getHash(), receipts);
    }
    blockchain.storeBlockHeaders(
        chain.subList(1, chain.size()).stream().map(Block::getHeader).toList(), Difficulty.ONE);
    for (long number = bodiesFrom; number <= HEAD; number++) {
      final Block block = chain.get((int) number);
      blockchain.unsafeImportBlock(block, receiptsByHash.get(block.getHash()), Optional.empty());
    }
    blockchain.unsafeSetChainHead(chain.get(HEAD).getHeader(), Difficulty.ONE);
    blockchain.unsafeSetBodiesFrom(bodiesFrom);
    return blockchain;
  }

  private MessageData answer(final MessageData message) {
    return switch (message.getCode()) {
      case EthProtocolMessages.GET_BLOCK_BODIES -> {
        final List<BlockBody> bodies = new ArrayList<>();
        for (final Hash hash : GetBlockBodiesMessage.readFrom(message).hashes()) {
          bodies.add(blocksByHash.get(hash).getBody());
        }
        yield BlockBodiesMessage.create(bodies);
      }
      case EthProtocolMessages.GET_RECEIPTS -> {
        final BytesValueRLPOutput out = new BytesValueRLPOutput();
        out.startList();
        for (final Hash hash : GetReceiptsMessage.readFrom(message).blockHashes()) {
          out.startList();
          receiptsByHash
              .get(hash)
              .forEach(
                  receipt ->
                      TransactionReceiptEncoder.writeTo(
                          receipt,
                          out,
                          TransactionReceiptEncodingConfiguration.DEFAULT_NETWORK_CONFIGURATION));
          out.endList();
        }
        out.endList();
        yield ReceiptsMessage.createUnsafe(out.encoded());
      }
      default -> throw new IllegalArgumentException("unexpected eth message " + message.getCode());
    };
  }

  private long number(final Hash hash) {
    return blocksByHash.get(hash).getHeader().getNumber();
  }

  private void assertBodiesStored(final long from, final long to) {
    for (long number = from; number <= to; number++) {
      final Block block = chain.get((int) number);
      assertThat(blockchain.getBlockBody(block.getHash())).contains(block.getBody());
      assertThat(blockchain.getTxReceipts(block.getHash()))
          .contains(receiptsByHash.get(block.getHash()));
    }
  }

  // a wall clock the tests move
  private static final class SettableClock extends Clock {
    private volatile Instant now = Instant.now();

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(final ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }

    void advance(final Duration duration) {
      now = now.plus(duration);
    }
  }
}
