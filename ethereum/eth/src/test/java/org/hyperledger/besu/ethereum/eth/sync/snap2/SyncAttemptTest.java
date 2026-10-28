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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BalFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.TestStateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Format;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Storages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.TestTries;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

import com.google.common.base.Throwables;
import org.junit.jupiter.api.Test;

/**
 * Runs an attempt on the test thread, in rounds: the network answers the state requests, then the
 * state download, the header download, the window and the final build run what they queued. The
 * chain answers at once.
 */
class SyncAttemptTest {

  // 16 account ranges, 4 KiB responses, 8 storages a request, 8 ranges of a large storage
  private static final StateSync.Settings STATE = new StateSync.Settings(16, 4096, 8, 8, 16);
  // a move once the head is six blocks ahead, to four blocks behind it
  private static final PivotMoves.Settings MOVES = new PivotMoves.Settings(4, 2);
  private static final int WINDOW = 4;
  // far more than an attempt on these worlds takes
  private static final int MAX_ROUNDS = 10_000;

  private final FakeSnapNetwork network = new FakeSnapNetwork();
  private final TestChain chain =
      new TestChain(new Random(23), TestWorld.random(new Random(23), 60, 2, 50), network, 300, 4);
  private final MutableBlockchain blockchain =
      InMemoryKeyValueStorageProvider.createInMemoryBlockchain(chain.genesisBlock());
  private final Storages storages = SyncStorageFixture.create(Format.BONSAI);
  private final SyncStateStorage storage = spy(storages.sync());
  // the loop and the writes of the state download
  private final QueuedExecutor tasks = new QueuedExecutor();
  private final QueuedExecutor headers = new QueuedExecutor();
  private final QueuedExecutor windowWorkers = new QueuedExecutor();
  private final QueuedExecutor resumes = new QueuedExecutor();
  private final QueuedExecutor builds = new QueuedExecutor();
  private final QueuedExecutor retries = new QueuedExecutor();

  @Test
  void syncsTheChainAndTheStateWhileTheHeadMoves() {
    chain.extend(12);
    final SyncAttempt attempt = attempt(8);
    final CompletableFuture<BlockHeader> synced = attempt.start();
    stateRound();
    chain.extend(4);

    attempt.maybeMove();
    runUntil(synced::isDone);

    // four blocks behind the head, the bodies of the window up to it stored
    assertThat(synced).isCompletedWithValue(chain.header(12));
    assertThat(attempt.pivotMoves()).isOne();
    assertThat(blockchain.getChainHeadHeader()).isEqualTo(chain.header(12));
    assertThat(storages.bonsai().getWorldStateRootHash()).contains(chain.world(12).root());
    assertThat(storages.bonsai().getWorldStateBlockHash()).contains(chain.header(12).getHash());
    for (long number = 1; number <= 12; number++) {
      assertThat(blockchain.getBlockHeader(number)).contains(chain.header(number));
      assertThat(blockchain.getBlockBody(chain.header(number).getHash()).isPresent())
          .isEqualTo(number > 12 - WINDOW);
    }
  }

  @Test
  void setsTheChainHeadOnceEveryHeaderIsStored() {
    chain.extend(10);
    final SyncAttempt attempt = attempt(6);
    final CompletableFuture<BlockHeader> synced = attempt.start();

    // the state and the tries, the header download left waiting
    runUntil(() -> storages.bonsai().getWorldStateRootHash().isPresent(), this::stateAndBuild);

    assertThat(synced).isNotDone();
    assertThat(blockchain.getChainHeadBlockNumber()).isZero();
    runUntil(synced::isDone);
    assertThat(synced).isCompletedWithValue(chain.header(6));
    assertThat(blockchain.getChainHeadHeader()).isEqualTo(chain.header(6));
  }

  @Test
  void restartsWhenTheBuiltRootDoesNotMatchThePivot() {
    chain.extend(10);
    final SyncAttempt attempt = attempt(6);
    final CompletableFuture<BlockHeader> synced = attempt.start();
    // the whole state, the final build left waiting
    stateRound();
    runUntil(() -> !network.hasUnanswered(), this::stateRound);
    // the flat state lost an account
    storage.write(batch -> batch.removeAccount(chain.world(6).accountLeaves().firstKey()));

    runUntil(synced::isDone);

    assertThat(restartCause(synced)).hasMessageContaining("does not match");
    assertThat(storages.bonsai().getWorldStateRootHash()).isEmpty();
    assertThat(blockchain.getChainHeadBlockNumber()).isZero();
  }

  @Test
  void restartsWhenTheStateDownloadFails() {
    chain.extend(12);
    final SyncAttempt attempt = attempt(8);
    final CompletableFuture<BlockHeader> synced = attempt.start();
    stateRound();
    chain.extend(4);
    doThrow(new StorageException("disk failure")).when(storage).batch();

    attempt.maybeMove();

    assertThat(restartCause(synced)).isNotNull();
  }

  @Test
  void restartsWhenTheChainReorganisesBelowThePivot() {
    chain.extend(12);
    final SyncAttempt attempt = attempt(8);
    final CompletableFuture<BlockHeader> synced = attempt.start();
    chain.reorg(6, 10);

    attempt.maybeMove();

    assertThat(restartCause(synced)).hasMessageContaining("reorganised");
  }

  @Test
  void writesNothingOnceCancelled() throws InterruptedException {
    chain.extend(12);
    final SyncAttempt attempt = attempt(8);
    final CompletableFuture<BlockHeader> synced = attempt.start();
    stateRound();

    attempt.cancel();
    attempt.awaitStop();
    clearInvocations(storage);
    for (int rounds = 0; rounds < 10; rounds++) {
      round();
    }

    assertThat(synced)
        .failsWithin(Duration.ZERO)
        .withThrowableThat()
        .havingRootCause()
        .isInstanceOf(CancellationException.class);
    verify(storage, never()).batch();
    verify(storage, never()).trieWriter();
    assertThat(blockchain.getChainHeadBlockNumber()).isZero();
  }

  private SyncAttempt attempt(final long pivot) {
    final HeaderFetcher fetcher =
        new HeaderFetcher(network, ProtocolScheduleFixture.MAINNET, chain, 8);
    final SyncAttempt.Context context =
        new SyncAttempt.Context(
            blockchain,
            storage,
            chain,
            fetcher,
            // a round without the lists fails the fetch
            new BalFetcher(network, 32, 1 << 20, 1),
            MOVES,
            () -> TestStateSync.create(storage, network, STATE, tasks),
            () ->
                new SyncChain(
                    blockchain,
                    ProtocolScheduleFixture.MAINNET,
                    network,
                    fetcher,
                    Optional.empty(),
                    new SyncChain.Settings(
                        8, 4, WINDOW, false, false, Optional.of(Difficulty.ZERO)),
                    new NoOpMetricsSystem(),
                    headers,
                    windowWorkers,
                    resumes,
                    millis -> {},
                    10_000),
            trieLeft -> TestTries.builder(storage, trieLeft),
            builds,
            retries);
    return new SyncAttempt(context, chain.header(pivot));
  }

  // answers the state requests, then runs what the state download queued
  private void stateRound() {
    network.answerAll();
    tasks.runAll();
  }

  private void stateAndBuild() {
    stateRound();
    builds.runAll();
  }

  private void round() {
    stateRound();
    headers.runAll();
    windowWorkers.runAll();
    builds.runAll();
  }

  private void runUntil(final BooleanSupplier condition) {
    runUntil(condition, this::round);
  }

  private void runUntil(final BooleanSupplier condition, final Runnable round) {
    for (int rounds = 0; !condition.getAsBoolean(); rounds++) {
      assertThat(rounds).as("rounds").isLessThan(MAX_ROUNDS);
      round.run();
    }
  }

  private static Throwable restartCause(final CompletableFuture<BlockHeader> synced) {
    assertThat(synced).isCompletedExceptionally();
    final Throwable failure = synced.handle((unused, error) -> error).join();
    return Throwables.getCausalChain(failure).stream()
        .filter(RestartException.class::isInstance)
        .findFirst()
        .orElseThrow(() -> new AssertionError("no restart in " + failure));
  }
}
