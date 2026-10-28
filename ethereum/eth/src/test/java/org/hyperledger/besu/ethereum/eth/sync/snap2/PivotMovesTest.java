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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BalFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.TestStateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Format;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.TestTries;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Moves the pivot of a state download on the test thread: the chain answers at once, the state
 * requests wait for the rounds of the test.
 */
class PivotMovesTest {

  // 16 account ranges, 4 KiB responses, 8 storages a request, 8 ranges of a large storage
  private static final StateSync.Settings STATE = new StateSync.Settings(16, 4096, 8, 8, 16);
  // a move once the head is six blocks ahead, to four blocks behind it
  private static final PivotMoves.Settings SETTINGS = new PivotMoves.Settings(4, 2);
  // far more than a download of these worlds takes
  private static final int MAX_ROUNDS = 10_000;

  private final FakeSnapNetwork network = new FakeSnapNetwork();
  private TestChain chain = chain(TestWorld.random(new Random(11), 60, 2, 50));
  private MutableBlockchain blockchain =
      InMemoryKeyValueStorageProvider.createInMemoryBlockchain(chain.genesisBlock());
  private final SyncStateStorage storage = spy(SyncStorageFixture.create(Format.BONSAI).sync());
  private final QueuedExecutor tasks = new QueuedExecutor();
  private final QueuedExecutor retries = new QueuedExecutor();
  private final List<BlockHeader> storedHeaders = new ArrayList<>();
  private final List<RestartException> restarts = new ArrayList<>();
  private StateSync stateSync;

  @AfterEach
  void stopTheDownload() {
    if (stateSync != null) {
      stateSync.stop();
    }
  }

  @Test
  void movesThePivotOnceTheHeadIsFarEnough() {
    chain.extend(7);
    final PivotMoves moves = moves(2);

    moves.maybeMove();
    assertThat(pivot()).isEqualTo(2);

    chain.extend(1);
    moves.maybeMove();

    // four blocks behind the head, the headers and lists of the blocks passed stored
    assertThat(pivot()).isEqualTo(4);
    assertThat(storedHeaders).isEqualTo(chain.headers(3, 4));
    assertThat(blockchain.getBlockAccessList(chain.header(3).getHash())).isPresent();
    assertThat(blockchain.getBlockAccessList(chain.header(4).getHash())).isPresent();
    assertThat(moves.moves()).isOne();
    assertThat(builtRoot()).isEqualTo(chain.world(4).root());
  }

  @Test
  void movesAPivotPeersStoppedServing() {
    // enough state for many requests once peers stop serving it
    chain = chain(TestWorld.random(new Random(5), 1500, 4, 800));
    blockchain = InMemoryKeyValueStorageProvider.createInMemoryBlockchain(chain.genesisBlock());
    chain.extend(7);
    final PivotMoves moves = moves(2);
    // peers serve some of its state, then none of it
    network.answerAll();
    tasks.runAll();
    network.forget(chain.world(2).root());
    runToEnd();
    assertThat(stateSync.isPivotStale()).isTrue();

    // five blocks ahead, not enough for a move of a pivot still served
    moves.maybeMove();

    assertThat(pivot()).isEqualTo(3);
    assertThat(builtRoot()).isEqualTo(chain.world(3).root());
  }

  @Test
  void movesInStepsWhileTheDownloadSendsNothing() {
    chain.extend(140);
    stateSync = spy(TestStateSync.create(storage, network, STATE, tasks));
    final PivotMoves moves = startedMoves(2);

    moves.maybeMove();

    // 128 blocks then the 6 left
    assertThat(pivot()).isEqualTo(136);
    assertThat(moves.moves()).isEqualTo(2);
    assertThat(storedHeaders).isEqualTo(chain.headers(3, 136));
    final InOrder order = inOrder(stateSync);
    order.verify(stateSync).pause();
    order.verify(stateSync, times(2)).moveTo(any(), any());
    order.verify(stateSync).resume();
    assertThat(builtRoot()).isEqualTo(chain.world(136).root());
  }

  @Test
  void restartsWhenTheChainReorganisesBelowThePivot() {
    chain.extend(4);
    final PivotMoves moves = moves(4);
    chain.reorg(2, 8);

    moves.maybeMove();

    assertThat(restarts).singleElement().extracting(Throwable::getMessage).asString().contains("4");
    assertThat(pivot()).isEqualTo(4);
    assertThat(storedHeaders).isEmpty();
  }

  @Test
  void restartsWhenAMoveFailsToApply() {
    chain.extend(8);
    final PivotMoves moves = moves(2);
    runToEnd();
    doThrow(new StorageException("disk failure")).when(storage).batch();

    moves.maybeMove();

    assertThat(restarts).singleElement().extracting(Throwable::getCause).isNotNull();
    assertThat(moves.moves()).isZero();
  }

  @Test
  void settlesOnceTheMoveInProgressEnds() {
    chain.extend(8);
    final HeaderFetcher fetcher = spy(fetcher());
    final CompletableFuture<BlockHeader> head = new CompletableFuture<>();
    doReturn(head).doCallRealMethod().when(fetcher).byHash(any(), any());
    final PivotMoves moves = moves(2, fetcher);
    moves.maybeMove();

    final CompletableFuture<Void> settled = moves.settle();

    assertThat(settled).isNotDone();
    // a head too close for a move ends it
    head.complete(chain.header(3));
    assertThat(settled).isCompleted();
    // the head is far enough, but the moves ended
    moves.maybeMove();
    assertThat(pivot()).isEqualTo(2);
  }

  @Test
  void catchesUpWithAHeadTooCloseForAMove() {
    chain.extend(7);
    final PivotMoves moves = moves(2);

    assertThat(moves.catchUp()).isCompleted();

    assertThat(pivot()).isEqualTo(3);
    assertThat(builtRoot()).isEqualTo(chain.world(3).root());
  }

  @Test
  void restartsACatchUpOnAPivotOffTheChainOfTheHead() {
    chain.extend(6);
    final PivotMoves moves = moves(5);
    chain.reorg(4, 3);

    final CompletableFuture<Void> catchUp = moves.catchUp();

    assertThat(catchUp).isCompletedExceptionally();
    // not tried again
    retries.runAll();
    assertThat(restarts).hasSize(1);
  }

  @Test
  void triesACatchUpAgainAfterAFailure() {
    chain.extend(7);
    final PivotMoves moves = moves(2);
    chain.sender().fail(message -> message.getCode() == EthProtocolMessages.GET_BLOCK_ACCESS_LISTS);

    final CompletableFuture<Void> catchUp = moves.catchUp();

    assertThat(catchUp).isNotDone();
    chain.sender().fail(message -> false);
    retries.runAll();
    assertThat(catchUp).isCompleted();
    assertThat(pivot()).isEqualTo(3);
  }

  @Test
  void movesNothingOnceStopped() {
    chain.extend(8);
    final PivotMoves moves = moves(2);

    moves.stop();
    moves.maybeMove();

    assertThat(moves.catchUp()).isCompletedExceptionally();
    assertThat(pivot()).isEqualTo(2);
    assertThat(storedHeaders).isEmpty();
  }

  private TestChain chain(final TestWorld genesis) {
    return new TestChain(new Random(17), genesis, network, 300, 4);
  }

  private HeaderFetcher fetcher() {
    return new HeaderFetcher(network, ProtocolScheduleFixture.MAINNET, chain, 8);
  }

  // the moves of a download started at the pivot, through the network
  private PivotMoves moves(final long pivot) {
    return moves(pivot, fetcher());
  }

  private PivotMoves moves(final long pivot, final HeaderFetcher fetcher) {
    stateSync = TestStateSync.create(storage, network, STATE, tasks);
    return startedMoves(pivot, fetcher);
  }

  private PivotMoves startedMoves(final long pivot) {
    return startedMoves(pivot, fetcher());
  }

  private PivotMoves startedMoves(final long pivot, final HeaderFetcher fetcher) {
    stateSync.start(chain.header(pivot));
    tasks.runAll();
    return new PivotMoves(
        stateSync,
        storedHeaders::addAll,
        blockchain,
        chain,
        fetcher,
        // a round without the lists fails the fetch
        new BalFetcher(network, 32, 1 << 20, 1),
        SETTINGS,
        restarts::add,
        retries);
  }

  private long pivot() {
    return stateSync.pivot().header().getNumber();
  }

  // runs rounds until no request waits for an answer
  private void runToEnd() {
    tasks.runAll();
    for (int rounds = 0; network.hasUnanswered(); rounds++) {
      assertThat(rounds).as("rounds").isLessThan(MAX_ROUNDS);
      network.answerAll();
      tasks.runAll();
    }
  }

  // the root of the state downloaded and moved, once the download ends
  private Bytes32 builtRoot() {
    runToEnd();
    stateSync.finishStorageTries();
    stateSync.stop();
    return TestTries.build(storage, stateSync::storageTrieLeft).root();
  }
}
