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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.sync.snap2.FakeSnapNetwork;
import org.hyperledger.besu.ethereum.eth.sync.snap2.QueuedExecutor;
import org.hyperledger.besu.ethereum.eth.sync.snap2.TestWorld;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Format;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Storages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.TestTries;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.WorldStateTrieBuilder;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Runs the download on the test thread, in rounds: the network answers the requests sent, then the
 * loop and the writers of the download run what they queued. Its periodic dispatch only runs when a
 * test ticks it.
 */
class StateSyncTest {

  // 16 account ranges, 4 KiB responses, 8 storages a request, 8 ranges of a large storage
  private static final StateSync.Settings SETTINGS = new StateSync.Settings(16, 4096, 8, 8, 16);
  // far more than a download of these worlds takes
  private static final int MAX_ROUNDS = 10_000;

  private final FakeSnapNetwork network = new FakeSnapNetwork();
  private final QueuedExecutor tasks = new QueuedExecutor();
  private StateSync stateSync;

  @AfterEach
  void stopTheDownload() {
    if (stateSync != null) {
      stateSync.stop();
    }
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void downloadsAStaticState(final Format format) {
    final TestWorld world = TestWorld.random(new Random(3), 400, 3, 600);
    network.serve(world);
    // small responses split the large storages, and leave out some of the 16 bytecodes asked
    network.maxResponseBytes(1024);
    final Storages storages = SyncStorageFixture.create(format);
    final CompletableFuture<Void> completion = start(storages.sync(), header(1, world.root()));

    runToEnd();

    assertThat(completion).isCompleted();
    assertThat(world.codes().keySet()).allMatch(storages.sync()::hasCode);
    // every storage gets its trie as its slots arrive
    final int withStorage = world.storageLeaves().size();
    assertThat(stateSync.storageTriesBuilt()).isEqualTo(withStorage);
    assertThat(stateSync.storageTriesLeft()).isZero();
    final WorldStateTrieBuilder.Result built = build(storages);
    assertThat(built.root()).isEqualTo(world.root());
    assertThat(built.keptStorageTries()).isEqualTo(withStorage);
  }

  // its ranges come at two roots, so the final build makes its trie from its slots
  @ParameterizedTest
  @EnumSource(Format.class)
  void leavesAStorageThatChangesWhileDownloadingToTheFinalBuild(final Format format) {
    final Random random = new Random(19);
    final TestWorld world = TestWorld.random(random, 1, 1, 20_000);
    network.serve(world);
    network.maxResponseBytes(1024);
    final Storages storages = SyncStorageFixture.create(format);
    final CompletableFuture<Void> completion = start(storages.sync(), header(1, world.root()));

    runUntil(() -> stateSync.downloadedSlots() >= 500);
    final TestWorld next = move(world, random, 2, 1, 3);
    runToEnd();

    assertThat(completion).isCompleted();
    assertThat(stateSync.storageTriesLeft()).isOne();
    final WorldStateTrieBuilder.Result built = build(storages);
    assertThat(built.root()).isEqualTo(next.root());
    assertThat(built.keptStorageTries()).isEqualTo(next.storageLeaves().size() - 1);
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void updatesTheTrieOfAStorageThatChangesOnceDownloaded(final Format format) {
    final Random random = new Random(29);
    final TestWorld world = TestWorld.random(random, 1, 1, 2_000);
    network.serve(world);
    final Storages storages = SyncStorageFixture.create(format);
    final CompletableFuture<Void> completion = start(storages.sync(), header(1, world.root()));
    runToEnd();
    assertThat(completion).isCompleted();
    assertThat(stateSync.storageTriesBuilt()).isOne();

    final TestWorld next = move(world, random, 2, 1, 3);

    final WorldStateTrieBuilder.Result built = build(storages);
    assertThat(stateSync.storageTriesUpdated()).isPositive();
    assertThat(built.root()).isEqualTo(next.root());
    assertThat(built.keptStorageTries()).isEqualTo(next.storageLeaves().size());
  }

  // its account fetched again at the new pivot tells that its storage is now empty
  @ParameterizedTest
  @EnumSource(Format.class)
  void dropsAStorageThatAMoveEmptiesWhileDownloading(final Format format) {
    final TestWorld world = TestWorld.random(new Random(67), 1, 1, 2_000);
    network.serve(world);
    network.maxResponseBytes(1024);
    final Storages storages = SyncStorageFixture.create(format);
    final CompletableFuture<Void> completion = start(storages.sync(), header(1, world.root()));
    runUntil(() -> stateSync.downloadedSlots() > 0);

    final TestWorld next = world.copy();
    final StateDiff diff = new StateDiff();
    diff.add(next.clearStorages());
    network.serve(next);
    assertThat(stateSync.moveTo(header(2, next.root()), diff)).isCompleted();
    runToEnd();

    assertThat(completion).isCompleted();
    assertThat(build(storages).root()).isEqualTo(next.root());
  }

  // a write that fails loses the root node of the storage it completes, not the ranges of the other
  @ParameterizedTest
  @EnumSource(Format.class)
  void leavesAStorageWhoseRootNodeWasLostToTheFinalBuild(final Format format) {
    // two storages of 60 slots in one request: the answer completes the first and cuts the second
    final TestWorld world = TestWorld.random(new Random(43), 2, 2, 60);
    network.serve(world);
    final Storages storages = SyncStorageFixture.create(format);
    final AtomicBoolean failed = new AtomicBoolean();
    // the first commit holding a root node fails
    final SyncStateStorage storage =
        FailingTrieWriter.failingOnce(storages.sync(), Bytes::isEmpty, failed);
    final CompletableFuture<Void> completion =
        start(storage, accountRanges(1), header(1, world.root()));

    runToEnd();

    assertThat(completion).isCompleted();
    assertThat(failed).isTrue();
    assertThat(stateSync.storageTriesLeft()).isOne();
    assertThat(build(storages).root()).isEqualTo(world.root());
    // every storage trie reads back whole, the lost one rebuilt
    world
        .accountLeaves()
        .forEach(
            (hash, leaf) -> {
              final Bytes32 root = Bytes32.wrap(Accounts.decode(leaf).getStorageRoot().getBytes());
              assertThat(
                      storages.storageTrie(hash, root).entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE))
                  .isEqualTo(world.storageLeaves().getOrDefault(hash, new TreeMap<>()));
            });
  }

  @Test
  void requestsTheAccountRangesLowestFirst() {
    final TestWorld world = TestWorld.random(new Random(7), 2000, 0, 0);
    network.serve(world);
    final int ranges = 1024;
    final List<Bytes32> starts =
        List.copyOf(
            RangeManager.generateRanges(RangeManager.MIN_RANGE, RangeManager.MAX_RANGE, ranges)
                .keySet());

    final CompletableFuture<Void> completion =
        start(
            SyncStorageFixture.create(Format.BONSAI).sync(),
            accountRanges(ranges),
            header(1, world.root()));

    // before any answer, the requests sent are those of the lowest ranges
    final List<Bytes32> first = List.copyOf(network.accountRangeOrigins());
    assertThat(first)
        .isNotEmpty()
        .hasSizeLessThan(ranges)
        .isEqualTo(starts.subList(0, first.size()));
    runToEnd();
    assertThat(completion).isCompleted();
    // each range is started once, in key order
    assertThat(network.accountRangeOrigins().stream().filter(Set.copyOf(starts)::contains))
        .isSorted()
        .hasSize(ranges);
  }

  static Stream<Arguments> formatsAndSeeds() {
    return Stream.of(Format.values())
        .flatMap(
            format ->
                LongStream.of(11, 23, 37, 41, 59).mapToObj(seed -> Arguments.of(format, seed)));
  }

  // the moves change storages still downloading and storages whose trie is written
  @ParameterizedTest
  @MethodSource("formatsAndSeeds")
  void followsAMovingPivot(final Format format, final long seed) {
    final Random random = new Random(seed);
    TestWorld world = TestWorld.random(random, 1500, 6, 800);
    network.serve(world);
    final Storages storages = SyncStorageFixture.create(format);
    final CompletableFuture<Void> completion = start(storages.sync(), header(100, world.root()));

    long block = 100;
    for (int move = 0; move < 4; move++) {
      final long progress = progress() + 50;
      runUntil(() -> progress() >= progress);
      block += 3;
      world = move(world, random, block, 3, 25);
    }
    runToEnd();

    assertThat(completion).isCompleted();
    assertThat(stateSync.pivot().header().getNumber()).isEqualTo(block);
    assertThat(build(storages).root()).isEqualTo(world.root());
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void leavesOutAPeerThatServesNoState(final Format format) {
    final TestWorld world = TestWorld.random(new Random(13), 800, 3, 400);
    network.serve(world);
    network.serveNoState(1);
    // small responses make many requests, among which the few it got before being left out
    network.maxResponseBytes(1024);
    final Storages storages = SyncStorageFixture.create(format);
    final CompletableFuture<Void> completion = start(storages.sync(), header(1, world.root()));

    runToEnd();

    assertThat(completion).isCompleted();
    // its empty answers neither stalled the pivot nor kept it busy; it still gets bytecodes
    assertThat(stateSync.isPivotStale()).isFalse();
    final EthPeer leftOut = network.peers().get(1);
    assertThat(network.stateRequestsTo(leftOut)).isLessThan(network.stateRequestCount() / 10);
    assertThat(network.codeRequestsTo(leftOut) * 4).isGreaterThan(network.codeRequestCount());
    assertThat(build(storages).root()).isEqualTo(world.root());
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void detectsAStalePivot(final Format format) {
    final TestWorld world = TestWorld.random(new Random(5), 1500, 4, 800);
    network.serve(world);
    final Storages storages = SyncStorageFixture.create(format);
    final CompletableFuture<Void> completion = start(storages.sync(), header(7, world.root()));
    // every peer serves some of the state, then none serves its root, as when it gets too old
    round();
    network.forget(world.root());

    runToEnd();

    assertThat(stateSync.isPivotStale()).isTrue();
    assertThat(completion).isNotDone();
    // a move resumes the download, the empty answers of the stale pivot forgotten
    network.serve(world);
    assertThat(stateSync.moveTo(header(8, world.root()), new StateDiff())).isCompleted();
    tasks.runAll();
    network.forget(world.root());
    network.answer("account_range", 1);
    tasks.runAll();
    assertThat(stateSync.isPivotStale()).isFalse();
    network.serve(world);
    runToEnd();
    assertThat(completion).isCompleted();
    assertThat(build(storages).root()).isEqualTo(world.root());
  }

  // the empty answers to the requests of an older pivot say nothing about the new one
  @Test
  void ignoresTheEmptyAnswersOfAnOlderPivot() {
    final Random random = new Random(47);
    final TestWorld world = TestWorld.random(random, 1200, 0, 0);
    network.serve(world);
    start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));
    round();
    final TestWorld next = world.copy();
    final StateDiff diff = new StateDiff();
    diff.add(next.mutate(random, 10));
    network.serve(next);
    assertThat(stateSync.moveTo(header(2, next.root()), diff)).isCompleted();

    // the requests sent at the old pivot come back empty, as it is no longer served
    network.forget(world.root());
    round();

    assertThat(stateSync.isPivotStale()).isFalse();
  }

  // only empty answers in a row tell that the pivot is too old
  @Test
  void startsTheCountOfEmptyAnswersOverAfterAnAnswerWithState() {
    final TestWorld world = TestWorld.random(new Random(31), 1200, 0, 0);
    network.serve(world);
    start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));
    round();

    // ten empty answers, one with state, then two empty answers again
    network.forget(world.root());
    network.answer("account_range", 5);
    network.answer("storage_ranges", 5);
    tasks.runAll();
    network.serve(world);
    network.answer("storage_ranges", 1);
    tasks.runAll();
    network.forget(world.root());
    network.answer("storage_ranges", 2);
    tasks.runAll();

    assertThat(stateSync.isPivotStale()).isFalse();
  }

  // only left out peers serve the state: a request fails at once, as the next ones would
  @Test
  void waitsForThePeriodicDispatchWhileEveryPeerIsLeftOut() {
    final CompletableFuture<Void> completion =
        start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, root(1)));
    // no peer serves either pivot: answering nothing for the first says nothing about its age
    for (int round = 0; round < 3; round++) {
      round();
    }
    assertThat(stateSync.isPivotStale()).isFalse();
    // and leaves each peer out at the second
    assertThat(stateSync.moveTo(header(2, root(2)), new StateDiff())).isCompleted();
    runToEnd();
    assertThat(stateSync.peersWithoutState()).isEqualTo(network.peers().size());

    final int sent = network.requestCount();
    tasks.tick();
    assertThat(network.requestCount()).isGreaterThan(sent);
    final int retried = network.requestCount();
    round();
    assertThat(network.requestCount()).isEqualTo(retried);
    assertThat(completion).isNotDone();
  }

  // a diff that failed is partly applied, so the download does not go on from it
  @Test
  void stopsAndFailsWhenAMoveFailsToApply() {
    final TestWorld world = TestWorld.random(new Random(41), 300, 2, 200);
    network.serve(world);
    final SyncStateStorage storage = spy(SyncStorageFixture.create(Format.BONSAI).sync());
    final CompletableFuture<Void> completion = start(storage, header(1, world.root()));
    round();
    final StorageException diskFailure = new StorageException("disk failure");
    doThrow(diskFailure).when(storage).batch();

    final CompletableFuture<StateDiffApplier.Outcome> move =
        stateSync.moveTo(header(2, world.root()), new StateDiff());

    assertThat(move).isCompletedExceptionally();
    assertThatThrownBy(move::join).hasCause(diskFailure);
    assertThat(completion).isCompletedExceptionally();
    assertThatThrownBy(completion::join).hasCause(diskFailure);
    // the answers in flight write nothing, and nothing more is sent
    final long written = progress();
    final int sent = network.requestCount();
    round();
    tasks.tick();
    assertThat(progress()).isEqualTo(written);
    assertThat(network.requestCount()).isEqualTo(sent);
  }

  // a quarter more than the peers take, so that a peer gets its next request at once
  @Test
  void keepsAQuarterMoreRequestsInFlightThanThePeersTake() {
    final TestWorld world = TestWorld.random(new Random(31), 1200, 0, 0);
    network.serve(world);
    start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));

    round();

    // the peers take 12 requests, a third of the 15 sent for account ranges
    assertThat(network.unansweredRequests())
        .hasSize(15)
        .filteredOn("account_range"::equals)
        .hasSize(5);
  }

  static Stream<Arguments> backlogs() {
    return Stream.of(
        Arguments.of("bytecodes", TestWorld.random(new Random(59), 80_000, 80_000, 0)),
        Arguments.of("storages", TestWorld.storagesWithoutCode(new Random(61), 80_000)));
  }

  // the accounts wait while more than 50,000 storages or bytecodes do, to bound their memory
  @ParameterizedTest(name = "{0}")
  @MethodSource("backlogs")
  void asksNoAccountRangeWhileTheirStoragesOrBytecodesLag(
      final String backlog, final TestWorld world) {
    network.serve(world);
    // the bound is passed once 13 of the 20 ranges are in
    start(
        SyncStorageFixture.create(Format.BONSAI).sync(),
        new StateSync.Settings(20, 64 * 1024 * 1024, 8, 8, 16),
        header(1, world.root()));

    // each round asks for as many ranges as it answered, all of them by the third
    for (int round = 0; round < 4; round++) {
      round();
    }

    assertThat(network.accountRangeOrigins()).hasSizeLessThan(20);
  }

  // as many ranges as the density of its first slots suggests, up to the most a storage gets
  @Test
  void asksALargeStorageInParallelSlotRanges() {
    final TestWorld world = TestWorld.random(new Random(37), 1, 1, 20_000);
    network.serve(world);
    start(
        SyncStorageFixture.create(Format.BONSAI).sync(), accountRanges(1), header(1, world.root()));

    // the account, then the first slots of its storage
    round();
    round();

    assertThat(network.unansweredRequests()).containsOnly("storage_chunk").hasSize(8);
  }

  // the last account range brings a bytecode, which the download waits for
  @Test
  void completesOnceTheLastBytecodeIsStored() {
    // one account, with a bytecode and no storage
    final TestWorld world = TestWorld.random(new Random(53), 1, 1, 0);
    network.serve(world);
    final Storages storages = SyncStorageFixture.create(Format.BONSAI);
    final CompletableFuture<Void> completion =
        start(storages.sync(), accountRanges(1), header(1, world.root()));

    round();
    assertThat(completion).isNotDone();
    round();

    assertThat(completion).isCompleted();
    assertThat(world.codes().keySet()).hasSize(1).allMatch(storages.sync()::hasCode);
  }

  @Test
  void sendsNothingWhilePaused() {
    final TestWorld world = TestWorld.random(new Random(17), 300, 1, 100);
    network.serve(world);
    final CompletableFuture<Void> completion =
        start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));

    stateSync.pause();
    round();
    tasks.tick();
    assertThat(network.hasUnanswered()).isFalse();

    stateSync.resume();
    runToEnd();
    assertThat(completion).isCompleted();
  }

  // a request no peer takes fails, as the next ones would: they wait for the periodic dispatch
  @Test
  void waitsForThePeriodicDispatchWhileNoPeerTakesItsRequests() {
    final TestWorld world = TestWorld.random(new Random(71), 300, 1, 100);
    network.serve(world);
    final CompletableFuture<Void> completion =
        start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));

    network.disconnect();
    round();
    final int sent = network.requestCount();
    tasks.tick();
    assertThat(network.requestCount()).isGreaterThan(sent);
    final int retried = network.requestCount();
    round();
    assertThat(network.requestCount()).isEqualTo(retried);

    // the periodic dispatch resumes the download once peers are back
    network.reconnect();
    tasks.tick();
    runToEnd();
    assertThat(completion).isCompleted();
  }

  static Stream<StateSync.Settings> settingsWithAZero() {
    return Stream.of(
        new StateSync.Settings(0, 4096, 8, 8, 16),
        new StateSync.Settings(16, 0, 8, 8, 16),
        new StateSync.Settings(16, 4096, 0, 8, 16),
        new StateSync.Settings(16, 4096, 8, 0, 16),
        new StateSync.Settings(16, 4096, 8, 8, 0));
  }

  // a zero would leave nothing to ask, or a queue that never drains
  @ParameterizedTest
  @MethodSource("settingsWithAZero")
  void refusesSettingsWithAZero(final StateSync.Settings settings) {
    final SyncStateStorage storage = SyncStorageFixture.create(Format.BONSAI).sync();

    assertThatThrownBy(() -> stateSync(storage, settings, tasks))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void startsOnce() {
    final TestWorld world = TestWorld.random(new Random(73), 10, 0, 0);
    network.serve(world);
    start(SyncStorageFixture.create(Format.BONSAI).sync(), header(1, world.root()));

    assertThatThrownBy(() -> stateSync.start(header(1, world.root())))
        .isInstanceOf(IllegalStateException.class);
  }

  // its loop shut down, as a real one refuses the periodic dispatch
  @Test
  void startsNothingOnceStopped() {
    final ScheduledExecutorService loop = Executors.newSingleThreadScheduledExecutor();
    stateSync = stateSync(SyncStorageFixture.create(Format.BONSAI).sync(), SETTINGS, loop);
    stateSync.stop();

    assertThat(stateSync.start(header(1, Bytes32.ZERO))).isCompletedExceptionally();
    assertThat(network.requestCount()).isZero();
  }

  // the settings, the account key space split into that many ranges
  private static StateSync.Settings accountRanges(final int ranges) {
    return new StateSync.Settings(
        ranges,
        SETTINGS.responseBytes(),
        SETTINGS.storageBatchSize(),
        SETTINGS.maxStorageChunks(),
        SETTINGS.codeBatchSize());
  }

  // a download on the test thread, its first requests sent
  private CompletableFuture<Void> start(final SyncStateStorage storage, final BlockHeader pivot) {
    return start(storage, SETTINGS, pivot);
  }

  private CompletableFuture<Void> start(
      final SyncStateStorage storage, final StateSync.Settings settings, final BlockHeader pivot) {
    stateSync = stateSync(storage, settings, tasks);
    final CompletableFuture<Void> completion = stateSync.start(pivot);
    tasks.runAll();
    return completion;
  }

  // a download whose writes and storage trie updates run on the test thread
  private StateSync stateSync(
      final SyncStateStorage storage,
      final StateSync.Settings settings,
      final ScheduledExecutorService loop) {
    // with a clock that stands still, a peer left out stays out
    return new StateSync(
        storage,
        network,
        settings,
        loop,
        tasks,
        () -> 0L,
        (withoutMoves, trieLeft) ->
            TestTries.updaterWithoutWorkers(storage, withoutMoves, trieLeft));
  }

  // answers the requests sent, then runs what the download queued
  private void round() {
    network.answerAll();
    tasks.runAll();
  }

  // runs rounds until the condition holds or no request waits for an answer
  private void runUntil(final BooleanSupplier condition) {
    tasks.runAll();
    for (int rounds = 0; network.hasUnanswered() && !condition.getAsBoolean(); rounds++) {
      assertThat(rounds).as("rounds").isLessThan(MAX_ROUNDS);
      round();
    }
  }

  private void runToEnd() {
    runUntil(() -> false);
  }

  // moves the pivot over blocks that change the world; the answers at the old pivot are dropped
  private TestWorld move(
      final TestWorld world,
      final Random random,
      final long toBlock,
      final int blocks,
      final int changes) {
    final TestWorld next = world.copy();
    final StateDiff diff = new StateDiff();
    for (int b = 0; b < blocks; b++) {
      diff.add(next.mutate(random, changes));
    }
    network.serve(next);
    assertThat(stateSync.moveTo(header(toBlock, next.root()), diff)).isCompleted();
    round();
    network.forget(world.root());
    return next;
  }

  private long progress() {
    return stateSync.downloadedAccounts() + stateSync.downloadedSlots();
  }

  // the download applies its last changes to the tries and stops first, as before the final build
  private WorldStateTrieBuilder.Result build(final Storages storages) {
    stateSync.finishStorageTries();
    stateSync.stop();
    return TestTries.build(storages.sync(), stateSync::storageTrieLeft);
  }

  // a state root no peer serves
  private static Bytes32 root(final int value) {
    return Bytes32.leftPad(Bytes.of(value));
  }

  private static BlockHeader header(final long number, final Bytes32 root) {
    return new BlockHeaderTestFixture().number(number).stateRoot(Hash.wrap(root)).buildHeader();
  }
}
