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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.ProtocolScheduleFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Format;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Storages;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Timeout(180)
class Snap2SyncControllerTest {

  @TempDir Path dataDirectory;

  private final FakeSnapNetwork network = new FakeSnapNetwork();
  private Snap2SyncController controller;

  @AfterEach
  void tearDown() {
    if (controller != null) {
      controller.stop();
    }
    network.stop();
    assertThat(network.unagreedRequests()).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void syncsTheChainAndTheStateWhileTheHeadMoves(final Format format) throws Exception {
    final Random random = new Random(17);
    network.maxResponseBytes(4096);
    network.latency(25);
    final TestChain chain =
        new TestChain(random, TestWorld.random(random, 1500, 6, 1200), network, 24, 6);
    chain.extend(40);
    final MutableBlockchain blockchain =
        InMemoryKeyValueStorageProvider.createInMemoryBlockchain(chain.genesisBlock());
    final Storages storages = SyncStorageFixture.create(format);
    final Snap2Marker marker = new Snap2Marker(dataDirectory);
    controller = controller(chain, blockchain, storages, marker);

    final CompletableFuture<SnapSyncProcessState> result = controller.start();
    // the chain keeps growing while the sync runs
    while (!result.isDone() && chain.headNumber() < 140) {
      chain.extend(1);
      Thread.sleep(30);
    }
    final BlockHeader pivot = result.get(120, TimeUnit.SECONDS).getPivotBlockHeader().orElseThrow();

    assertThat(pivot.getNumber()).isGreaterThan(40 - 8);
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(pivot.getNumber());
    for (long number = 1; number <= pivot.getNumber(); number++) {
      assertThat(blockchain.getBlockHeader(number)).contains(chain.header(number));
      assertThat(blockchain.getBlockBody(chain.header(number).getHash())).isPresent();
    }
    // the stored tries hold the state of the pivot, whatever the storage format
    final TestWorld world = chain.world(pivot.getNumber());
    final var accountTrie = storages.accountTrie(Bytes32.wrap(pivot.getStateRoot().getBytes()));
    world
        .accountLeaves()
        .forEach((hash, account) -> assertThat(accountTrie.get(hash)).contains(account));
    assertThat(controller.metrics().pivotMoves()).isPositive();
    assertThat(controller.metrics().reorgs()).isZero();
    assertThat(controller.metrics().restarts()).isZero();
    controller.deletePivotSyncState();
    assertThat(marker.exists()).isFalse();
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void recoversFromAReorgBelowThePivot(final Format format) throws Exception {
    final Random random = new Random(29);
    network.maxResponseBytes(4096);
    network.latency(25);
    final TestChain chain =
        new TestChain(random, TestWorld.random(random, 1500, 6, 1200), network, 24, 6);
    chain.extend(40);
    final MutableBlockchain blockchain =
        InMemoryKeyValueStorageProvider.createInMemoryBlockchain(chain.genesisBlock());
    final Storages storages = SyncStorageFixture.create(format);
    controller = controller(chain, blockchain, storages, new Snap2Marker(dataDirectory));

    final CompletableFuture<SnapSyncProcessState> result = controller.start();
    // once the pivot 32 is chosen, the chain reorganises from block 29
    awaitStored(blockchain, chain.header(32));
    chain.reorg(29, 16);
    while (!result.isDone() && chain.headNumber() < 120) {
      chain.extend(1);
      Thread.sleep(30);
    }
    final BlockHeader pivot = result.get(120, TimeUnit.SECONDS).getPivotBlockHeader().orElseThrow();

    // the synced chain and state are the canonical ones
    assertThat(pivot.getHash()).isEqualTo(chain.header(pivot.getNumber()).getHash());
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(pivot.getNumber());
    for (long number = 1; number <= pivot.getNumber(); number++) {
      assertThat(blockchain.getBlockHeader(number)).contains(chain.header(number));
      assertThat(blockchain.getBlockBody(chain.header(number).getHash())).isPresent();
    }
    final var accountTrie = storages.accountTrie(Bytes32.wrap(pivot.getStateRoot().getBytes()));
    chain
        .world(pivot.getNumber())
        .accountLeaves()
        .forEach((hash, account) -> assertThat(accountTrie.get(hash)).contains(account));
    // recovered without starting over
    assertThat(controller.metrics().reorgs()).isPositive();
    assertThat(controller.metrics().restarts()).isZero();
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void startsOverWhenTheOrphanedBlockAccessListsAreGone(final Format format) throws Exception {
    final Random random = new Random(31);
    network.maxResponseBytes(4096);
    network.latency(25);
    final TestChain chain =
        new TestChain(random, TestWorld.random(random, 1500, 6, 1200), network, 24, 6);
    chain.extend(40);
    final MutableBlockchain blockchain =
        InMemoryKeyValueStorageProvider.createInMemoryBlockchain(chain.genesisBlock());
    final Storages storages = SyncStorageFixture.create(format);
    controller = controller(chain, blockchain, storages, new Snap2Marker(dataDirectory));

    final CompletableFuture<SnapSyncProcessState> result = controller.start();
    awaitStored(blockchain, chain.header(32));
    // the spec asks to discard the state when the orphaned lists cannot be fetched
    chain.reorg(29, 16, false);
    while (!result.isDone() && chain.headNumber() < 140) {
      chain.extend(1);
      Thread.sleep(30);
    }
    final BlockHeader pivot = result.get(150, TimeUnit.SECONDS).getPivotBlockHeader().orElseThrow();

    assertThat(pivot.getHash()).isEqualTo(chain.header(pivot.getNumber()).getHash());
    assertThat(blockchain.getChainHeadBlockNumber()).isEqualTo(pivot.getNumber());
    for (long number = 1; number <= pivot.getNumber(); number++) {
      assertThat(blockchain.getBlockHeader(number)).contains(chain.header(number));
      assertThat(blockchain.getBlockBody(chain.header(number).getHash())).isPresent();
    }
    final var accountTrie = storages.accountTrie(Bytes32.wrap(pivot.getStateRoot().getBytes()));
    chain
        .world(pivot.getNumber())
        .accountLeaves()
        .forEach((hash, account) -> assertThat(accountTrie.get(hash)).contains(account));
    assertThat(controller.metrics().restarts()).isPositive();
  }

  private static void awaitStored(final MutableBlockchain blockchain, final BlockHeader header) {
    await()
        .atMost(Duration.ofSeconds(30))
        .until(() -> blockchain.getBlockHeader(header.getHash()).isPresent());
  }

  private Snap2SyncController controller(
      final TestChain chain,
      final MutableBlockchain blockchain,
      final Storages storages,
      final Snap2Marker marker) {
    final SyncState syncState = mock(SyncState.class);
    when(syncState.getCheckpoint()).thenReturn(Optional.empty());
    final Snap2Configuration config =
        Snap2Configuration.getDefault().toBuilder()
            .pivotDistance(8)
            .pivotMoveThreshold(4)
            .accountChunks(16)
            .responseBytes(4096)
            .storageBatchSize(16)
            .maxStorageChunks(8)
            .codeBatchSize(8)
            .workerThreads(2)
            .trieBuildThreads(2)
            .pollInterval(Duration.ofMillis(100))
            .build();
    return new Snap2SyncController(
        config,
        ProtocolScheduleFixture.MAINNET,
        blockchain,
        mock(EthPeers.class),
        syncState,
        storages.sync(),
        chain,
        new NoOpMetricsSystem(),
        marker,
        false,
        false,
        new SyncDurationMetrics(new NoOpMetricsSystem()),
        () -> network);
  }
}
