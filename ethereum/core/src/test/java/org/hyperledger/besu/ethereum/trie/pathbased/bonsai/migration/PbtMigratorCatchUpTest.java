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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** The migrator catching up from genesis through trie logs: roll size, pauses, write priority. */
class PbtMigratorCatchUpTest {

  private static final long PBT_TIME = 1_000L;
  private static final DataStorageConfiguration CONFIG =
      DataStorageConfiguration.DEFAULT_BONSAI_PBT_CONFIG;

  /**
   * Blocks after genesis; block {@code i} creates account {@code i} with a balance of {@code i}.
   */
  private static final int BLOCKS = 5;

  private final Map<Hash, BlockHeader> headers = new HashMap<>();
  private final List<BlockHeader> chain = new ArrayList<>();
  private final Blockchain blockchain = mock(Blockchain.class);
  private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

  /** The block the binary column holds at each pause. */
  private final List<Long> pausedAt = new ArrayList<>();

  private final List<Duration> pauses = new ArrayList<>();
  private Runnable onPause = () -> {};

  private SegmentedKeyValueStorage database;
  private BonsaiWorldStateKeyValueStorage storage;
  private BonsaiWorldStateProvider provider;
  private PbtMigrator migrator;

  @BeforeEach
  void buildChain() throws Exception {
    database = spy(new SegmentedInMemoryKeyValueStorage());
    storage =
        (BonsaiWorldStateKeyValueStorage)
            new KeyValueStorageProvider(
                    segments -> database, new InMemoryKeyValueStorage(), new NoOpMetricsSystem())
                .createWorldStateStorage(CONFIG);
    // The provider reads the head before genesis is built.
    when(blockchain.getChainHeadHeader())
        .thenAnswer(
            i ->
                chain.isEmpty()
                    ? new BlockHeaderTestFixture().number(0).timestamp(0).buildHeader()
                    : chain.getLast());
    when(blockchain.getBlockHeader(any(Hash.class)))
        .thenAnswer(i -> Optional.ofNullable(headers.get(i.<Hash>getArgument(0))));
    when(blockchain.getBlockHeader(anyLong())).thenAnswer(i -> canonical(i.getArgument(0)));
    when(blockchain.getBlockHashByNumber(anyLong()))
        .thenAnswer(i -> canonical(i.getArgument(0)).map(BlockHeader::getBlockHash));
    when(scheduler.awaitTermination(anyLong(), any())).thenReturn(true);
    provider = newProvider(storage, blockchain, PBT_TIME);

    block((BonsaiWorldState) provider.getWorldState(), u -> {});
    for (int i = 1; i <= BLOCKS; i++) {
      final int account = i;
      block(parentState(), u -> u.createAccount(address(account), 0, Wei.of(account)));
    }
  }

  @AfterEach
  void closeMigrator() {
    if (migrator != null) {
      migrator.close();
    }
  }

  @Test
  void catchUpRollsAtMostBlocksPerRollAndPausesAfterEachStep() {
    tick(2, Optional.of(Duration.ofSeconds(1)));

    // Genesis, then rolls of two blocks at most, each followed by a pause.
    assertThat(pausedAt).containsExactly(0L, 2L, 4L, 5L);
    assertThat(binaryRoot()).contains(binaryRootFromScratch());
  }

  @Test
  void noPauseWhenThePauseIsZero() {
    tick(PbtMigrator.Tuning.DEFAULT.blocksPerRoll(), Optional.of(Duration.ZERO));

    assertThat(pausedAt).isEmpty();
    assertThat(cursor()).isEqualTo(BLOCKS);
    assertThat(binaryRoot()).contains(binaryRootFromScratch());
  }

  @Test
  void everyStepCommitsAtLowPriority() {
    clearInvocations(database);

    tick(2, Optional.of(Duration.ZERO));

    // Genesis and three rolls.
    assertThat(cursor()).isEqualTo(BLOCKS);
    verify(database, times(4)).startLowPriorityTransaction();
  }

  @Test
  void pausesAsLongAsTheLastStepTookByDefault() {
    tick(2, Optional.empty(), Duration.ofSeconds(3));

    assertThat(pauses).hasSize(4).containsOnly(Duration.ofSeconds(3));
  }

  @Test
  void capsTheDefaultPauseAtTenSeconds() {
    tick(2, Optional.empty(), Duration.ofMinutes(1));

    assertThat(pauses).hasSize(4).containsOnly(Duration.ofSeconds(10));
  }

  @Test
  void stopsOnceTheChainClaimsTheBinaryColumn() {
    onPause = () -> provider.getBinaryColumnOwnership().claimForChain();

    tick(2, Optional.of(Duration.ofSeconds(1)));

    // Genesis only: the chain claims the column during the pause after it.
    assertThat(pausedAt).containsExactly(0L);
    assertThat(cursor()).isZero();
  }

  private void tick(final int blocksPerRoll, final Optional<Duration> rollPause) {
    tick(blocksPerRoll, rollPause, Duration.ZERO);
  }

  /**
   * Runs one tick of a migrator with no cursor yet, recording its pauses instead of sleeping; its
   * clock moves by {@code stepTime} at each read, so each step takes that long.
   */
  private void tick(
      final int blocksPerRoll, final Optional<Duration> rollPause, final Duration stepTime) {
    final AtomicLong now = new AtomicLong();
    migrator =
        new PbtMigrator(
            provider,
            blockchain,
            Optional.of(PBT_TIME),
            scheduler,
            Stream::empty,
            Optional.empty(),
            headers -> Map.of(),
            1,
            new PbtMigrator.Tuning(1, blocksPerRoll, rollPause, 1 << 20),
            Duration.ZERO,
            () -> now.addAndGet(stepTime.toMillis())) {
          @Override
          void pauseBetweenRolls(final Duration pause) {
            pauses.add(pause);
            pausedAt.add(cursor());
            onPause.run();
          }
        };
    migrator.start();
    final ArgumentCaptor<Runnable> tick = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler)
        .scheduleWithFixedDelay(tick.capture(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
    tick.getValue().run();
  }

  /** The number of the block the binary column holds. */
  private long cursor() {
    return storage
        .getWorldStateBlockHash(TrieBranchType.BINARY)
        .map(headers::get)
        .map(BlockHeader::getNumber)
        .orElse(-1L);
  }

  private Optional<Hash> binaryRoot() {
    return storage
        .getWorldStateRootHash(TrieBranchType.BINARY)
        .map(root -> Hash.wrap(Bytes32.wrap(root)));
  }

  private Optional<BlockHeader> canonical(final long number) {
    return number < chain.size() ? Optional.of(chain.get((int) number)) : Optional.empty();
  }

  /** The head's state, for the next block. */
  private BonsaiWorldState parentState() {
    final BlockHeader parent = chain.getLast();
    return (BonsaiWorldState)
        provider
            .getWorldState(
                WorldStateQueryParams.newBuilder()
                    .withBlockHeader(parent)
                    .withShouldWorldStateUpdateHead(true)
                    .withTimeStamp(parent.getTimestamp() + 1)
                    .build())
            .orElseThrow();
  }

  /** Applies {@code changes}, then seals and persists the next block of the chain. */
  private void block(final BonsaiWorldState worldState, final Consumer<WorldUpdater> changes) {
    final WorldUpdater updater = worldState.updater();
    changes.accept(updater);
    updater.commit();
    final BlockHeader parent = chain.isEmpty() ? null : chain.getLast();
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .number(chain.size())
            .parentHash(parent == null ? Hash.ZERO : parent.getHash())
            .timestamp(chain.size())
            .stateRoot(worldState.frontierRootHash())
            .buildHeader();
    headers.put(header.getHash(), header);
    chain.add(header);
    worldState.persist(header);
  }

  /** The binary root of the last block's state, built on a trie binary from genesis. */
  private static Hash binaryRootFromScratch() {
    final Blockchain other = mock(Blockchain.class);
    when(other.getChainHeadHeader())
        .thenReturn(new BlockHeaderTestFixture().number(0).timestamp(0).buildHeader());
    final BonsaiWorldState state =
        (BonsaiWorldState)
            newProvider(
                    (BonsaiWorldStateKeyValueStorage)
                        new KeyValueStorageProvider(
                                segments -> new SegmentedInMemoryKeyValueStorage(),
                                new InMemoryKeyValueStorage(),
                                new NoOpMetricsSystem())
                            .createWorldStateStorage(CONFIG),
                    other,
                    0L)
                .getWorldState();
    final WorldUpdater updater = state.updater();
    for (int i = 1; i <= BLOCKS; i++) {
      updater.createAccount(address(i), 0, Wei.of(i));
    }
    updater.commit();
    return state.frontierRootHash();
  }

  private static BonsaiWorldStateProvider newProvider(
      final BonsaiWorldStateKeyValueStorage storage,
      final Blockchain blockchain,
      final long pbtTime) {
    return new BonsaiWorldStateProvider(
        storage,
        blockchain,
        CONFIG.getExtraStorageConfiguration(),
        new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
        null,
        EvmConfiguration.DEFAULT,
        new BonsaiCodeCache(),
        Optional.empty(),
        Optional.of(pbtTime));
  }

  private static Address address(final int index) {
    return Address.fromHexString(String.format("0x%040x", 0x1000 + index));
  }
}
