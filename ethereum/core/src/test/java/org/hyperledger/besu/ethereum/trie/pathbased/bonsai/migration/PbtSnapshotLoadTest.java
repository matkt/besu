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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.BINARY_TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.Eip8347Fixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.Eip8347Fixture.Artifacts;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.verify.Eip8347DualCheckVerifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.MigrationScopedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.OptimisticRocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

/** The snapshot load into RocksDB through sorted files, alone and through the migrator. */
class PbtSnapshotLoadTest {

  private static final Bytes CODE = Bytes.fromHexString("0x" + "60016001".repeat(40));

  @TempDir Path tmp;

  private final List<SegmentedKeyValueStorage> opened = new ArrayList<>();

  @AfterEach
  void closeDatabases() throws Exception {
    for (final SegmentedKeyValueStorage storage : opened) {
      storage.close();
    }
  }

  // ---- The column writer ----

  @Test
  void sortedFilesLoadTheSameColumnAsTransactions() throws Exception {
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");

    final SortedFileLoad load =
        sortedFileLoad(fixture, artifacts, SnapshotColumnWriter.DEFAULT_WINDOW_BYTES, 0);
    assertThat(load.column()).isEqualTo(transactionalLoad(fixture, artifacts));
    assertThat(load.files()).isEqualTo(1);
  }

  @Test
  void nodesBelowAWrittenLocationAreWrittenWithTheCursor() throws Exception {
    // A window of 1 KiB writes nodes before their branches arrive, and spreads them over files.
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");

    final SortedFileLoad load = sortedFileLoad(fixture, artifacts, 1 << 10, 1 << 10);
    assertThat(load.column()).isEqualTo(transactionalLoad(fixture, artifacts));
    assertThat(load.files()).isGreaterThan(1);
  }

  @Test
  void emptySnapshotLoadsTheEmptyRoot() throws Exception {
    final Eip8347Fixture fixture = Eip8347Fixture.builder().build();
    final Artifacts artifacts = fixture.write(tmp, "empty");

    final List<Map.Entry<Bytes, Bytes>> column =
        sortedFileLoad(fixture, artifacts, SnapshotColumnWriter.DEFAULT_WINDOW_BYTES, 0).column();
    assertThat(column).isEqualTo(transactionalLoad(fixture, artifacts));
    assertThat(column.getFirst()).isEqualTo(Map.entry(Bytes.EMPTY, Bytes.EMPTY));
  }

  // ---- The migrator ----

  @Test
  void acceptedSnapshotIsAddedThenItsCursorWritten() throws Exception {
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");
    final BonsaiWorldStateKeyValueStorage storage = worldStateStorage(rocksDb("db"));

    final Migrator migrator = migrator(storage, artifacts, fixture.mptRoot());
    migrator.tick();

    assertThat(storage.getWorldStateBlockHash(TrieBranchType.BINARY))
        .contains(migrator.anchor().getBlockHash());
    assertThat(column(storage.getComposedWorldStateStorage()))
        .isEqualTo(transactionalLoad(fixture, artifacts));
    assertNoFileLeft();
  }

  @Test
  void rejectedSnapshotLeavesTheDatabaseUntouched() throws Exception {
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");
    final SegmentedKeyValueStorage database = spy(rocksDb("db"));
    final BonsaiWorldStateKeyValueStorage storage = worldStateStorage(database);

    // Check 2 rejects it, after the PBT pass has written every file.
    final Migrator migrator = migrator(storage, artifacts, Bytes32.repeat((byte) 1));
    clearInvocations(database);
    migrator.tick();

    verify(migrator.scheduler()).shutdown();
    verify(database, never()).ingestSortedFiles(any(), any());
    verify(database, never()).startTransaction();
    assertThat(column(database)).isEmpty();
    assertNoFileLeft();
  }

  @Test
  void failureAfterTheFilesWereAddedEmptiesTheColumnAndRetries() throws Exception {
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");
    final SegmentedKeyValueStorage database = spy(rocksDb("db"));
    final BonsaiWorldStateKeyValueStorage storage = worldStateStorage(database);
    // The first load adds its files, then fails before the cursor.
    doAnswer(
            invocation -> {
              invocation.callRealMethod();
              throw new StorageException("disk full");
            })
        .doCallRealMethod()
        .when(database)
        .ingestSortedFiles(any(), any());

    final Migrator migrator = migrator(storage, artifacts, fixture.mptRoot());
    migrator.tick();

    verify(migrator.scheduler(), never()).shutdown();
    assertThat(storage.getWorldStateBlockHash(TrieBranchType.BINARY)).isEmpty();
    assertThat(column(database)).isEmpty();
    assertNoFileLeft();

    migrator.tick();

    assertThat(storage.getWorldStateBlockHash(TrieBranchType.BINARY))
        .contains(migrator.anchor().getBlockHash());
    assertThat(column(database)).isEqualTo(transactionalLoad(fixture, artifacts));
    assertNoFileLeft();
  }

  @Test
  void failedLoadWaitsBeforeTryingAgain() throws Exception {
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");
    final SegmentedKeyValueStorage database = spy(rocksDb("db"));
    final BonsaiWorldStateKeyValueStorage storage = worldStateStorage(database);
    doThrow(new StorageException("No space left on device"))
        .when(database)
        .ingestSortedFiles(any(), any());

    final Migrator migrator =
        migrator(storage, artifacts, fixture.mptRoot(), PbtMigrator.SNAPSHOT_RETRY_DELAY);
    migrator.tick();
    migrator.tick();

    verify(database).ingestSortedFiles(any(), any());
    verify(migrator.scheduler(), never()).shutdown();
    assertThat(column(database)).isEmpty();
    assertNoFileLeft();
  }

  @Test
  void closingTheNodeWaitsForALoadInProgress() throws Exception {
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");
    final SegmentedKeyValueStorage database = spy(rocksDb("db"));
    final BonsaiWorldStateKeyValueStorage storage = worldStateStorage(database);
    // The load stops at its first file until the node starts closing.
    final CountDownLatch writing = new CountDownLatch(1);
    final CountDownLatch closing = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              writing.countDown();
              closing.await();
              return invocation.callRealMethod();
            })
        .when(database)
        .createSortedFileWriter(any(), any());
    final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    final PbtMigrator migrator =
        newMigrator(storage, artifacts, fixture.mptRoot(), scheduler, Duration.ZERO);
    migrator.start();
    assertThat(writing.await(1, TimeUnit.MINUTES)).isTrue();

    final CompletableFuture<Void> closed = CompletableFuture.runAsync(migrator::close);
    while (!scheduler.isShutdown()) {
      Thread.sleep(10);
    }
    closing.countDown();
    closed.get(1, TimeUnit.MINUTES);

    // close() returned once the load was over, with the database still open.
    assertThat(scheduler.isTerminated()).isTrue();
    assertThat(database.isClosed()).isFalse();
    assertThat(storage.getWorldStateBlockHash(TrieBranchType.BINARY))
        .contains(anchorHeader(fixture.mptRoot()).getBlockHash());
    assertNoFileLeft();
  }

  @Test
  void crashBeforeTheCursorEmptiesAndReloadsTheColumn() throws Exception {
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");
    final BonsaiWorldStateKeyValueStorage storage = worldStateStorage(rocksDb("db"));
    migrator(storage, artifacts, fixture.mptRoot()).tick();

    // Crashed after the files were added, before the cursor: a stray node and file left too.
    final BonsaiWorldStateKeyValueStorage.Updater updater =
        new MigrationScopedWorldStateKeyValueStorage(storage).updater();
    updater.getWorldStateTransaction().remove(BINARY_TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY);
    updater.getWorldStateTransaction().remove(BINARY_TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY);
    updater
        .getWorldStateTransaction()
        .put(BINARY_TRIE_BRANCH_STORAGE, new byte[] {0, (byte) 0xff}, new byte[] {1});
    updater.commit();
    final Path strayFile = workDir().resolve("pbt-column").resolve("binary-trie-000000.sst");
    Files.createDirectories(strayFile.getParent());
    Files.write(strayFile, new byte[] {1, 2, 3});

    final Migrator restarted = migrator(storage, artifacts, fixture.mptRoot());
    restarted.tick();

    assertThat(storage.getWorldStateBlockHash(TrieBranchType.BINARY))
        .contains(restarted.anchor().getBlockHash());
    assertThat(column(storage.getComposedWorldStateStorage()))
        .isEqualTo(transactionalLoad(fixture, artifacts));
    assertNoFileLeft();
  }

  // ---- Helpers ----

  /** A state with shared code, a delegation, header slots and a storage of several groups. */
  private static Eip8347Fixture fixture() {
    final Eip8347Fixture.Builder builder = Eip8347Fixture.builder();
    for (int i = 1; i <= 40; i++) {
      builder.eoa(address(i), i, Wei.of(1000L * i), Map.of());
    }
    final Map<UInt256, UInt256> storage = new HashMap<>();
    for (int slot = 0; slot < 700; slot++) {
      storage.put(UInt256.valueOf(slot * 13L), UInt256.valueOf(slot + 1L));
    }
    return builder
        .contract(address(0x100), 1L, Wei.ONE, CODE, storage)
        .contract(address(0x101), 2L, Wei.ONE, CODE, Map.of(UInt256.ONE, UInt256.ONE))
        .contract(
            address(0x102),
            3L,
            Wei.ONE,
            Bytes.concatenate(Bytes.fromHexString("0xef0100"), address(7).getBytes()),
            Map.of())
        .build();
  }

  /** A column loaded through sorted files, and how many files it took. */
  private record SortedFileLoad(List<Map.Entry<Bytes, Bytes>> column, int files) {}

  /** The column loaded through sorted files; {@code fileBytes} 0 keeps the default. */
  private SortedFileLoad sortedFileLoad(
      final Eip8347Fixture fixture,
      final Artifacts artifacts,
      final long windowBytes,
      final long fileBytes)
      throws Exception {
    final SegmentedKeyValueStorage database = spy(rocksDb("sorted"));
    final BonsaiWorldStateKeyValueStorage storage = worldStateStorage(database);
    try (final SnapshotColumnWriter writer =
        new SnapshotColumnWriter(
            storage,
            workDir(),
            windowBytes,
            fileBytes == 0 ? SnapshotColumnWriter.DEFAULT_FILE_BYTES : fileBytes)) {
      final Eip8347DualCheckVerifier.Loaded loaded =
          Eip8347DualCheckVerifier.verifyAndLoad(
              artifacts.snapshot(), artifacts.preimages(), fixture.mptRoot(), workDir(), writer);
      writer.finish(loaded.pbtRoot(), migratorAnchor(fixture.mptRoot()));
    }
    assertNoFileLeft();
    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<Path>> files = ArgumentCaptor.forClass(List.class);
    verify(database).ingestSortedFiles(eq(BINARY_TRIE_BRANCH_STORAGE), files.capture());
    return new SortedFileLoad(column(database), files.getValue().size());
  }

  /** The column as the transactional load wrote it: every node, then the cursor, in a commit. */
  private List<Map.Entry<Bytes, Bytes>> transactionalLoad(
      final Eip8347Fixture fixture, final Artifacts artifacts) throws Exception {
    final SegmentedKeyValueStorage database = rocksDb("transactional-" + opened.size());
    final BonsaiWorldStateKeyValueStorage.Updater updater =
        new MigrationScopedWorldStateKeyValueStorage(worldStateStorage(database)).updater();
    final Eip8347DualCheckVerifier.Loaded loaded =
        Eip8347DualCheckVerifier.verifyAndLoad(
            artifacts.snapshot(),
            artifacts.preimages(),
            fixture.mptRoot(),
            tmp.resolve("transactional-work"),
            (location, hash, value) ->
                updater.putTrieNode(TrieBranchType.BINARY, location, hash, value));
    PbtMigrator.putCursor(updater, loaded.pbtRoot(), migratorAnchor(fixture.mptRoot()));
    updater.commit();
    return column(database);
  }

  private static List<Map.Entry<Bytes, Bytes>> column(final SegmentedKeyValueStorage database) {
    try (final Stream<org.apache.commons.lang3.tuple.Pair<byte[], byte[]>> entries =
        database.stream(BINARY_TRIE_BRANCH_STORAGE)) {
      return entries.map(e -> Map.entry(Bytes.wrap(e.getKey()), Bytes.wrap(e.getValue()))).toList();
    }
  }

  private SegmentedKeyValueStorage rocksDb(final String name) {
    final SegmentedKeyValueStorage database =
        new OptimisticRocksDBColumnarKeyValueStorage(
            new RocksDBConfigurationBuilder().databaseDir(tmp.resolve(name)).build(),
            Arrays.stream(KeyValueSegmentIdentifier.values())
                .filter(segment -> segment.includeInDatabaseFormat(DataStorageFormat.BONSAI))
                .<SegmentIdentifier>map(segment -> segment)
                .toList(),
            List.of(),
            new NoOpMetricsSystem(),
            RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
    opened.add(database);
    return database;
  }

  private static BonsaiWorldStateKeyValueStorage worldStateStorage(
      final SegmentedKeyValueStorage database) {
    return (BonsaiWorldStateKeyValueStorage)
        new KeyValueStorageProvider(
                segments -> database, new InMemoryKeyValueStorage(), new NoOpMetricsSystem())
            .createWorldStateStorage(DataStorageConfiguration.DEFAULT_BONSAI_PBT_CONFIG);
  }

  /** A migrator with no cursor yet, its anchor canonical and finalized, and its first tick. */
  private record Migrator(
      PbtMigrator migrator,
      ScheduledExecutorService scheduler,
      Runnable scheduledTick,
      BlockHeader anchor) {
    void tick() {
      scheduledTick.run();
    }
  }

  private Migrator migrator(
      final BonsaiWorldStateKeyValueStorage storage,
      final Artifacts artifacts,
      final Bytes32 anchorStateRoot) {
    return migrator(storage, artifacts, anchorStateRoot, Duration.ZERO);
  }

  private Migrator migrator(
      final BonsaiWorldStateKeyValueStorage storage,
      final Artifacts artifacts,
      final Bytes32 anchorStateRoot,
      final Duration retryDelay) {
    final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    final PbtMigrator migrator =
        newMigrator(storage, artifacts, anchorStateRoot, scheduler, retryDelay);
    migrator.start();
    final ArgumentCaptor<Runnable> tick = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler)
        .scheduleWithFixedDelay(tick.capture(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
    return new Migrator(migrator, scheduler, tick.getValue(), anchorHeader(anchorStateRoot));
  }

  private PbtMigrator newMigrator(
      final BonsaiWorldStateKeyValueStorage storage,
      final Artifacts artifacts,
      final Bytes32 anchorStateRoot,
      final ScheduledExecutorService scheduler,
      final Duration retryDelay) {
    final BlockHeader anchor = anchorHeader(anchorStateRoot);
    final Blockchain blockchain = mock(Blockchain.class);
    when(blockchain.getBlockHeader(any(Hash.class))).thenReturn(Optional.of(anchor));
    when(blockchain.getBlockHashByNumber(anyLong())).thenReturn(Optional.of(anchor.getHash()));
    when(blockchain.getFinalized()).thenReturn(Optional.of(anchor.getHash()));
    when(blockchain.getChainHeadHeader()).thenReturn(anchor);
    final BonsaiWorldStateProvider provider = mock(BonsaiWorldStateProvider.class);
    when(provider.getWorldStateKeyValueStorage()).thenReturn(storage);
    when(provider.getBinaryColumnOwnership()).thenReturn(new PbtColumnOwnership(storage));
    return new PbtMigrator(
        provider,
        blockchain,
        Optional.of(Long.MAX_VALUE),
        scheduler,
        Stream::empty,
        Optional.of(
            new PbtMigrator.SnapshotBootstrap(
                artifacts.snapshot(), artifacts.preimages(), anchor.getHash(), workDir())),
        headers -> Map.of(),
        1,
        retryDelay);
  }

  private static BlockHeader anchorHeader(final Bytes32 stateRoot) {
    return new BlockHeaderTestFixture()
        .number(1)
        .timestamp(1)
        .stateRoot(Hash.wrap(stateRoot))
        .buildHeader();
  }

  private static Hash migratorAnchor(final Bytes32 stateRoot) {
    return anchorHeader(stateRoot).getHash();
  }

  private Path workDir() {
    return PbtMigrator.SnapshotBootstrap.workDir(tmp);
  }

  private void assertNoFileLeft() throws Exception {
    if (Files.exists(workDir())) {
      try (final Stream<Path> files = Files.walk(workDir())) {
        assertThat(files.filter(Files::isRegularFile)).isEmpty();
      }
    }
  }

  private static Address address(final int index) {
    return Address.fromHexString(String.format("0x%040x", index));
  }
}
