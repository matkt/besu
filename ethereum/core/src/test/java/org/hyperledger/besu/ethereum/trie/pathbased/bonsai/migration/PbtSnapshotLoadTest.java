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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.Eip8347Fixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.Eip8347Fixture.Artifacts;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.verify.Eip8347DualCheckVerifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.MigrationScopedWorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

/** Loading the PBT column into RocksDB: the snapshot load and the self-conversion. */
class PbtSnapshotLoadTest {

  private static final Bytes CODE = Bytes.fromHexString("0x" + "60016001".repeat(40));

  /** The time of the head, the anchor, in epoch millis. */
  private static final long HEAD_TIME = 1000L;

  @TempDir Path tmp;

  private RocksDbWorldStates databases;

  @BeforeEach
  void openDatabases() {
    databases = new RocksDbWorldStates(tmp);
  }

  @AfterEach
  void closeDatabases() throws Exception {
    databases.close();
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
  void filesAreAddedAFewAtATimeInLocationOrder() throws Exception {
    // Each ingestion stops the database's writes while it runs: it adds at most 4 files here.
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");

    final SortedFileLoad load = sortedFileLoad(fixture, artifacts, 1 << 10, 1 << 10, 4);
    assertThat(load.column()).isEqualTo(transactionalLoad(fixture, artifacts));
    assertThat(load.ingestions())
        .hasSize((load.files() + 3) / 4)
        .allSatisfy(files -> assertThat(files).hasSizeBetween(1, 4));
    // Files are numbered as they are written, in location order.
    assertThat(load.ingestions().stream().flatMap(List::stream).map(Path::getFileName))
        .hasSizeGreaterThan(4)
        .isSorted()
        .doesNotHaveDuplicates();
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
  void loadCommitsAtLowPriority() throws Exception {
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");
    final SegmentedKeyValueStorage database = spy(rocksDb("db"));
    final BonsaiWorldStateKeyValueStorage storage = worldStateStorage(database);
    final List<SegmentedKeyValueStorageTransaction> normal = recordTransactions(database, false);
    final List<SegmentedKeyValueStorageTransaction> lowPriority =
        recordTransactions(database, true);

    migrator(storage, artifacts, fixture.mptRoot()).tick();

    assertThat(storage.getWorldStateBlockHash(TrieBranchType.BINARY)).isPresent();
    // The import marker, the last nodes and the cursor: throttled behind block import's writes.
    assertThat(lowPriority).hasSize(2);
    lowPriority.forEach(transaction -> verify(transaction).commit());
    normal.forEach(transaction -> verify(transaction, never()).commit());
  }

  @Test
  void rejectedSnapshotLeavesNoNodeOnlyItsRefusal() throws Exception {
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
    assertRefused(storage, "MPT stateRoot mismatch");
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
  void errorWritingTheFilesFailsTheLoadBeforeAnythingIsAdded() throws Exception {
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");
    final SegmentedKeyValueStorage database = spy(rocksDb("db"));
    final BonsaiWorldStateKeyValueStorage storage = worldStateStorage(database);
    // An Error, not a RuntimeException, on the file thread: the nodes it held must not be lost.
    doThrow(new LinkageError("file writer failed"))
        .when(database)
        .createSortedFileWriter(any(), any());

    final Migrator migrator = migrator(storage, artifacts, fixture.mptRoot());
    migrator.tick();

    verify(database, never()).ingestSortedFiles(any(), any());
    verify(migrator.scheduler(), never()).shutdown();
    assertThat(storage.getWorldStateBlockHash(TrieBranchType.BINARY)).isEmpty();
    assertThat(column(database)).isEmpty();
    assertNoFileLeft();
  }

  @Test
  void closingTheNodeStopsALoadInProgress() throws Exception {
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");
    final SegmentedKeyValueStorage database = spy(rocksDb("db"));
    final BonsaiWorldStateKeyValueStorage storage = worldStateStorage(database);
    final Migrator migrator = migrator(storage, artifacts, fixture.mptRoot());
    final ScheduledExecutorService scheduler = migrator.scheduler();
    // shutdownNow interrupts the tick, which runs on this thread here.
    final Thread tickThread = Thread.currentThread();
    doAnswer(
            invocation -> {
              tickThread.interrupt();
              return List.of();
            })
        .when(scheduler)
        .shutdownNow();
    when(scheduler.awaitTermination(anyLong(), any())).thenReturn(true);
    // The node starts closing while the load writes its first file.
    doAnswer(
            invocation -> {
              migrator.migrator().close();
              return invocation.callRealMethod();
            })
        .doCallRealMethod()
        .when(database)
        .createSortedFileWriter(any(), any());

    migrator.tick();

    // Still interrupted, for the scheduler's thread to stop.
    assertThat(Thread.interrupted()).isTrue();
    final InOrder closing = inOrder(scheduler);
    closing.verify(scheduler).shutdownNow();
    closing.verify(scheduler).awaitTermination(anyLong(), any());
    // Stopped with the node: nothing added, no cursor, and the database still usable.
    verify(database, never()).ingestSortedFiles(any(), any());
    assertThat(storage.getWorldStateBlockHash(TrieBranchType.BINARY)).isEmpty();
    assertThat(column(database)).isEmpty();
    assertNoFileLeft();
    assertThat(database.isClosed()).isFalse();
    final SegmentedKeyValueStorageTransaction transaction = database.startTransaction();
    transaction.put(BINARY_TRIE_BRANCH_STORAGE, new byte[] {1}, new byte[] {2});
    transaction.commit();
    assertThat(database.get(BINARY_TRIE_BRANCH_STORAGE, new byte[] {1}).map(Bytes::wrap))
        .contains(Bytes.of(2));
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

  // ---- The self-conversion ----

  @Test
  void selfConversionWritesTheColumnTheSnapshotLoads() throws Exception {
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");
    final BonsaiWorldStateKeyValueStorage storage = flatDatabase(fixture);

    selfConvertingMigrator(storage, artifacts.preimages(), fixture.mptRoot(), () -> HEAD_TIME)
        .tick();

    assertThat(column(storage.getComposedWorldStateStorage()))
        .isEqualTo(transactionalLoad(fixture, artifacts));
    assertNoFileLeft();
  }

  @Test
  void selfConversionWaitsUntilTheNodeFollowsTheHead() throws Exception {
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");
    final BonsaiWorldStateKeyValueStorage storage = flatDatabase(fixture);
    final AtomicLong now = new AtomicLong(HEAD_TIME + PbtMigrator.MAX_HEAD_AGE.toMillis() + 1);
    final Migrator migrator =
        selfConvertingMigrator(storage, artifacts.preimages(), fixture.mptRoot(), now::get);

    migrator.tick();
    assertThat(column(storage.getComposedWorldStateStorage())).isEmpty();

    now.set(HEAD_TIME);
    migrator.tick();
    assertThat(storage.getWorldStateBlockHash(TrieBranchType.BINARY))
        .contains(migrator.anchor().getBlockHash());
  }

  @Test
  void selfConversionWaitsForTheFinalizedBlockToBeCloseToTheHead() throws Exception {
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");
    final BonsaiWorldStateKeyValueStorage storage = flatDatabase(fixture);
    final BlockHeader head =
        new BlockHeaderTestFixture()
            .number(1 + PbtMigrator.MAX_FINALITY_LAG + 1)
            .timestamp(HEAD_TIME / 1000)
            .buildHeader();

    final Migrator migrator =
        started(
            storage,
            PbtMigrator.SnapshotBootstrap.selfConversion(
                artifacts.preimages(), migratorAnchor(fixture.mptRoot()), workDir()),
            anchorHeader(fixture.mptRoot()),
            head,
            Duration.ZERO,
            () -> HEAD_TIME);
    migrator.tick();

    verify(migrator.scheduler(), never()).shutdown();
    assertThat(column(storage.getComposedWorldStateStorage())).isEmpty();
  }

  @Test
  void selfConversionStopsOnAKeyWithoutPreimage() throws Exception {
    final Eip8347Fixture fixture = fixture();
    final BonsaiWorldStateKeyValueStorage storage = flatDatabase(fixture);
    final Path preimages = tmp.resolve("partial.pre");
    Eip8347PreimageFile.write(
        preimages, fixture.preimages().subList(1, fixture.preimages().size()));

    final Migrator migrator =
        selfConvertingMigrator(storage, preimages, fixture.mptRoot(), () -> HEAD_TIME);
    migrator.tick();

    verify(migrator.scheduler()).shutdown();
    assertRefused(storage, "no preimage for flat key");
    assertNoFileLeft();
  }

  /** Only the import marker and the refusal reason are left, for the progress report. */
  private static void assertRefused(
      final BonsaiWorldStateKeyValueStorage storage, final String reason) {
    assertThat(column(storage.getComposedWorldStateStorage()))
        .extracting(Map.Entry::getKey)
        .containsExactlyInAnyOrder(
            Bytes.wrap(BonsaiWorldStateKeyValueStorage.PBT_IMPORTING_KEY),
            Bytes.wrap(BonsaiWorldStateKeyValueStorage.PBT_IMPORT_FAILED_KEY));
    assertThat(storage.getPbtImportFailure())
        .hasValueSatisfying(r -> assertThat(r).contains(reason));
  }

  // ---- Without a snapshot ----

  @Test
  void aStartWithoutSnapshotWritesNothingSoALaterSnapshotLoads() throws Exception {
    final Eip8347Fixture fixture = fixture();
    final Artifacts artifacts = fixture.write(tmp, "state");
    final BonsaiWorldStateKeyValueStorage storage = worldStateStorage(rocksDb("db"));
    final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    startWithoutSnapshot(storage, scheduler).run();

    assertThat(storage.getWorldStateBlockHash(TrieBranchType.BINARY)).isEmpty();
    verify(scheduler).shutdown();

    final Migrator restarted = migrator(storage, artifacts, fixture.mptRoot());
    restarted.tick();

    assertThat(storage.getWorldStateBlockHash(TrieBranchType.BINARY))
        .contains(restarted.anchor().getBlockHash());
    assertThat(column(storage.getComposedWorldStateStorage()))
        .isEqualTo(transactionalLoad(fixture, artifacts));
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

  /** A column loaded through sorted files, and the files each ingestion added. */
  private record SortedFileLoad(List<Map.Entry<Bytes, Bytes>> column, List<List<Path>> ingestions) {
    int files() {
      return ingestions.stream().mapToInt(List::size).sum();
    }
  }

  /** The column loaded through sorted files; {@code fileBytes} 0 keeps the default. */
  private SortedFileLoad sortedFileLoad(
      final Eip8347Fixture fixture,
      final Artifacts artifacts,
      final long windowBytes,
      final long fileBytes)
      throws Exception {
    return sortedFileLoad(
        fixture,
        artifacts,
        windowBytes,
        fileBytes,
        SnapshotColumnWriter.DEFAULT_FILES_PER_INGESTION);
  }

  private SortedFileLoad sortedFileLoad(
      final Eip8347Fixture fixture,
      final Artifacts artifacts,
      final long windowBytes,
      final long fileBytes,
      final int filesPerIngestion)
      throws Exception {
    final SegmentedKeyValueStorage database = spy(rocksDb("sorted"));
    final BonsaiWorldStateKeyValueStorage storage = worldStateStorage(database);
    try (final SnapshotColumnWriter writer =
        new SnapshotColumnWriter(
            storage,
            workDir(),
            windowBytes,
            fileBytes == 0 ? SnapshotColumnWriter.DEFAULT_FILE_BYTES : fileBytes,
            filesPerIngestion)) {
      final Eip8347DualCheckVerifier.Loaded loaded =
          Eip8347DualCheckVerifier.verifyAndLoad(
              artifacts.snapshot(), artifacts.preimages(), fixture.mptRoot(), workDir(), writer);
      writer.finish(loaded.pbtRoot(), migratorAnchor(fixture.mptRoot()));
    }
    assertNoFileLeft();
    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<Path>> files = ArgumentCaptor.forClass(List.class);
    verify(database, atLeastOnce())
        .ingestSortedFiles(eq(BINARY_TRIE_BRANCH_STORAGE), files.capture());
    return new SortedFileLoad(
        column(database), files.getAllValues().stream().<List<Path>>map(List::copyOf).toList());
  }

  /** The column as the transactional load wrote it: every node, then the cursor, in a commit. */
  private List<Map.Entry<Bytes, Bytes>> transactionalLoad(
      final Eip8347Fixture fixture, final Artifacts artifacts) throws Exception {
    final SegmentedKeyValueStorage database = rocksDb("transactional");
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

  /** The transactions {@code database} starts from now on, of one priority, as spies. */
  private static List<SegmentedKeyValueStorageTransaction> recordTransactions(
      final SegmentedKeyValueStorage database, final boolean lowPriority) {
    final List<SegmentedKeyValueStorageTransaction> started = new ArrayList<>();
    final SegmentedKeyValueStorage stub =
        doAnswer(
                invocation -> {
                  final SegmentedKeyValueStorageTransaction transaction =
                      spy((SegmentedKeyValueStorageTransaction) invocation.callRealMethod());
                  started.add(transaction);
                  return transaction;
                })
            .when(database);
    if (lowPriority) {
      stub.startLowPriorityTransaction();
    } else {
      stub.startTransaction();
    }
    return started;
  }

  private static List<Map.Entry<Bytes, Bytes>> column(final SegmentedKeyValueStorage database) {
    try (final Stream<org.apache.commons.lang3.tuple.Pair<byte[], byte[]>> entries =
        database.stream(BINARY_TRIE_BRANCH_STORAGE)) {
      return entries.map(e -> Map.entry(Bytes.wrap(e.getKey()), Bytes.wrap(e.getValue()))).toList();
    }
  }

  private SegmentedKeyValueStorage rocksDb(final String name) {
    return databases.database(name);
  }

  private static BonsaiWorldStateKeyValueStorage worldStateStorage(
      final SegmentedKeyValueStorage database) {
    return RocksDbWorldStates.worldStateStorage(database);
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
    return started(
        storage,
        new PbtMigrator.SnapshotBootstrap(
            artifacts.snapshot(),
            artifacts.preimages(),
            migratorAnchor(anchorStateRoot),
            workDir()),
        anchorHeader(anchorStateRoot),
        anchorHeader(anchorStateRoot),
        retryDelay,
        System::currentTimeMillis);
  }

  /** A self-converting migrator at {@code clock}; the anchor is its finalized head. */
  private Migrator selfConvertingMigrator(
      final BonsaiWorldStateKeyValueStorage storage,
      final Path preimages,
      final Bytes32 anchorStateRoot,
      final LongSupplier clock) {
    return started(
        storage,
        PbtMigrator.SnapshotBootstrap.selfConversion(
            preimages, migratorAnchor(anchorStateRoot), workDir()),
        anchorHeader(anchorStateRoot),
        anchorHeader(anchorStateRoot),
        Duration.ZERO,
        clock);
  }

  /** A started migrator: {@code anchor} is canonical and finalized, {@code head} the head. */
  private static Migrator started(
      final BonsaiWorldStateKeyValueStorage storage,
      final PbtMigrator.SnapshotBootstrap bootstrap,
      final BlockHeader anchor,
      final BlockHeader head,
      final Duration retryDelay,
      final LongSupplier clock) {
    final Blockchain blockchain = mock(Blockchain.class);
    when(blockchain.getBlockHeader(any(Hash.class))).thenReturn(Optional.of(anchor));
    when(blockchain.getBlockHashByNumber(anyLong())).thenReturn(Optional.of(anchor.getHash()));
    when(blockchain.getFinalized()).thenReturn(Optional.of(anchor.getHash()));
    when(blockchain.getChainHeadHeader()).thenReturn(head);
    final BonsaiWorldStateProvider provider = mock(BonsaiWorldStateProvider.class);
    when(provider.getWorldStateKeyValueStorage()).thenReturn(storage);
    when(provider.getBinaryColumnOwnership()).thenReturn(new PbtColumnOwnership(storage));
    final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    final PbtMigrator migrator =
        new PbtMigrator(
            provider,
            blockchain,
            Optional.of(Long.MAX_VALUE),
            scheduler,
            Stream::empty,
            Optional.of(bootstrap),
            false,
            headers -> Map.of(),
            1,
            // Two threads, and sorts that spill every few records.
            new PbtMigrator.Tuning(2, 128, Optional.of(Duration.ZERO), 1 << 10),
            retryDelay,
            clock);
    migrator.start();
    final ArgumentCaptor<Runnable> tick = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler)
        .scheduleWithFixedDelay(tick.capture(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
    return new Migrator(migrator, scheduler, tick.getValue(), anchor);
  }

  /** A database whose full flat database holds {@code fixture} at the anchor. */
  private BonsaiWorldStateKeyValueStorage flatDatabase(final Eip8347Fixture fixture) {
    final BonsaiWorldStateKeyValueStorage storage = worldStateStorage(rocksDb("db"));
    storage.upgradeToFullFlatDbMode();
    fixture.writeFlatDatabase(storage, migratorAnchor(fixture.mptRoot()));
    return storage;
  }

  /** Starts a migrator with no snapshot and no start from genesis; returns its scheduled tick. */
  private static Runnable startWithoutSnapshot(
      final BonsaiWorldStateKeyValueStorage storage, final ScheduledExecutorService scheduler) {
    final BonsaiWorldStateProvider provider = mock(BonsaiWorldStateProvider.class);
    when(provider.getWorldStateKeyValueStorage()).thenReturn(storage);
    when(provider.getBinaryColumnOwnership()).thenReturn(new PbtColumnOwnership(storage));
    new PbtMigrator(
            provider,
            mock(Blockchain.class),
            Optional.of(Long.MAX_VALUE),
            scheduler,
            Stream::empty,
            Optional.empty(),
            false,
            headers -> Map.of(),
            1,
            new PbtMigrator.Tuning(1, 128, Optional.of(Duration.ZERO), 1 << 20),
            Duration.ZERO)
        .start();
    final ArgumentCaptor<Runnable> tick = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler)
        .scheduleWithFixedDelay(tick.capture(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
    return tick.getValue();
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
