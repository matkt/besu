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
package org.hyperledger.besu.plugin.services.storage.rocksdb.segmented;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDBException;

@ExtendWith(MockitoExtension.class)
public class OptimisticTransactionDBRocksDBColumnarKeyValueStorageTest
    extends RocksDBColumnarKeyValueStorageTest {

  @Override
  protected SegmentedKeyValueStorage createSegmentedStore() throws Exception {
    return new OptimisticRocksDBColumnarKeyValueStorage(
        new RocksDBConfigurationBuilder()
            .databaseDir(Files.createTempDirectory("segmentedStore"))
            .build(),
        Arrays.asList(TestSegment.DEFAULT, TestSegment.FOO, TestSegment.BAR),
        List.of(),
        new NoOpMetricsSystem(),
        RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
  }

  @Override
  protected SegmentedKeyValueStorage createSegmentedStore(
      final Path path,
      final List<SegmentIdentifier> segments,
      final List<SegmentIdentifier> ignorableSegments) {
    return new OptimisticRocksDBColumnarKeyValueStorage(
        new RocksDBConfigurationBuilder().databaseDir(path).build(),
        segments,
        ignorableSegments,
        new NoOpMetricsSystem(),
        RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
  }

  @Override
  protected SegmentedKeyValueStorage createSegmentedStore(
      final Path path,
      final MetricsSystem metricsSystem,
      final List<SegmentIdentifier> segments,
      final List<SegmentIdentifier> ignorableSegments) {
    return new OptimisticRocksDBColumnarKeyValueStorage(
        new RocksDBConfigurationBuilder().databaseDir(path).build(),
        segments,
        ignorableSegments,
        metricsSystem,
        RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
  }

  @Test
  public void snapshotMultigetReadsStableSnapshotState() throws Exception {
    final OptimisticRocksDBColumnarKeyValueStorage store =
        (OptimisticRocksDBColumnarKeyValueStorage) createSegmentedStore();

    final SegmentedKeyValueStorageTransaction initialTx = store.startTransaction();
    initialTx.put(TestSegment.FOO, bytesOf(1), bytesOf(10));
    initialTx.commit();

    final RocksDBColumnarKeyValueSnapshot snapshot = store.takeSnapshot();
    try {
      final SegmentedKeyValueStorageTransaction updateTx = store.startTransaction();
      updateTx.put(TestSegment.FOO, bytesOf(1), bytesOf(11));
      updateTx.put(TestSegment.FOO, bytesOf(2), bytesOf(20));
      updateTx.commit();

      final List<Optional<byte[]>> values =
          snapshot.multiget(TestSegment.FOO, List.of(bytesOf(1), bytesOf(2), bytesOf(1)));

      assertThat(values).hasSize(3);
      assertThat(values.get(0)).isPresent();
      assertThat(values.get(0).get()).isEqualTo(bytesOf(10));
      assertThat(values.get(1)).isEmpty();
      assertThat(values.get(2)).isPresent();
      assertThat(values.get(2).get()).isEqualTo(bytesOf(10));
    } finally {
      snapshot.close();
      store.close();
    }
  }

  @Test
  public void uncachedSnapshotReadsSeeTheSnapshotAndLeaveTheBlockCacheAsItWas() throws Exception {
    final OptimisticRocksDBColumnarKeyValueStorage store =
        (OptimisticRocksDBColumnarKeyValueStorage) createSegmentedStore();
    final ColumnFamilyHandle handle = store.safeColumnHandle(TestSegment.FOO);
    // Two values in blocks of their own, on disk.
    final byte[] large = new byte[64 << 10];
    final SegmentedKeyValueStorageTransaction initialTx = store.startTransaction();
    initialTx.put(TestSegment.FOO, bytesOf(1), large);
    initialTx.put(TestSegment.FOO, bytesOf(2), large);
    initialTx.commit();
    try (final FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
      store.getDB().flush(flush, handle);
    }

    final RocksDBColumnarKeyValueSnapshot snapshot = store.takeSnapshot();
    try {
      final SegmentedKeyValueStorageTransaction updateTx = store.startTransaction();
      updateTx.put(TestSegment.FOO, bytesOf(2), bytesOf(20));
      updateTx.commit();
      // Opens the file and caches the block of the first value.
      snapshot.get(TestSegment.FOO, bytesOf(1));
      final long cached = blockCacheUsage(store, handle);

      assertThat(snapshot.getUncached(TestSegment.FOO, bytesOf(2))).get().isEqualTo(large);
      try (final Stream<Pair<byte[], byte[]>> entries =
          snapshot.streamFromKeyUncached(TestSegment.FOO, bytesOf(2))) {
        assertThat(entries.map(Pair::getValue)).containsExactly(large);
      }
      assertThat(blockCacheUsage(store, handle)).isEqualTo(cached);

      snapshot.get(TestSegment.FOO, bytesOf(2));
      assertThat(blockCacheUsage(store, handle)).isGreaterThan(cached);
    } finally {
      snapshot.close();
      store.close();
    }
  }

  private static long blockCacheUsage(
      final OptimisticRocksDBColumnarKeyValueStorage store, final ColumnFamilyHandle handle)
      throws RocksDBException {
    return store.getDB().getLongProperty(handle, "rocksdb.block-cache-usage");
  }

  @Test
  public void batchTransactionCommitsWithoutCheckingConflicts() throws Exception {
    final SegmentedKeyValueStorage store = createSegmentedStore();
    final byte[] key = bytesFromHexString("0001");

    final SegmentedKeyValueStorageTransaction batch = store.startBatchTransaction();
    batch.put(TestSegment.FOO, key, bytesFromHexString("0FFF"));
    // the same key is written meanwhile, which fails an optimistic transaction
    final SegmentedKeyValueStorageTransaction other = store.startTransaction();
    other.put(TestSegment.FOO, key, bytesFromHexString("1337"));
    other.commit();
    batch.commit();

    assertThat(store.get(TestSegment.FOO, key)).contains(bytesFromHexString("0FFF"));
    store.close();
  }
}
