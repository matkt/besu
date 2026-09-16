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

import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SnappableKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBTransaction;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfiguration;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageTransactionValidatorDecorator;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Optimistic RocksDB Columnar key value storage */
public class OptimisticRocksDBColumnarKeyValueStorage extends RocksDBColumnarKeyValueStorage
    implements SnappableKeyValueStorage {
  private static final Logger LOG =
      LoggerFactory.getLogger(OptimisticRocksDBColumnarKeyValueStorage.class);

  private final OptimisticTransactionDB db;
  private final Object snapshotLock = new Object();
  private RocksDBColumnarKeyValueSnapshot trackedSnapshot;
  private final ExecutorService fcuCompactionExecutor =
      Executors.newSingleThreadExecutor(
          (ThreadFactory)
              r -> {
                final Thread thread = new Thread(r, "rocksdb-fcu-state-compact");
                thread.setDaemon(true);
                return thread;
              });

  /**
   * Instantiates a new Rocks db columnar key value optimistic storage.
   *
   * @param configuration the configuration
   * @param segments the segments
   * @param ignorableSegments the ignorable segments
   * @param metricsSystem the metrics system
   * @param rocksDBMetricsFactory the rocks db metrics factory
   * @throws StorageException the storage exception
   */
  public OptimisticRocksDBColumnarKeyValueStorage(
      final RocksDBConfiguration configuration,
      final List<SegmentIdentifier> segments,
      final List<SegmentIdentifier> ignorableSegments,
      final MetricsSystem metricsSystem,
      final RocksDBMetricsFactory rocksDBMetricsFactory)
      throws StorageException {
    super(configuration, segments, ignorableSegments, metricsSystem, rocksDBMetricsFactory);
    try {

      db =
          RocksDBOpener.openOptimisticTransactionDBWithWarning(
              options, configuration.getDatabaseDir().toString(), columnDescriptors, columnHandles);
      initMetrics();
      initColumnHandles();

    } catch (final RocksDBException e) {
      throw parseRocksDBException(e, segments, ignorableSegments);
    }
  }

  @Override
  RocksDB getDB() {
    return db;
  }

  /**
   * Start a transaction
   *
   * @return the new transaction started
   * @throws StorageException the storage exception
   */
  @Override
  public SegmentedKeyValueStorageTransaction startTransaction() throws StorageException {
    throwIfClosed();
    final WriteOptions writeOptions = new WriteOptions();
    writeOptions.setIgnoreMissingColumnFamilies(true);
    return new SegmentedKeyValueStorageTransactionValidatorDecorator(
        new RocksDBTransaction(
            this::safeColumnHandle, db.beginTransaction(writeOptions), writeOptions, this.metrics),
        this.closed::get);
  }

  @Override
  public SegmentedKeyValueStorageTransaction startLowPriorityTransaction() throws StorageException {
    throwIfClosed();
    final WriteOptions writeOptions = new WriteOptions();
    writeOptions.setIgnoreMissingColumnFamilies(true);
    writeOptions.setLowPri(true);
    return new SegmentedKeyValueStorageTransactionValidatorDecorator(
        new RocksDBTransaction(
            this::safeColumnHandle, db.beginTransaction(writeOptions), writeOptions, this.metrics),
        this.closed::get);
  }

  /**
   * Take snapshot RocksDb columnar key value snapshot.
   *
   * @return the RocksDb columnar key value snapshot
   * @throws StorageException the storage exception
   */
  @Override
  public RocksDBColumnarKeyValueSnapshot takeSnapshot() throws StorageException {
    throwIfClosed();
    synchronized (snapshotLock) {
      final int maxOpenSnapshots = configuration.getMaxOpenRocksDbSnapshots();
      if (maxOpenSnapshots > 0 && trackedSnapshot != null && !trackedSnapshot.isClosed()) {
        LOG.warn(
            "Closing previous RocksDB snapshot to enforce max-open-snapshots={}",
            maxOpenSnapshots);
        try {
          trackedSnapshot.close();
        } catch (final IOException e) {
          throw new StorageException(e);
        }
      }
      trackedSnapshot =
          new RocksDBColumnarKeyValueSnapshot(
              db, configuration.isReadCacheEnabledForSnapshots(), this::safeColumnHandle, metrics);
      return trackedSnapshot;
    }
  }

  /** Schedules Bonsai state column family compaction on a background thread (used after FCU). */
  public void scheduleCompactBonsaiStateColumnFamilies() {
    fcuCompactionExecutor.execute(
        () -> {
          try {
            compactBonsaiStateColumnFamilies();
          } catch (final Exception e) {
            LOG.warn("FCU-triggered RocksDB state compaction failed", e);
          }
        });
  }

  @Override
  public void close() {
    fcuCompactionExecutor.shutdownNow();
    super.close();
  }
}
