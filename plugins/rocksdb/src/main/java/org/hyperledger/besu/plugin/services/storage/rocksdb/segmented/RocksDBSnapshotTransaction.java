/*
 * Copyright Hyperledger Besu Contributors.
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
 *
 */
package org.hyperledger.besu.plugin.services.storage.rocksdb.segmented;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetrics;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbIterator;

import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBSnapshotTransaction implements KeyValueStorageTransaction, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(RocksDBSnapshotTransaction.class);
  private static final String NO_SPACE_LEFT_ON_DEVICE = "No space left on device";

  private final RocksDBMetrics metrics;
  private final OptimisticTransactionDB db;
  private final ColumnFamilyHandle columnFamilyHandle;
  private final Transaction snapTx;
  private final RocksDBSnapshot snapshot;
  private final WriteOptions writeOptions;
  private final ReadOptions readOptions;

  RocksDBSnapshotTransaction(
      final OptimisticTransactionDB db,
      final ColumnFamilyHandle columnFamilyHandle,
      final RocksDBMetrics metrics) {
    this.metrics = metrics;
    this.db = db;
    this.columnFamilyHandle = columnFamilyHandle;
    this.snapshot = new RocksDBSnapshot(db);
    this.writeOptions = new WriteOptions();
    this.snapTx = db.beginTransaction(writeOptions);
    this.readOptions = new ReadOptions().setSnapshot(snapshot.markAndUseSnapshot());
  }

  private RocksDBSnapshotTransaction(
      final OptimisticTransactionDB db,
      final ColumnFamilyHandle columnFamilyHandle,
      final RocksDBMetrics metrics,
      final RocksDBSnapshot snapshot,
      final Transaction snapTx,
      final ReadOptions readOptions) {
    this.metrics = metrics;
    this.db = db;
    this.columnFamilyHandle = columnFamilyHandle;
    this.snapshot = snapshot;
    this.writeOptions = new WriteOptions();
    this.readOptions = readOptions;
    this.snapTx = snapTx;
  }

  public Optional<byte[]> get(final byte[] key) {
    try (final OperationTimer.TimingContext ignored = metrics.getReadLatency().startTimer()) {
      return Optional.ofNullable(snapTx.get(columnFamilyHandle, readOptions, key));
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void put(final byte[] key, final byte[] value) {
    try (final OperationTimer.TimingContext ignored = metrics.getWriteLatency().startTimer()) {
      snapTx.put(columnFamilyHandle, key, value);
    } catch (final RocksDBException e) {
      if (e.getMessage().contains(NO_SPACE_LEFT_ON_DEVICE)) {
        LOG.error(e.getMessage());
        System.exit(0);
      }
      throw new StorageException(e);
    }
  }

  @Override
  public void remove(final byte[] key) {
    try (final OperationTimer.TimingContext ignored = metrics.getRemoveLatency().startTimer()) {
      snapTx.delete(columnFamilyHandle, key);
    } catch (final RocksDBException e) {
      if (e.getMessage().contains(NO_SPACE_LEFT_ON_DEVICE)) {
        LOG.error(e.getMessage());
        System.exit(0);
      }
      throw new StorageException(e);
    }
  }

  public Optional<Pair<byte[], byte[]>> getMoreClosedByPrefix(final Bytes prefix) {
    final RocksIterator rocksIterator = snapTx.getIterator(readOptions, columnFamilyHandle);
    // System.out.println("seek for p rev snapshot "+prefix);
    rocksIterator.seekForPrev(prefix.toArrayUnsafe());
    final RocksDbIterator rocksDbKeyIterator = RocksDbIterator.create(rocksIterator);
    try {
      if (rocksDbKeyIterator.hasNext()) {
        final Pair<byte[], byte[]> next = rocksDbKeyIterator.next();
        final Bytes key = Bytes.wrap(next.getKey());
        // System.out.println("-> "+prefix+" "+key);
        if (key.commonPrefixLength(prefix) == key.size()) {
          return Optional.of(next);
        }
      }
      return Optional.empty();
    } finally {
      rocksDbKeyIterator.close();
      rocksIterator.close();
    }
  }

  public Optional<Pair<Bytes, Bytes>> getNearestKey(final Bytes key) {
    final RocksIterator rocksIterator = snapTx.getIterator(readOptions, columnFamilyHandle);
    // System.out.println("seek for p rev "+prefix);
    rocksIterator.seekForPrev(key.toArrayUnsafe());
    final RocksDbIterator rocksDbKeyIterator = RocksDbIterator.create(rocksIterator);
    try {
      if (rocksDbKeyIterator.hasNext()) {
        final Pair<byte[], byte[]> next = rocksDbKeyIterator.next();
        return Optional.of(Pair.of(Bytes.wrap(next.getKey()), Bytes.wrap(next.getValue())));
      }
      return Optional.empty();
    } finally {
      rocksDbKeyIterator.close();
      rocksIterator.close();
    }
  }

  public TreeMap<Bytes, Bytes> getByPrefix(final Bytes prefix) {
    final RocksIterator rocksIterator = snapTx.getIterator(readOptions, columnFamilyHandle);
    rocksIterator.seek(prefix.toArrayUnsafe());
    final RocksDbIterator rocksDbKeyIterator = RocksDbIterator.create(rocksIterator);
    try {
      final TreeMap<Bytes, Bytes> res = new TreeMap<>();
      while (rocksDbKeyIterator.hasNext()) {
        final Pair<byte[], byte[]> next = rocksDbKeyIterator.next();
        final Bytes key = Bytes.wrap(next.getKey());
        if (key.commonPrefixLength(prefix) == prefix.size()) {
          res.put(key, Bytes.of(next.getValue()));
        } else {
          return res;
        }
      }
      return res;
    } finally {
      rocksDbKeyIterator.close();
      rocksIterator.close();
    }
  }

  public TreeMap<Bytes, Bytes> getInRange(final Bytes startKeyHash, final Bytes endKeyHash) {
    final RocksIterator rocksIterator = snapTx.getIterator(readOptions, columnFamilyHandle);
    rocksIterator.seek(startKeyHash.toArrayUnsafe());
    final RocksDbIterator rocksDbKeyIterator = RocksDbIterator.create(rocksIterator);
    try {
      final TreeMap<Bytes, Bytes> res = new TreeMap<>();
      while (rocksDbKeyIterator.hasNext()) {
        final Pair<byte[], byte[]> next = rocksDbKeyIterator.next();
        final Bytes key = Bytes.wrap(next.getKey());
        if (key.compareTo(startKeyHash) >= 0) {
          if (key.compareTo(endKeyHash) <= 0) {
            res.put(key, Bytes.of(next.getValue()));
          } else {
            return res;
          }
        }
      }
      return res;
    } finally {
      rocksDbKeyIterator.close();
      rocksIterator.close();
    }
  }

  public Stream<Pair<byte[], byte[]>> stream() {
    final RocksIterator rocksIterator = snapTx.getIterator(readOptions, columnFamilyHandle);
    rocksIterator.seekToFirst();
    return RocksDbIterator.create(rocksIterator).toStream();
  }

  public Stream<byte[]> streamKeys() {
    final RocksIterator rocksIterator = snapTx.getIterator(readOptions, columnFamilyHandle);
    rocksIterator.seekToFirst();
    return RocksDbIterator.create(rocksIterator).toStreamKeys();
  }

  @Override
  public void commit() throws StorageException {
    // no-op or throw?
    throw new UnsupportedOperationException("RocksDBSnapshotTransaction does not support commit");
  }

  @Override
  public void rollback() {
    try {
      snapTx.rollback();
      metrics.getRollbackCount().inc();
    } catch (final RocksDBException e) {
      if (e.getMessage().contains(NO_SPACE_LEFT_ON_DEVICE)) {
        LOG.error(e.getMessage());
        System.exit(0);
      }
      throw new StorageException(e);
    } finally {
      close();
    }
  }

  public RocksDBSnapshotTransaction copy() {
    try {
      var copyReadOptions = new ReadOptions().setSnapshot(snapshot.markAndUseSnapshot());
      var copySnapTx = db.beginTransaction(writeOptions);
      copySnapTx.rebuildFromWriteBatch(snapTx.getWriteBatch().getWriteBatch());
      return new RocksDBSnapshotTransaction(
          db, columnFamilyHandle, metrics, snapshot, copySnapTx, copyReadOptions);
    } catch (Exception ex) {
      LOG.error("Failed to copy snapshot transaction", ex);
      snapshot.unMarkSnapshot();
      throw new StorageException(ex);
    }
  }

  @Override
  public void close() {
    snapTx.close();
    writeOptions.close();
    readOptions.close();
    snapshot.unMarkSnapshot();
  }
}
