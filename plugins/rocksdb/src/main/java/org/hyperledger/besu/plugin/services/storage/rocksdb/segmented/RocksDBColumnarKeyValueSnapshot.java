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

import static java.util.stream.Collectors.toUnmodifiableSet;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransactionAdapter;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorageAdapter;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetrics;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbSegmentIdentifier;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.rocksdb.OptimisticTransactionDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The RocksDb columnar key value snapshot. */
public class RocksDBColumnarKeyValueSnapshot implements SnappedKeyValueStorageAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBColumnarKeyValueSnapshot.class);

  /** The Db. */
  final OptimisticTransactionDB db;

  /** The Snap tx. */
  final RocksDBSnapshotTransaction snapTx;

  protected final Map<SegmentIdentifier, RocksDbSegmentIdentifier> segmentsHandle;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Instantiates a new RocksDb columnar key value snapshot.
   *
   * @param db the db
   * @param metrics the metrics
   * @param segmentsHandle
   */
  RocksDBColumnarKeyValueSnapshot(
      final OptimisticTransactionDB db,
      final RocksDBMetrics metrics,
      final Map<SegmentIdentifier, RocksDbSegmentIdentifier> segmentsHandle) {
    this.db = db;
    this.segmentsHandle = segmentsHandle;
    this.snapTx = new RocksDBSnapshotTransaction(db, metrics);
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segmentIdentifier, final byte[] key)
      throws StorageException {
    throwIfClosed();
    return snapTx.get(segmentsHandle.get(segmentIdentifier), key);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segmentIdentifier) {
    throwIfClosed();
    return snapTx.stream(segmentsHandle.get(segmentIdentifier));
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey) {
    return stream(segmentIdentifier)
        .filter(e -> Bytes.wrap(startKey).compareTo(Bytes.wrap(e.getKey())) <= 0);
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segmentIdentifier) {
    throwIfClosed();
    return snapTx.streamKeys(segmentsHandle.get(segmentIdentifier));
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segmentIdentifier, final byte[] key)
      throws StorageException {
    throwIfClosed();
    snapTx.remove(segmentsHandle.get(segmentIdentifier), key);
    return true;
  }

  @Override
  public Set<byte[]> getAllKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return streamKeys(segmentIdentifier).filter(returnCondition).collect(toUnmodifiableSet());
  }

  @Override
  public Set<byte[]> getAllValuesFromKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return stream(segmentIdentifier)
        .filter(pair -> returnCondition.test(pair.getKey()))
        .map(Pair::getValue)
        .collect(toUnmodifiableSet());
  }

  @Override
  public KeyValueStorageTransactionAdapter startTransaction() throws StorageException {
    // The use of a transaction on a transaction based key value store is dubious
    // at best.  return our snapshot transaction instead.
    return new KeyValueStorageTransactionAdapter() {
      @Override
      public void put(
          final SegmentIdentifier segmentIdentifier, final byte[] key, final byte[] value) {
        snapTx.put(segmentsHandle.get(segmentIdentifier), key, value);
      }

      @Override
      public void remove(final SegmentIdentifier segmentIdentifier, final byte[] key) {
        snapTx.remove(segmentsHandle.get(segmentIdentifier), key);
      }

      @Override
      public void commit() throws StorageException {
        // no op
      }

      @Override
      public void rollback() {
        snapTx.rollback();
      }
    };
  }

  @Override
  public KeyValueStorageTransaction startTransaction(final SegmentIdentifier segmentIdentifier) throws StorageException {
    return new KeyValueStorageTransaction() {
      @Override
      public void put(final byte[] key, final byte[] value) {
        snapTx.put(segmentsHandle.get(segmentIdentifier), key, value);
      }

      @Override
      public void remove(final byte[] key) {
        snapTx.remove(segmentsHandle.get(segmentIdentifier), key);
      }

      @Override
      public void commit() throws StorageException {
        // no op
      }

      @Override
      public void rollback() {
        snapTx.rollback();
      }
    };
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException(
        "RocksDBColumnarKeyValueSnapshot does not support clear");
  }

  @Override
  public void clear(final List<SegmentIdentifier> segmentIdentifiers) {
    throw new UnsupportedOperationException(
        "RocksDBColumnarKeyValueSnapshot does not support clear");
  }

  @Override
  public boolean containsKey(final SegmentIdentifier segmentIdentifier, final byte[] key)
      throws StorageException {
    throwIfClosed();
    return snapTx.get(segmentsHandle.get(segmentIdentifier), key).isPresent();
  }

  @Override
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      snapTx.close();
    }
  }

  private void throwIfClosed() {
    if (closed.get()) {
      LOG.error("Attempting to use a closed RocksDBKeyValueStorage");
      throw new IllegalStateException("Storage has been closed");
    }
  }

  @Override
  public KeyValueStorageTransactionAdapter getSnapshotTransaction() {
    return startTransaction();
  }
}
