/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.services.kvstore.adapter;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageAdapter;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransactionAdapter;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorage;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Segmented key value storage adapter.
 *
 * @param <S> the type parameter
 */
public class SegmentedKeyValueStorageAdapter<S> implements KeyValueStorageAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentedKeyValueStorageAdapter.class);
  protected final Map<SegmentIdentifier, S> segmentsHandle;
  protected final SegmentedKeyValueStorage<S> storage;

  /**
   * Instantiates a new Segmented key value storage adapter.
   *
   * @param segments the segments
   * @param storage the storage
   */
  public SegmentedKeyValueStorageAdapter(
      final List<SegmentIdentifier> segments, final SegmentedKeyValueStorage<S> storage) {
    segmentsHandle = new HashMap<>();
    segments.forEach(s -> segmentsHandle.put(s, storage.getSegmentIdentifierByName(s)));
    this.storage = storage;
  }

  @Override
  public void clear() {
    throwIfClosed();
    segmentsHandle.forEach((k, v) -> storage.clear(v));
  }

  @Override
  public void clear(final List<SegmentIdentifier> segmentIdentifiers) {
    throwIfClosed();
    segmentIdentifiers.forEach(segmentIdentifier -> storage.clear(segmentsHandle.get(segmentIdentifier)));
  }

  @Override
  public boolean containsKey(final SegmentIdentifier segmentIdentifier, final byte[] key)
      throws StorageException {
    throwIfClosed();
    return storage.containsKey(segmentsHandle.get(segmentIdentifier), key);
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segmentIdentifier, final byte[] key)
      throws StorageException {
    throwIfClosed();
    return storage.get(segmentsHandle.get(segmentIdentifier), key);
  }

  @Override
  public Set<byte[]> getAllKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    throwIfClosed();
    return storage.getAllKeysThat(segmentsHandle.get(segmentIdentifier), returnCondition);
  }

  @Override
  public Set<byte[]> getAllValuesFromKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    throwIfClosed();
    return storage.getAllValuesFromKeysThat(segmentsHandle.get(segmentIdentifier), returnCondition);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segmentIdentifier) {
    throwIfClosed();
    return storage.stream(segmentsHandle.get(segmentIdentifier));
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey) throws StorageException {
    return storage.streamFromKey(segmentsHandle.get(segmentIdentifier), startKey);
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segmentIdentifier) {
    throwIfClosed();
    return storage.streamKeys(segmentsHandle.get(segmentIdentifier));
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segmentIdentifier, final byte[] key) {
    throwIfClosed();
    return storage.tryDelete(segmentsHandle.get(segmentIdentifier), key);
  }

  @Override
  public void close() throws IOException {
    storage.close();
  }

  @Override
  public KeyValueStorageTransactionAdapter startTransaction() throws StorageException {
    SegmentedKeyValueStorage.Transaction<S> transaction = storage.startTransaction();
    return new KeyValueStorageTransactionAdapter() {

      @Override
      public void put(
          final SegmentIdentifier segmentIdentifier, final byte[] key, final byte[] value) {
        transaction.put(segmentsHandle.get(segmentIdentifier), key, value);
      }

      @Override
      public void remove(final SegmentIdentifier segmentIdentifier, final byte[] key) {
        transaction.remove(segmentsHandle.get(segmentIdentifier), key);
      }

      @Override
      public void commit() throws StorageException {
        transaction.commit();
      }

      @Override
      public void rollback() {
        transaction.rollback();
      }
    };
  }

  @Override
  public KeyValueStorageTransaction startTransaction(final SegmentIdentifier segmentIdentifier)
      throws StorageException {
    final SegmentedKeyValueStorage.Transaction<S> transaction = storage.startTransaction();
    return new KeyValueStorageTransaction() {

      @Override
      public void put(final byte[] key, final byte[] value) {
        throwIfClosed();
        transaction.put(segmentsHandle.get(segmentIdentifier), key, value);
      }

      @Override
      public void remove(final byte[] key) {
        throwIfClosed();
        transaction.remove(segmentsHandle.get(segmentIdentifier), key);
      }

      @Override
      public void commit() throws StorageException {
        throwIfClosed();
        transaction.commit();
      }

      @Override
      public void rollback() {
        throwIfClosed();
        transaction.rollback();
      }
    };
  }

  @Override
  public boolean isClosed() {
    return storage.isClosed();
  }

  protected void throwIfClosed() {
    if (storage.isClosed()) {
      LOG.error("Attempting to use a closed Storage instance.");
      throw new StorageException("Storage has been closed");
    }
  }
}
