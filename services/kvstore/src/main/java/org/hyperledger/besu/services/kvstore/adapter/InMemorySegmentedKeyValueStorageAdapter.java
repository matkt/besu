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
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

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
public class InMemorySegmentedKeyValueStorageAdapter<S> implements KeyValueStorageAdapter {

  private static final Logger LOG =
      LoggerFactory.getLogger(InMemorySegmentedKeyValueStorageAdapter.class);

  protected final Map<SegmentIdentifier,InMemoryKeyValueStorage> segmentsHandler;

  /** Instantiates a new Segmented key value storage adapter. */
  public InMemorySegmentedKeyValueStorageAdapter(final List<SegmentIdentifier> segments) {
    segmentsHandler = new HashMap<>();
    segments.forEach(segmentIdentifier -> segmentsHandler.put(segmentIdentifier, new InMemoryKeyValueStorage()));
  }

  @Override
  public void clear() {
    segmentsHandler.values().clear();
  }

  @Override
  public void clear(final List<SegmentIdentifier> segmentIdentifiers) {
    segmentIdentifiers.forEach(segmentIdentifier -> segmentsHandler.get(segmentIdentifier).clear());
  }

  @Override
  public boolean containsKey(final SegmentIdentifier segmentIdentifier, final byte[] key)
      throws StorageException {
    return segmentsHandler.get(segmentIdentifier).containsKey(key);
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segmentIdentifier, final byte[] key)
      throws StorageException {
    return segmentsHandler.get(segmentIdentifier).get(key);
  }

  @Override
  public Set<byte[]> getAllKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return segmentsHandler.get(segmentIdentifier).getAllKeysThat(returnCondition);
  }

  @Override
  public Set<byte[]> getAllValuesFromKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return segmentsHandler.get(segmentIdentifier).getAllValuesFromKeysThat(returnCondition);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segmentIdentifier) {
    return segmentsHandler.get(segmentIdentifier).stream();
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey) throws StorageException {
    return segmentsHandler.get(segmentIdentifier).streamFromKey(startKey);
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segmentIdentifier) {
    return segmentsHandler.get(segmentIdentifier).streamKeys();
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segmentIdentifier, final byte[] key) {
    return segmentsHandler.get(segmentIdentifier).tryDelete(key);
  }

  @Override
  public void close() throws IOException {
    segmentsHandler.values().forEach(InMemoryKeyValueStorage::close);
  }

  @Override
  public KeyValueStorageTransactionAdapter startTransaction() throws StorageException {
    final Map<SegmentIdentifier, KeyValueStorageTransaction> transactions = new HashMap<>();
    segmentsHandler.forEach((key, value) -> transactions.put(key, value.startTransaction()));
    return new KeyValueStorageTransactionAdapter() {
      @Override
      public void put(
          final SegmentIdentifier segmentIdentifier, final byte[] key, final byte[] value) {
        transactions.get(segmentIdentifier).put(key, value);
      }

      @Override
      public void remove(final SegmentIdentifier segmentIdentifier, final byte[] key) {
        transactions.get(segmentIdentifier).remove(key);
      }

      @Override
      public void commit() throws StorageException {
        transactions.values().forEach(KeyValueStorageTransaction::commit);
      }

      @Override
      public void rollback() {
        transactions.values().forEach(KeyValueStorageTransaction::rollback);
      }
    };
  }

  @Override
  public KeyValueStorageTransaction startTransaction(final SegmentIdentifier segmentIdentifier)
      throws StorageException {
    return segmentsHandler.get(segmentIdentifier).startTransaction();
  }

  @Override
  public boolean isClosed() {
    return false;
  }


}
