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
package org.hyperledger.besu.services.kvstore;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SnappableKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;

/**
 * Routes segment operations to multiple {@link SegmentedKeyValueStorage} backends. Transactions
 * commit to every involved backend; a failure after partial commit cannot be rolled back across DBs.
 */
public class ComposedSegmentedKeyValueStorage implements SnappableKeyValueStorage {

  private final Map<SegmentIdentifier, SegmentedKeyValueStorage> segmentToStorage;
  private final List<SegmentedKeyValueStorage> uniqueStorages;

  public ComposedSegmentedKeyValueStorage(
      final Map<SegmentIdentifier, SegmentedKeyValueStorage> segmentToStorage) {
    this.segmentToStorage = Map.copyOf(segmentToStorage);
    this.uniqueStorages = segmentToStorage.values().stream().distinct().toList();
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    return storageFor(segment).get(segment, key);
  }

  @Override
  public Optional<NearestKeyValue> getNearestBefore(final SegmentIdentifier segmentIdentifier, final Bytes key)
      throws StorageException {
    return storageFor(segmentIdentifier).getNearestBefore(segmentIdentifier, key);
  }

  @Override
  public Optional<NearestKeyValue> getNearestAfter(final SegmentIdentifier segmentIdentifier, final Bytes key)
      throws StorageException {
    return storageFor(segmentIdentifier).getNearestAfter(segmentIdentifier, key);
  }

  @Override
  public SegmentedKeyValueStorageTransaction startTransaction() throws StorageException {
    final Map<SegmentedKeyValueStorage, SegmentedKeyValueStorageTransaction> transactions =
        new HashMap<>();
    for (final SegmentedKeyValueStorage storage : uniqueStorages) {
      transactions.put(storage, storage.startTransaction());
    }
    return new ComposedSegmentedKeyValueStorageTransaction(segmentToStorage, transactions);
  }

  @Override
  public SegmentedKeyValueStorageTransaction startLowPriorityTransaction()
      throws StorageException {
    final Map<SegmentedKeyValueStorage, SegmentedKeyValueStorageTransaction> transactions =
        new HashMap<>();
    for (final SegmentedKeyValueStorage storage : uniqueStorages) {
      transactions.put(storage, storage.startLowPriorityTransaction());
    }
    return new ComposedSegmentedKeyValueStorageTransaction(segmentToStorage, transactions);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segmentIdentifier) {
    return storageFor(segmentIdentifier).stream(segmentIdentifier);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey) {
    return storageFor(segmentIdentifier).streamFromKey(segmentIdentifier, startKey);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segmentIdentifier, final byte[] startKey, final byte[] endKey) {
    return storageFor(segmentIdentifier).streamFromKey(segmentIdentifier, startKey, endKey);
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segmentIdentifier) {
    return storageFor(segmentIdentifier).streamKeys(segmentIdentifier);
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segmentIdentifier, final byte[] key)
      throws StorageException {
    return storageFor(segmentIdentifier).tryDelete(segmentIdentifier, key);
  }

  @Override
  public Set<byte[]> getAllKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return storageFor(segmentIdentifier).getAllKeysThat(segmentIdentifier, returnCondition);
  }

  @Override
  public Set<byte[]> getAllValuesFromKeysThat(
      final SegmentIdentifier segmentIdentifier, final Predicate<byte[]> returnCondition) {
    return storageFor(segmentIdentifier).getAllValuesFromKeysThat(segmentIdentifier, returnCondition);
  }

  @Override
  public void clear(final SegmentIdentifier segmentIdentifier) {
    storageFor(segmentIdentifier).clear(segmentIdentifier);
  }

  @Override
  public boolean isClosed() {
    return uniqueStorages.stream().allMatch(SegmentedKeyValueStorage::isClosed);
  }

  @Override
  public void close() throws IOException {
    IOException first = null;
    for (final SegmentedKeyValueStorage storage : uniqueStorages) {
      try {
        storage.close();
      } catch (final IOException e) {
        if (first == null) {
          first = e;
        } else {
          first.addSuppressed(e);
        }
      }
    }
    if (first != null) {
      throw first;
    }
  }

  @Override
  public SnappedKeyValueStorage takeSnapshot() {
    final Map<SegmentIdentifier, SegmentedKeyValueStorage> snapshotSegmentToStorage =
        new HashMap<>();
    final Map<SegmentedKeyValueStorage, SnappedKeyValueStorage> storageToSnapshot =
        new HashMap<>();
    for (final Map.Entry<SegmentIdentifier, SegmentedKeyValueStorage> entry :
        segmentToStorage.entrySet()) {
      final SegmentedKeyValueStorage storage = entry.getValue();
      final SnappedKeyValueStorage snapshot =
          storageToSnapshot.computeIfAbsent(
              storage,
              ignored -> {
                if (storage instanceof SnappableKeyValueStorage snappable) {
                  return snappable.takeSnapshot();
                }
                throw new StorageException(
                    "Cannot snapshot composed storage: backend "
                        + storage.getClass().getName()
                        + " is not snappable");
              });
      snapshotSegmentToStorage.put(entry.getKey(), snapshot);
    }
    return new LayeredKeyValueStorage(new ComposedSegmentedKeyValueStorage(snapshotSegmentToStorage));
  }

  private SegmentedKeyValueStorage storageFor(final SegmentIdentifier segment) {
    final SegmentedKeyValueStorage storage = segmentToStorage.get(segment);
    if (storage == null) {
      throw new StorageException("No backing storage configured for segment " + segment);
    }
    return storage;
  }

  private static final class ComposedSegmentedKeyValueStorageTransaction
      implements SegmentedKeyValueStorageTransaction {

    private final Map<SegmentIdentifier, SegmentedKeyValueStorage> segmentToStorage;
    private final Map<SegmentedKeyValueStorage, SegmentedKeyValueStorageTransaction> transactions;
    private boolean closed;

    private ComposedSegmentedKeyValueStorageTransaction(
        final Map<SegmentIdentifier, SegmentedKeyValueStorage> segmentToStorage,
        final Map<SegmentedKeyValueStorage, SegmentedKeyValueStorageTransaction> transactions) {
      this.segmentToStorage = segmentToStorage;
      this.transactions = transactions;
    }

    @Override
    public void put(
        final SegmentIdentifier segmentIdentifier, final byte[] key, final byte[] value) {
      transactionFor(segmentIdentifier).put(segmentIdentifier, key, value);
    }

    @Override
    public void remove(final SegmentIdentifier segmentIdentifier, final byte[] key) {
      transactionFor(segmentIdentifier).remove(segmentIdentifier, key);
    }

    @Override
    public void commit() throws StorageException {
      final Set<SegmentedKeyValueStorageTransaction> committed = new HashSet<>();
      try {
        for (final SegmentedKeyValueStorageTransaction transaction : transactions.values()) {
          transaction.commit();
          committed.add(transaction);
        }
      } catch (final StorageException e) {
        for (final SegmentedKeyValueStorageTransaction transaction : transactions.values()) {
          if (!committed.contains(transaction)) {
            try {
              transaction.rollback();
            } catch (final RuntimeException suppressed) {
              e.addSuppressed(suppressed);
            }
          }
        }
        throw e;
      }
    }

    @Override
    public void rollback() {
      for (final SegmentedKeyValueStorageTransaction transaction : transactions.values()) {
        transaction.rollback();
      }
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      for (final SegmentedKeyValueStorageTransaction transaction : transactions.values()) {
        transaction.close();
      }
    }

    private SegmentedKeyValueStorageTransaction transactionFor(
        final SegmentIdentifier segmentIdentifier) {
      final SegmentedKeyValueStorage storage = segmentToStorage.get(segmentIdentifier);
      if (storage == null) {
        throw new StorageException("No backing storage configured for segment " + segmentIdentifier);
      }
      return transactions.get(storage);
    }
  }
}
