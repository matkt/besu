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
package org.hyperledger.besu.services.kvstore;

import com.google.common.collect.Streams;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.SORTED;

/** Key value storage which stores in memory all updates to a parent worldstate storage. */
public class CacherLayeredKeyValueStorage extends LayeredKeyValueStorage
    implements SnappedKeyValueStorage {


  public CacherLayeredKeyValueStorage(final SegmentedKeyValueStorage parent) {
    super(parent);
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segmentId, final byte[] key)
      throws StorageException {
    throwIfClosed();

    final Lock lock = rwLock.readLock();
    lock.lock();
    try {
      Bytes wrapKey = Bytes.wrap(key);
      NavigableMap<Bytes, Optional<byte[]>> bytesOptionalNavigableMap = hashValueStore.computeIfAbsent(segmentId, __ -> newSegmentMap());
      final Optional<byte[]> foundKey =
              bytesOptionalNavigableMap.get(wrapKey);
      if (foundKey == null) {
        Optional<byte[]> bytes = parent.get(segmentId, key);
        bytesOptionalNavigableMap.put(wrapKey, bytes);
        return bytes;
      } else {

        return foundKey;
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segmentId, final byte[] key) {
    return parent.tryDelete(segmentId, key);
  }

  @Override
  public SegmentedKeyValueStorageTransaction startTransaction() {
    throwIfClosed();

    throwIfClosed();

    return new SegmentedKeyValueStorageTransactionValidatorDecorator(
            new SegmentedInMemoryTransaction() {
              @Override
              public void commit() throws StorageException {
                final Lock lock = rwLock.writeLock();
                lock.lock();
                try {
                  SegmentedKeyValueStorageTransaction segmentedKeyValueStorageTransaction = parent.startTransaction();
                  updatedValues.forEach((key, value) -> value.forEach((bytes, bytes2) -> {
                    segmentedKeyValueStorageTransaction.put(key, bytes.toArrayUnsafe(), bytes2.orElseThrow());
                  }));

                  // put empty rather than remove in order to not ask parent in case of deletion
                  removedKeys.forEach((key, value) -> value.forEach((bytes) -> {
                    segmentedKeyValueStorageTransaction.remove(key, bytes.toArrayUnsafe());
                  }));
                  segmentedKeyValueStorageTransaction.commit();
                  hashValueStore.clear();
                  updatedValues.clear();
                  removedKeys.clear();
                } finally {
                  lock.unlock();
                }
              }
            },
            this::isClosed);
  }


}
