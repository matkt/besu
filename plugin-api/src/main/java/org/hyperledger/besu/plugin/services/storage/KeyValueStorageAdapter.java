package org.hyperledger.besu.plugin.services.storage;

import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

public interface KeyValueStorageAdapter {
  void clear();

  void clear(List<SegmentIdentifier> segmentIdentifiers);

  boolean containsKey(SegmentIdentifier segmentIdentifier, byte[] key) throws StorageException;

  Optional<byte[]> get(SegmentIdentifier segmentIdentifier, byte[] key) throws StorageException;

  Set<byte[]> getAllKeysThat(
      SegmentIdentifier segmentIdentifier, Predicate<byte[]> returnCondition);

  Set<byte[]> getAllValuesFromKeysThat(
      SegmentIdentifier segmentIdentifier, Predicate<byte[]> returnCondition);

  Stream<Pair<byte[], byte[]>> stream(SegmentIdentifier segmentIdentifier);

  Stream<Pair<byte[], byte[]>> streamFromKey(SegmentIdentifier segmentIdentifier, byte[] startKey)
      throws StorageException;

  Stream<byte[]> streamKeys(SegmentIdentifier segmentIdentifier);

  boolean tryDelete(SegmentIdentifier segmentIdentifier, byte[] key);

  void close() throws IOException;

  KeyValueStorageTransactionAdapter startTransaction() throws StorageException;

  KeyValueStorageTransaction startTransaction(SegmentIdentifier segmentIdentifier)
      throws StorageException;

  boolean isClosed();

  static KeyValueStorage getKeyValueStorage(
      final SegmentIdentifier segmentIdentifier,
      final KeyValueStorageAdapter keyValueStorageAdapter) {
    return new KeyValueStorage() {
      @Override
      public void clear() throws StorageException {
        keyValueStorageAdapter.clear();
      }

      @Override
      public boolean containsKey(final byte[] key) throws StorageException {
        return keyValueStorageAdapter.containsKey(segmentIdentifier, key);
      }

      @Override
      public Optional<byte[]> get(final byte[] key) throws StorageException {
        return keyValueStorageAdapter.get(segmentIdentifier, key);
      }

      @Override
      public Stream<Pair<byte[], byte[]>> stream() throws StorageException {
        return keyValueStorageAdapter.stream(segmentIdentifier);
      }

      @Override
      public Stream<Pair<byte[], byte[]>> streamFromKey(final byte[] startKey) {
        return keyValueStorageAdapter.streamFromKey(segmentIdentifier, startKey);
      }

      @Override
      public Stream<byte[]> streamKeys() throws StorageException {
        return keyValueStorageAdapter.streamKeys(segmentIdentifier);
      }

      @Override
      public boolean tryDelete(final byte[] key) throws StorageException {
        return keyValueStorageAdapter.tryDelete(segmentIdentifier, key);
      }

      @Override
      public Set<byte[]> getAllKeysThat(final Predicate<byte[]> returnCondition) {
        return keyValueStorageAdapter.getAllKeysThat(segmentIdentifier, returnCondition);
      }

      @Override
      public Set<byte[]> getAllValuesFromKeysThat(final Predicate<byte[]> returnCondition) {
        return keyValueStorageAdapter.getAllValuesFromKeysThat(segmentIdentifier, returnCondition);
      }

      @Override
      public KeyValueStorageTransaction startTransaction() throws StorageException {
        return keyValueStorageAdapter.startTransaction(segmentIdentifier);
      }

      @Override
      public boolean isClosed() {
        return keyValueStorageAdapter.isClosed();
      }

      @Override
      public void close() throws IOException {
        keyValueStorageAdapter.close();
      }
    };
  }
}
