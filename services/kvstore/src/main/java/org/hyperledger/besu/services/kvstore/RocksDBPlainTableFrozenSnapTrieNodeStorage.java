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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** RocksDB PlainTable implementation of {@link FrozenSnapTrieNodeStorage}. */
public class RocksDBPlainTableFrozenSnapTrieNodeStorage implements FrozenSnapTrieNodeStorage {

  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBPlainTableFrozenSnapTrieNodeStorage.class);

  private static final String FROZEN_MARKER_FILE = ".frozen";
  private static final int KEY_SIZE_BYTES = 32;
  private static final long WRITE_BUFFER_SIZE = 256L * 1024 * 1024;

  static {
    RocksDB.loadLibrary();
  }

  private final Path directory;
  private final AtomicBoolean frozen = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final RocksDB db;
  private final ColumnFamilyHandle cfHandle;
  private final DBOptions dbOptions;
  private final ColumnFamilyOptions cfOptions;

  private RocksDBPlainTableFrozenSnapTrieNodeStorage(final Path directory) {
    this.directory = directory;
    this.frozen.set(Files.exists(directory.resolve(FROZEN_MARKER_FILE)));

    cfOptions =
        new ColumnFamilyOptions()
            .setTableFormatConfig(
                new PlainTableConfig()
                    .setKeySize(KEY_SIZE_BYTES)
                    .setBloomBitsPerKey(10)
                    .setHashTableRatio(0)
                    .setIndexSparseness(16))
            .setCompressionType(CompressionType.NO_COMPRESSION)
            .setMemTableConfig(new HashLinkedListMemTableConfig())
            .setWriteBufferSize(WRITE_BUFFER_SIZE);

    dbOptions =
        new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setAllowMmapReads(true)
            .setAllowMmapWrites(false)
            .setAllowConcurrentMemtableWrite(false)
            .setMaxOpenFiles(-1);

    final List<ColumnFamilyHandle> handles = new ArrayList<>();
    final String dbPath = directory.toAbsolutePath().toString();
    final List<ColumnFamilyDescriptor> cfDescriptors =
        List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
    try {
      if (frozen.get()) {
        db = RocksDB.openReadOnly(dbOptions, dbPath, cfDescriptors, handles);
      } else {
        db = RocksDB.open(dbOptions, dbPath, cfDescriptors, handles);
      }
      cfHandle = handles.get(0);
    } catch (final RocksDBException e) {
      throw new IllegalStateException("Failed to open PlainTable RocksDB at " + directory, e);
    }
  }

  public static RocksDBPlainTableFrozenSnapTrieNodeStorage open(final Path directory) {
    try {
      Files.createDirectories(directory);
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to create directory " + directory, e);
    }
    return new RocksDBPlainTableFrozenSnapTrieNodeStorage(directory);
  }

  @Override
  public void put(final Bytes32 hash, final Bytes value) {
    ensureWritable();
    final byte[] key = keyBytes(hash);
    final byte[] val = value.toArray();
    try {
      db.put(cfHandle, key, val);
    } catch (final RocksDBException e) {
      throw new IllegalStateException("RocksDB put failed: " + e.getMessage(), e);
    }
  }

  @Override
  public void putAll(final Map<Bytes32, Bytes> entries) {
    if (entries.isEmpty()) {
      return;
    }
    ensureWritable();
    try (WriteBatch batch = new WriteBatch();
        WriteOptions wo = new WriteOptions()) {
      for (final Map.Entry<Bytes32, Bytes> entry : entries.entrySet()) {
        batch.put(cfHandle, keyBytes(entry.getKey()), entry.getValue().toArray());
      }
      db.write(wo, batch);
    } catch (final RocksDBException e) {
      throw new IllegalStateException("RocksDB batch write failed: " + e.getMessage(), e);
    }
  }

  @Override
  public Optional<Bytes> get(final Bytes32 hash) {
    if (closed.get()) {
      throw new IllegalStateException("Frozen snap-sync trie node storage is closed");
    }
    try {
      final byte[] value = db.get(cfHandle, keyBytes(hash));
      if (value == null) {
        return Optional.empty();
      }
      return Optional.of(Bytes.wrap(Arrays.copyOf(value, value.length)));
    } catch (final RocksDBException e) {
      throw new IllegalStateException("RocksDB get failed: " + e.getMessage(), e);
    }
  }

  @Override
  public void freeze() {
    if (!frozen.compareAndSet(false, true)) {
      return;
    }
    try {
      Files.createFile(directory.resolve(FROZEN_MARKER_FILE));
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to write frozen marker at " + directory, e);
    }
    LOG.info("Frozen snap-sync trie node PlainTable RocksDB at {} (entries={})", directory, entryCount());
  }

  @Override
  public boolean isFrozen() {
    return frozen.get();
  }

  @Override
  public long entryCount() {
    try {
      final String prop = db.getProperty(cfHandle, "rocksdb.estimate-num-keys");
      return prop == null ? 0L : Long.parseLong(prop.trim());
    } catch (final RocksDBException e) {
      return 0L;
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    cfHandle.close();
    db.close();
    cfOptions.close();
    dbOptions.close();
  }

  private void ensureWritable() {
    if (frozen.get()) {
      throw new IllegalStateException("Frozen snap-sync trie node storage is read-only");
    }
    if (closed.get()) {
      throw new IllegalStateException("Frozen snap-sync trie node storage is closed");
    }
  }

  private static byte[] keyBytes(final Bytes32 hash) {
    return Arrays.copyOf(hash.toArrayUnsafe(), KEY_SIZE_BYTES);
  }
}
