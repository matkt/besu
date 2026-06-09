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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.LRUCache;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** BlockBased RocksDB implementation of {@link FrozenSnapTrieNodeStorage}. */
public class RocksDBBlockBasedFrozenSnapTrieNodeStorage implements FrozenSnapTrieNodeStorage {

  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBBlockBasedFrozenSnapTrieNodeStorage.class);

  private static final String FROZEN_MARKER_FILE = ".frozen";
  private static final int KEY_SIZE_BYTES = 32;

  /** Bounded block cache keeps frozen cold reads off unbounded mmap. */
  private static final long BLOCK_CACHE_BYTES = 1024L * 1024 * 1024;

  private static final long WRITE_BUFFER_SIZE = 64L * 1024 * 1024;
  private static final long DB_WRITE_BUFFER_SIZE = 128L * 1024 * 1024;
  private static final int MAX_OPEN_FILES = 1024;

  static {
    RocksDB.loadLibrary();
  }

  private final Path directory;
  private final AtomicBoolean frozen = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);
  private final ReadOptions readOptions = new ReadOptions().setVerifyChecksums(false);
  private Cache blockCache;
  private RocksDB db;
  private ColumnFamilyHandle cfHandle;
  private DBOptions dbOptions;
  private ColumnFamilyOptions cfOptions;

  private RocksDBBlockBasedFrozenSnapTrieNodeStorage(final Path directory) {
    this.directory = directory;
    final boolean alreadyFrozen = Files.exists(directory.resolve(FROZEN_MARKER_FILE));
    this.frozen.set(alreadyFrozen);
    openDB(alreadyFrozen);
  }

  public static RocksDBBlockBasedFrozenSnapTrieNodeStorage open(final Path directory) {
    try {
      Files.createDirectories(directory);
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to create directory " + directory, e);
    }
    return new RocksDBBlockBasedFrozenSnapTrieNodeStorage(directory);
  }

  @Override
  public void put(final Bytes32 hash, final Bytes value) {
    ensureWritable();
    try {
      db.put(cfHandle, writeOptions, keyBytes(hash), value.toArrayUnsafe());
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
    try (WriteBatch batch = new WriteBatch()) {
      for (final Map.Entry<Bytes32, Bytes> entry : entries.entrySet()) {
        batch.put(cfHandle, keyBytes(entry.getKey()), entry.getValue().toArrayUnsafe());
      }
      db.write(writeOptions, batch);
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
      final byte[] value = db.get(cfHandle, readOptions, keyBytes(hash));
      if (value == null) {
        return Optional.empty();
      }
      return Optional.of(Bytes.wrap(value));
    } catch (final RocksDBException e) {
      throw new IllegalStateException("RocksDB get failed: " + e.getMessage(), e);
    }
  }

  @Override
  public void freeze() {
    if (!frozen.compareAndSet(false, true)) {
      return;
    }

    try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
      db.flush(flushOptions, cfHandle);
    } catch (final RocksDBException e) {
      throw new IllegalStateException("RocksDB flush failed during freeze", e);
    }

    try {
      db.compactRange(cfHandle);
    } catch (final RocksDBException e) {
      throw new IllegalStateException("RocksDB compactRange failed during freeze", e);
    }

    closeDbResources();

    try {
      Files.createFile(directory.resolve(FROZEN_MARKER_FILE));
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to write frozen marker at " + directory, e);
    }

    openDB(true);
    LOG.info(
        "Frozen snap-sync trie node BlockBased RocksDB at {} (entries={})", directory, entryCount());
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
    writeOptions.close();
    readOptions.close();
    closeDbResources();
  }

  private void openDB(final boolean readOnly) {
    blockCache = new LRUCache(BLOCK_CACHE_BYTES);
    cfOptions = readOnly ? createReadOnlyCfOptions(blockCache) : createWritableCfOptions(blockCache);
    dbOptions = readOnly ? createReadOnlyDbOptions() : createWritableDbOptions();

    final List<ColumnFamilyHandle> handles = new ArrayList<>();
    final String dbPath = directory.toAbsolutePath().toString();
    final List<ColumnFamilyDescriptor> cfDescriptors =
        List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
    try {
      if (readOnly) {
        db = RocksDB.openReadOnly(dbOptions, dbPath, cfDescriptors, handles);
      } else {
        db = RocksDB.open(dbOptions, dbPath, cfDescriptors, handles);
      }
      cfHandle = handles.get(0);
    } catch (final RocksDBException e) {
      closeDbResources();
      throw new IllegalStateException("Failed to open BlockBased RocksDB at " + directory, e);
    }
  }

  private static BlockBasedTableConfig blockBasedTableConfig(final Cache cache) {
    return new BlockBasedTableConfig()
        .setBlockCache(cache)
        .setFilterPolicy(new BloomFilter(10, false))
        .setCacheIndexAndFilterBlocks(true)
        .setPinL0FilterAndIndexBlocksInCache(true);
  }

  private static ColumnFamilyOptions createReadOnlyCfOptions(final Cache cache) {
    return new ColumnFamilyOptions()
        .setTableFormatConfig(blockBasedTableConfig(cache))
        .setCompressionType(CompressionType.LZ4_COMPRESSION);
  }

  private static ColumnFamilyOptions createWritableCfOptions(final Cache cache) {
    return new ColumnFamilyOptions()
        .setTableFormatConfig(blockBasedTableConfig(cache))
        .setCompressionType(CompressionType.LZ4_COMPRESSION)
        .setMemTableConfig(new HashLinkedListMemTableConfig())
        .setWriteBufferSize(WRITE_BUFFER_SIZE)
        .setMaxWriteBufferNumber(2);
  }

  private static DBOptions createReadOnlyDbOptions() {
    return new DBOptions()
        .setCreateIfMissing(false)
        .setAllowMmapReads(false)
        .setAllowMmapWrites(false)
        .setMaxOpenFiles(MAX_OPEN_FILES);
  }

  private static DBOptions createWritableDbOptions() {
    return new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)
        .setAllowMmapReads(false)
        .setAllowMmapWrites(false)
        .setAllowConcurrentMemtableWrite(false)
        .setEnablePipelinedWrite(true)
        .setDbWriteBufferSize(DB_WRITE_BUFFER_SIZE)
        .setMaxTotalWalSize(0)
        .setMaxOpenFiles(MAX_OPEN_FILES);
  }

  private void closeDbResources() {
    if (cfHandle != null) {
      cfHandle.close();
      cfHandle = null;
    }
    if (db != null) {
      db.close();
      db = null;
    }
    if (cfOptions != null) {
      cfOptions.close();
      cfOptions = null;
    }
    if (dbOptions != null) {
      dbOptions.close();
      dbOptions = null;
    }
    if (blockCache != null) {
      blockCache.close();
      blockCache = null;
    }
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
    final byte[] key = hash.toArrayUnsafe();
    if (key.length == KEY_SIZE_BYTES) {
      return key;
    }
    return hash.toArray();
  }
}
