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
import org.rocksdb.FlushOptions;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB PlainTable implementation of {@link FrozenSnapTrieNodeStorage}.
 *
 * <p>PlainTable is a read-optimised SST format for memory-mapped access ({@code allow_mmap_reads}).
 * Keys are fixed 32-byte hashes; values are trie-node RLP bytes. During snap-sync all trie nodes
 * are written to this store; on {@link #freeze()} the DB is compacted and reopened in read-only
 * mode so that subsequent lookups hit fully-compacted PlainTable SSTs.
 *
 * <p>PlainTable constraints satisfied here:
 * <ul>
 *   <li>Fixed-size keys (32 bytes).</li>
 *   <li>{@code allow_mmap_reads = true} – required by PlainTable.</li>
 *   <li>No compression – PlainTable does not support compressed SSTs.</li>
 * </ul>
 */
public class RocksDBPlainTableFrozenSnapTrieNodeStorage implements FrozenSnapTrieNodeStorage {

  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBPlainTableFrozenSnapTrieNodeStorage.class);

  /** Marker file written after a successful freeze + compaction. */
  private static final String FROZEN_MARKER_FILE = ".frozen";

  /** Fixed key length – every key is a Bytes32 node hash. */
  private static final int KEY_SIZE_BYTES = 32;

  /** Bloom bits per key for the PlainTable prefix bloom filter. */
  private static final int BLOOM_BITS_PER_KEY = 10;

  /** Write-buffer: 256 MB so large snap batches stay in memory before one SST flush. */
  private static final long WRITE_BUFFER_SIZE = 256L * 1024 * 1024;

  /** Keep at most 4 memtables before forcing a flush. */
  private static final int MAX_WRITE_BUFFER_NUMBER = 4;

  static {
    RocksDB.loadLibrary();
  }

  private final Path directory;
  private final AtomicBoolean frozen = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private DBOptions dbOptions;
  private ColumnFamilyOptions cfOptions;
  private ColumnFamilyHandle cfHandle;
  private RocksDB db;

  private RocksDBPlainTableFrozenSnapTrieNodeStorage(
      final Path directory, final boolean readOnly) {
    this.directory = directory;
    this.frozen.set(readOnly);
    openDB(readOnly);
  }

  /** Opens or creates the PlainTable RocksDB at {@code directory}. */
  public static RocksDBPlainTableFrozenSnapTrieNodeStorage open(final Path directory) {
    try {
      Files.createDirectories(directory);
    } catch (final IOException e) {
      throw new IllegalStateException(
          "Failed to create PlainTable RocksDB directory " + directory, e);
    }
    final boolean alreadyFrozen = Files.exists(directory.resolve(FROZEN_MARKER_FILE));
    return new RocksDBPlainTableFrozenSnapTrieNodeStorage(directory, alreadyFrozen);
  }

  // -------------------------------------------------------------------------
  // FrozenSnapTrieNodeStorage API
  // -------------------------------------------------------------------------

  @Override
  public void put(final Bytes32 hash, final Bytes value) {
    ensureWritable();
    try {
      db.put(cfHandle, hash.toArrayUnsafe(), value.toArrayUnsafe());
    } catch (final RocksDBException e) {
      throw new IllegalStateException("RocksDB put failed", e);
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
        batch.put(cfHandle, entry.getKey().toArrayUnsafe(), entry.getValue().toArrayUnsafe());
      }
      db.write(wo, batch);
    } catch (final RocksDBException e) {
      throw new IllegalStateException("RocksDB batch write failed", e);
    }
  }

  @Override
  public Optional<Bytes> get(final Bytes32 hash) {
    if (closed.get()) {
      throw new IllegalStateException("Frozen snap-sync trie node storage is closed");
    }
    try (ReadOptions ro = new ReadOptions()) {
      final byte[] value = db.get(cfHandle, ro, hash.toArrayUnsafe());
      if (value == null) {
        return Optional.empty();
      }
      return Optional.of(Bytes.wrap(Arrays.copyOf(value, value.length)));
    } catch (final RocksDBException e) {
      throw new IllegalStateException("RocksDB get failed", e);
    }
  }

  /**
   * Seals the store: flushes memtable, compacts the DB into a single PlainTable SST, writes the
   * {@code .frozen} marker, then reopens in read-only mode.
   */
  @Override
  public void freeze() {
    if (!frozen.compareAndSet(false, true)) {
      return;
    }

    // Flush memtable to SST before compacting.
    try (FlushOptions fo = new FlushOptions().setWaitForFlush(true)) {
      db.flush(fo, cfHandle);
    } catch (final RocksDBException e) {
      throw new IllegalStateException("RocksDB flush failed during freeze", e);
    }

    // Compact to a single level-0 PlainTable SST – optimal for read-only access.
    try {
      db.compactRange(cfHandle);
    } catch (final RocksDBException e) {
      throw new IllegalStateException("RocksDB compactRange failed during freeze", e);
    }

    closeInternal();

    try {
      Files.createFile(directory.resolve(FROZEN_MARKER_FILE));
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to write frozen marker at " + directory, e);
    }

    // Reopen in read-only mode so the OS can mmap the SSTs without copy-on-write overhead.
    openDB(true);

    LOG.info(
        "Frozen snap-sync trie node PlainTable RocksDB at {} (entries={})",
        directory,
        entryCount());
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
      LOG.warn("Could not read estimate-num-keys property", e);
      return 0L;
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    closeInternal();
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  private void openDB(final boolean readOnly) {
    final PlainTableConfig tableConfig =
        new PlainTableConfig()
            .setKeySize(KEY_SIZE_BYTES)
            .setBloomBitsPerKey(BLOOM_BITS_PER_KEY)
            // 0.75: 75 % of the index buckets are used before chaining
            .setHashTableRatio(0.75)
            // index entry every 16 keys – balanced lookup vs. index size
            .setIndexSparseness(16)
            // full-scan mode off; we always do point lookups
            .setFullScanMode(false)
            // store raw (unsorted) data in the first 0 bytes of each key: 0 = auto
            .setStoreIndexInFile(false);

    cfOptions =
        new ColumnFamilyOptions()
            .setTableFormatConfig(tableConfig)
            // PlainTable requires no compression
            .setCompressionType(CompressionType.NO_COMPRESSION)
            .setWriteBufferSize(WRITE_BUFFER_SIZE)
            .setMaxWriteBufferNumber(MAX_WRITE_BUFFER_NUMBER);

    dbOptions =
        new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            // Required by PlainTable for optimal read performance
            .setAllowMmapReads(true)
            // Disable mmap writes; sequential SST writes are fine without it
            .setAllowMmapWrites(false)
            .setUseFsync(false)
            .setMaxOpenFiles(-1);

    final List<ColumnFamilyDescriptor> cfDescriptors =
        List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
    final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

    try {
      final String dbPath = directory.toAbsolutePath().toString();
      if (readOnly) {
        db = RocksDB.openReadOnly(dbOptions, dbPath, cfDescriptors, cfHandles);
      } else {
        db = RocksDB.open(dbOptions, dbPath, cfDescriptors, cfHandles);
      }
      cfHandle = cfHandles.get(0);
    } catch (final RocksDBException e) {
      throw new IllegalStateException(
          "Failed to open PlainTable RocksDB at " + directory, e);
    }
  }

  private void closeInternal() {
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
  }

  private void ensureWritable() {
    if (frozen.get()) {
      throw new IllegalStateException("Frozen snap-sync trie node storage is read-only");
    }
    if (closed.get()) {
      throw new IllegalStateException("Frozen snap-sync trie node storage is closed");
    }
  }
}
