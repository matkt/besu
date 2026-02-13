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
package org.hyperledger.besu.plugin.services.storage.rocksdb.segmented;

import static java.util.stream.Collectors.toUnmodifiableSet;

import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SnappableKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetrics;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbIterator;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbUtil;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfiguration;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageTransactionValidatorDecorator;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.LRUCache;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.Status;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB Columnar storage with separate database instance per column.
 *
 * <p>Instead of using column families within a single RocksDB instance, this implementation creates
 * a separate RocksDB database for each segment/column. This provides better isolation and
 * independent configuration per segment.
 */
public class SeparateDBRocksDBColumnarKeyValueStorage
    implements SegmentedKeyValueStorage, SnappableKeyValueStorage {

  private static final Logger LOG =
      LoggerFactory.getLogger(SeparateDBRocksDBColumnarKeyValueStorage.class);

  private static final int ROCKSDB_FORMAT_VERSION = 5;
  private static final long ROCKSDB_BLOCK_SIZE = 32768;
  protected static final long ROCKSDB_BLOCKCACHE_SIZE_HIGH_SPEC = 1_073_741_824L;
  protected static final long WAL_MAX_TOTAL_SIZE = 1_073_741_824L;
  protected static final long EXPECTED_WAL_FILE_SIZE = 67_108_864L;
  private static final long NUMBER_OF_LOG_FILES_TO_KEEP = 7;
  private static final long TIME_TO_ROLL_LOG_FILE = 86_400L;
  private static final int KEY_RANGE_SHARDS = 16;
  private static final String SHARDED_SEGMENT_NAME = "ACCOUNT_STORAGE_STORAGE";

  static {
    RocksDbUtil.loadNativeLibrary();
  }

  protected final AtomicBoolean closed = new AtomicBoolean(false);
  private final WriteOptions tryDeleteOptions =
      new WriteOptions().setNoSlowdown(true).setIgnoreMissingColumnFamilies(true);
  private final ReadOptions readOptions = new ReadOptions().setVerifyChecksums(false);

  protected final RocksDBConfiguration configuration;
  private final MetricsSystem metricsSystem;
  private final RocksDBMetricsFactory rocksDBMetricsFactory;
  private final org.hyperledger.besu.plugin.services.storage.rocksdb.configuration
          .PerColumnConfiguration
      perColumnConfig;
  private final Map<SegmentIdentifier, ShardBindings> segmentBindings = new HashMap<>();

  /** Map of RocksDB instances per segment shard. */
  private final Map<String, TransactionDB> databases = new HashMap<>();

  /** Map of default column handles per segment shard. */
  private final Map<String, ColumnFamilyHandle> defaultColumnHandles = new HashMap<>();

  /** Map of metrics per segment shard. */
  private final Map<String, RocksDBMetrics> segmentMetrics = new HashMap<>();

  /** Map of statistics per segment shard. */
  private final Map<String, Statistics> segmentStats = new HashMap<>();

  /** Map of row caches per segment shard (if enabled). */
  private final Map<String, org.rocksdb.Cache> segmentRowCaches = new HashMap<>();

  /** Map of block caches per segment shard. */
  private final Map<String, org.rocksdb.Cache> segmentBlockCaches = new HashMap<>();

  private static final class ShardBindings {
    private final TransactionDB[] databases;
    private final ColumnFamilyHandle[] handles;
    private final RocksDBMetrics[] metrics;

    private ShardBindings(final int shardCount) {
      this.databases = new TransactionDB[shardCount];
      this.handles = new ColumnFamilyHandle[shardCount];
      this.metrics = new RocksDBMetrics[shardCount];
    }
  }

  /**
   * Instantiates a new Separate DB RocksDB columnar key value storage.
   *
   * @param configuration the configuration
   * @param segments the segments to create separate databases for
   * @param ignorableSegments the ignorable segments (not used in this implementation)
   * @param metricsSystem the metrics system
   * @param rocksDBMetricsFactory the rocks db metrics factory
   * @throws StorageException the storage exception
   */
  public SeparateDBRocksDBColumnarKeyValueStorage(
      final RocksDBConfiguration configuration,
      final List<SegmentIdentifier> segments,
      final List<SegmentIdentifier> ignorableSegments,
      final MetricsSystem metricsSystem,
      final RocksDBMetricsFactory rocksDBMetricsFactory)
      throws StorageException {

    this.configuration = configuration;
    this.metricsSystem = metricsSystem;
    this.rocksDBMetricsFactory = rocksDBMetricsFactory;

    // Initialize per-column configuration with recommended defaults
    this.perColumnConfig = initializePerColumnConfig();

    try {
      // Create 16 sharded databases for each segment.
      for (SegmentIdentifier segment : segments) {
        createDatabasesForSegment(segment);
      }
    } catch (RocksDBException e) {
      // Close any opened databases before throwing
      close();
      throw new StorageException("Failed to initialize separate RocksDB instances", e);
    } catch (StorageException e) {
      // Close any opened databases before throwing
      close();
      throw e;
    }
  }

  /**
   * Initializes per-column configuration with optimized defaults.
   *
   * @return the per-column configuration
   */
  private org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.PerColumnConfiguration
      initializePerColumnConfig() {
    LOG.info("Initializing optimized per-column RocksDB configuration");
    return org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.PerColumnConfiguration
        .OptimizedConfigs.createRecommendedConfig();
  }

  /**
   * Creates a separate RocksDB database for a specific segment.
   *
   * @param segment the segment identifier
   * @throws RocksDBException if database creation fails
   */
  private void createDatabasesForSegment(final SegmentIdentifier segment) throws RocksDBException {
    final int shardCount = shardCountForSegment(segment);
    final ShardBindings bindings = new ShardBindings(shardCount);
    segmentBindings.put(segment, bindings);
    for (int shard = 0; shard < shardCount; shard++) {
      createDatabaseForSegmentShard(segment, shard, bindings);
    }
  }

  private void createDatabaseForSegmentShard(
      final SegmentIdentifier segment, final int shard, final ShardBindings bindings)
      throws RocksDBException {

    // Create a subdirectory for this segment using the segment name
    Path segmentPath =
        configuration.getDatabaseDir().resolve(segment.getName()).resolve(shardDirectory(shard));
    String dbPath = segmentPath.toString();
    final String segmentShardKey = segmentShardKey(segment, shard);

    LOG.info(
        "Creating separate RocksDB instance for segment shard '{}' at {}", segmentShardKey, dbPath);

    // Get column-specific configuration
    org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.PerColumnConfiguration
            .ColumnConfig
        columnConfig = perColumnConfig.getConfigForSegment(segment);

    LOG.info(
        "Segment '{}' optimized config: cache={}MB, maxFiles={}, threads={}",
        segment.getName(),
        columnConfig.getCacheCapacity() / (1024 * 1024),
        columnConfig.getMaxOpenFiles(),
        columnConfig.getBackgroundThreadCount());

    // Create the directory if it doesn't exist
    try {
      java.nio.file.Files.createDirectories(segmentPath);
    } catch (java.io.IOException e) {
      throw new StorageException("Failed to create directory for segment: " + segment.getName(), e);
    }

    // Create options for this segment's database
    Statistics stats = new Statistics();
    segmentStats.put(segmentShardKey, stats);

    DBOptions dbOptions = createDBOptions(segmentShardKey, stats, columnConfig);
    TransactionDBOptions txOptions = new TransactionDBOptions();

    // Create column family options for the default column family
    ColumnFamilyOptions cfOptions =
        createColumnFamilyOptions(segmentShardKey, segment, columnConfig);

    // Create column family descriptor for default column
    ColumnFamilyDescriptor defaultCfDescriptor =
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions);

    java.util.List<ColumnFamilyDescriptor> cfDescriptors = java.util.List.of(defaultCfDescriptor);
    java.util.List<ColumnFamilyHandle> cfHandles = new java.util.ArrayList<>();

    // Open the database
    TransactionDB db =
        RocksDBOpener.openTransactionDBWithWarning(
            dbOptions, txOptions, dbPath, cfDescriptors, cfHandles);

    databases.put(segmentShardKey, db);
    defaultColumnHandles.put(segmentShardKey, cfHandles.get(0));
    bindings.databases[shard] = db;
    bindings.handles[shard] = cfHandles.get(0);

    // Initialize metrics for this segment
    RocksDBMetrics metrics = rocksDBMetricsFactory.create(metricsSystem, configuration, db, stats);
    segmentMetrics.put(segmentShardKey, metrics);
    bindings.metrics[shard] = metrics;

    LOG.debug("Successfully created RocksDB instance for segment shard '{}'", segmentShardKey);
  }

  /**
   * Creates DBOptions for a segment database with column-specific configuration.
   *
   * @param stats the statistics object
   * @param columnConfig the column-specific configuration
   * @return configured DBOptions
   */
  private DBOptions createDBOptions(
      final String segmentShardKey,
      final Statistics stats,
      final org.hyperledger.besu.plugin.services.storage.rocksdb.configuration
              .PerColumnConfiguration.ColumnConfig
          columnConfig) {
    DBOptions options = new DBOptions();
    options
        .setCreateIfMissing(true)
        .setMaxOpenFiles(columnConfig.getMaxOpenFiles())
        .setStatistics(stats)
        .setCreateMissingColumnFamilies(true)
        .setLogFileTimeToRoll(TIME_TO_ROLL_LOG_FILE)
        .setKeepLogFileNum(NUMBER_OF_LOG_FILES_TO_KEEP)
        .setEnv(Env.getDefault().setBackgroundThreads(columnConfig.getBackgroundThreadCount()))
        .setMaxTotalWalSize(WAL_MAX_TOTAL_SIZE)
        .setRecycleLogFileNum(WAL_MAX_TOTAL_SIZE / EXPECTED_WAL_FILE_SIZE);

    // Configure row cache if specified
    columnConfig
        .getRowCacheSize()
        .ifPresent(
            rowCacheSize -> {
              LOG.info(
                  "Enabling row cache for segment shard '{}' with size {}MB",
                  segmentShardKey,
                  rowCacheSize / (1024 * 1024));
              org.rocksdb.Cache rowCache = new org.rocksdb.LRUCache(rowCacheSize);
              segmentRowCaches.put(segmentShardKey, rowCache);
              options.setRowCache(rowCache);
            });

    return options;
  }

  /**
   * Creates ColumnFamilyOptions for a segment with column-specific configuration.
   *
   * @param segment the segment identifier
   * @param columnConfig the column-specific configuration
   * @return configured ColumnFamilyOptions
   */
  private ColumnFamilyOptions createColumnFamilyOptions(
      final String segmentShardKey,
      final SegmentIdentifier segment,
      final org.hyperledger.besu.plugin.services.storage.rocksdb.configuration
              .PerColumnConfiguration.ColumnConfig
          columnConfig) {
    BlockBasedTableConfig tableConfig = createBlockBasedTableConfig(segmentShardKey, columnConfig);

    ColumnFamilyOptions options =
        new ColumnFamilyOptions()
            .setTtl(0)
            .setCompressionType(CompressionType.LZ4_COMPRESSION)
            .setTableFormatConfig(tableConfig);

    // Note: Row cache is configured via BlockBasedTableConfig, not here

    // Apply write buffer size if specified
    columnConfig.getWriteBufferSize().ifPresent(options::setWriteBufferSize);

    // Apply max write buffer number if specified
    columnConfig.getMaxWriteBufferNumber().ifPresent(options::setMaxWriteBufferNumber);

    // Apply level compaction dynamic level bytes if specified (expects boolean)
    columnConfig
        .getLevelCompactionDynamicLevelBytes()
        .ifPresent(dynLevel -> options.setLevelCompactionDynamicLevelBytes(dynLevel > 0));

    // Apply target file size base if specified
    columnConfig
        .getTargetFileSizeBase()
        .ifPresent(size -> options.setTargetFileSizeBase((long) size));

    // Configure BlobDB for segments with static data
    if (segment.containsStaticData()) {
      configureBlobDB(segment, options);
    }

    LOG.debug(
        "Created ColumnFamilyOptions for segment shard '{}' with cache={}, writeBuffer={}, rowCache={}",
        segmentShardKey,
        columnConfig.getCacheCapacity(),
        columnConfig.getWriteBufferSize().orElse(0),
        columnConfig.getRowCacheSize().orElse(0L));

    return options;
  }

  /**
   * Creates BlockBasedTableConfig for a segment with column-specific configuration.
   *
   * @param columnConfig the column-specific configuration
   * @return configured BlockBasedTableConfig
   */
  private BlockBasedTableConfig createBlockBasedTableConfig(
      final String segmentShardKey,
      final org.hyperledger.besu.plugin.services.storage.rocksdb.configuration
              .PerColumnConfiguration.ColumnConfig
          columnConfig) {
    final org.rocksdb.Cache blockCache = new LRUCache(columnConfig.getCacheCapacity());
    segmentBlockCaches.put(segmentShardKey, blockCache);

    BlockBasedTableConfig tableConfig =
        new BlockBasedTableConfig()
            .setFormatVersion(ROCKSDB_FORMAT_VERSION)
            .setBlockCache(blockCache)
            .setFilterPolicy(new BloomFilter(10, false))
            .setPartitionFilters(true)
            .setCacheIndexAndFilterBlocks(false)
            .setBlockSize(ROCKSDB_BLOCK_SIZE);

    LOG.debug(
        "Created BlockBasedTableConfig for segment shard '{}' with blockCache={}MB",
        segmentShardKey,
        columnConfig.getCacheCapacity() / (1024 * 1024));

    return tableConfig;
  }

  /**
   * Configures BlobDB settings for segments with static data.
   *
   * @param segment the segment identifier
   * @param options the column family options to configure
   */
  private void configureBlobDB(final SegmentIdentifier segment, final ColumnFamilyOptions options) {
    options
        .setEnableBlobFiles(true)
        .setEnableBlobGarbageCollection(segment.isStaticDataGarbageCollectionEnabled())
        .setMinBlobSize(100)
        .setBlobCompressionType(CompressionType.LZ4_COMPRESSION);

    if (configuration.getBlobGarbageCollectionAgeCutoff().isPresent()) {
      options.setBlobGarbageCollectionAgeCutoff(
          configuration.getBlobGarbageCollectionAgeCutoff().get());
    }
    if (configuration.getBlobGarbageCollectionForceThreshold().isPresent()) {
      options.setBlobGarbageCollectionForceThreshold(
          configuration.getBlobGarbageCollectionForceThreshold().get());
    }
  }

  /**
   * Gets the database instance for a segment.
   *
   * @param segment the segment identifier
   * @return the RocksDB instance
   */
  private TransactionDB getDatabase(final SegmentIdentifier segment, final int shard) {
    final ShardBindings bindings = segmentBindings.get(segment);
    if (bindings == null || shard < 0 || shard >= bindings.databases.length) {
      throw new IllegalArgumentException(
          "No shard bindings found for segment/shard: " + segment.getName() + "#" + shard);
    }
    TransactionDB db = bindings.databases[shard];
    if (db == null) {
      throw new IllegalArgumentException(
          "No database found for segment shard: " + segment.getName() + "#" + shard);
    }
    return db;
  }

  /**
   * Gets the default column handle for a segment's database.
   *
   * @param segment the segment identifier
   * @return the column family handle
   */
  private ColumnFamilyHandle getColumnHandle(final SegmentIdentifier segment, final int shard) {
    final ShardBindings bindings = segmentBindings.get(segment);
    if (bindings == null || shard < 0 || shard >= bindings.handles.length) {
      throw new IllegalArgumentException(
          "No shard bindings found for segment/shard: " + segment.getName() + "#" + shard);
    }
    ColumnFamilyHandle handle = bindings.handles[shard];
    if (handle == null) {
      throw new IllegalArgumentException(
          "No column handle found for segment shard: " + segment.getName() + "#" + shard);
    }
    return handle;
  }

  /**
   * Gets the metrics for a segment.
   *
   * @param segment the segment identifier
   * @return the RocksDB metrics
   */
  private RocksDBMetrics getMetrics(final SegmentIdentifier segment, final int shard) {
    final ShardBindings bindings = segmentBindings.get(segment);
    if (bindings == null || shard < 0 || shard >= bindings.metrics.length) {
      throw new IllegalArgumentException(
          "No shard bindings found for segment/shard: " + segment.getName() + "#" + shard);
    }
    RocksDBMetrics metrics = bindings.metrics[shard];
    if (metrics == null) {
      throw new IllegalArgumentException(
          "No metrics found for segment shard: " + segment.getName() + "#" + shard);
    }
    return metrics;
  }

  @Override
  public Optional<byte[]> get(final SegmentIdentifier segment, final byte[] key)
      throws StorageException {
    throwIfClosed();
    final int shard = shardForSegment(segment, key);

    try (final OperationTimer.TimingContext ignored =
        getMetrics(segment, shard).getReadLatency().startTimer()) {
      TransactionDB db = getDatabase(segment, shard);
      ColumnFamilyHandle handle = getColumnHandle(segment, shard);
      return Optional.ofNullable(db.get(handle, readOptions, key));
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Optional<NearestKeyValue> getNearestBefore(
      final SegmentIdentifier segment, final Bytes key) throws StorageException {
    throwIfClosed();
    return stream(segment)
        .map(p -> new NearestKeyValue(Bytes.wrap(p.getKey()), Optional.of(p.getValue())))
        .filter(kv -> Arrays.compareUnsigned(kv.key().toArrayUnsafe(), key.toArrayUnsafe()) <= 0)
        .max((a, b) -> Arrays.compareUnsigned(a.key().toArrayUnsafe(), b.key().toArrayUnsafe()));
  }

  @Override
  public Optional<NearestKeyValue> getNearestAfter(final SegmentIdentifier segment, final Bytes key)
      throws StorageException {
    throwIfClosed();
    return stream(segment)
        .map(p -> new NearestKeyValue(Bytes.wrap(p.getKey()), Optional.of(p.getValue())))
        .filter(kv -> Arrays.compareUnsigned(kv.key().toArrayUnsafe(), key.toArrayUnsafe()) >= 0)
        .min((a, b) -> Arrays.compareUnsigned(a.key().toArrayUnsafe(), b.key().toArrayUnsafe()));
  }

  @Override
  public Stream<Pair<byte[], byte[]>> stream(final SegmentIdentifier segment) {
    throwIfClosed();
    return streamAllShards(segment)
        .sorted((a, b) -> Arrays.compareUnsigned(a.getKey(), b.getKey()));
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segment, final byte[] startKey) {
    throwIfClosed();
    return stream(segment).filter(e -> Arrays.compareUnsigned(e.getKey(), startKey) >= 0);
  }

  @Override
  public Stream<Pair<byte[], byte[]>> streamFromKey(
      final SegmentIdentifier segment, final byte[] startKey, final byte[] endKey) {
    throwIfClosed();
    return stream(segment)
        .filter(e -> Arrays.compareUnsigned(e.getKey(), startKey) >= 0)
        .filter(e -> Arrays.compareUnsigned(e.getKey(), endKey) <= 0);
  }

  @Override
  public Stream<byte[]> streamKeys(final SegmentIdentifier segment) {
    throwIfClosed();
    return stream(segment).map(Pair::getKey);
  }

  @Override
  public boolean tryDelete(final SegmentIdentifier segment, final byte[] key) {
    throwIfClosed();
    final int shard = shardForSegment(segment, key);

    try {
      TransactionDB db = getDatabase(segment, shard);
      ColumnFamilyHandle handle = getColumnHandle(segment, shard);
      db.delete(handle, tryDeleteOptions, key);
      return true;
    } catch (RocksDBException e) {
      if (e.getStatus().getCode() == Status.Code.Incomplete) {
        return false;
      } else {
        throw new StorageException(e);
      }
    }
  }

  @Override
  public Set<byte[]> getAllKeysThat(
      final SegmentIdentifier segment, final Predicate<byte[]> returnCondition) {
    return stream(segment)
        .filter(pair -> returnCondition.test(pair.getKey()))
        .map(Pair::getKey)
        .collect(toUnmodifiableSet());
  }

  @Override
  public Set<byte[]> getAllValuesFromKeysThat(
      final SegmentIdentifier segment, final Predicate<byte[]> returnCondition) {
    return stream(segment)
        .filter(pair -> returnCondition.test(pair.getKey()))
        .map(Pair::getValue)
        .collect(toUnmodifiableSet());
  }

  @Override
  public void clear(final SegmentIdentifier segment) {
    throwIfClosed();

    try {
      for (int shard = 0; shard < shardCountForSegment(segment); shard++) {
        TransactionDB db = getDatabase(segment, shard);
        ColumnFamilyHandle handle = getColumnHandle(segment, shard);

        // Delete all keys in this segment shard.
        try (final RocksIterator iterator = db.newIterator(handle)) {
          iterator.seekToFirst();
          while (iterator.isValid()) {
            db.delete(handle, iterator.key());
            iterator.next();
          }
        }
      }
    } catch (RocksDBException e) {
      throw new StorageException("Failed to clear segment: " + segment.getName(), e);
    }
  }

  @Override
  public SegmentedKeyValueStorageTransaction startTransaction() throws StorageException {
    throwIfClosed();

    return new SegmentedKeyValueStorageTransactionValidatorDecorator(
        new SeparateDBRocksDBTransaction(), this.closed::get);
  }

  /**
   * Take snapshot of the storage.
   *
   * <p>Creates a snapshot across all segment databases. This provides a consistent point-in-time
   * view of all data across all segments.
   *
   * @return the snapshot
   * @throws StorageException if snapshot creation fails
   */
  @Override
  public SnappedKeyValueStorage takeSnapshot() throws StorageException {
    throwIfClosed();

    return new SeparateDBRocksDBSnapshot(
        databases,
        defaultColumnHandles,
        segmentMetrics,
        configuration.isReadCacheEnabledForSnapshots());
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      LOG.info("Closing {} separate RocksDB instances", databases.size());

      // Close all column handles
      defaultColumnHandles.values().forEach(ColumnFamilyHandle::close);

      // Close all databases
      databases.values().forEach(TransactionDB::close);

      // Close all block caches
      segmentBlockCaches.values().forEach(org.rocksdb.Cache::close);

      // Close all row caches
      segmentRowCaches.values().forEach(org.rocksdb.Cache::close);

      // Clear collections
      databases.clear();
      defaultColumnHandles.clear();
      segmentMetrics.clear();
      segmentStats.clear();
      segmentBlockCaches.clear();
      segmentRowCaches.clear();
      segmentBindings.clear();

      tryDeleteOptions.close();
      readOptions.close();
    }
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  void throwIfClosed() {
    if (closed.get()) {
      LOG.error("Attempting to use a closed SeparateDBRocksDBColumnarKeyValueStorage");
      throw new IllegalStateException("Storage has been closed");
    }
  }

  /** Transaction implementation for separate database architecture. */
  private class SeparateDBRocksDBTransaction implements SegmentedKeyValueStorageTransaction {

    private final Map<SegmentIdentifier, org.rocksdb.Transaction[]> transactions = new HashMap<>();
    private final Map<SegmentIdentifier, WriteOptions[]> writeOptions = new HashMap<>();

    SeparateDBRocksDBTransaction() {
      // Transactions are created lazily per segment when needed
    }

    private org.rocksdb.Transaction getTransaction(
        final SegmentIdentifier segment, final byte[] key) {
      final int shard = shardForSegment(segment, key);
      final int shardCount = shardCountForSegment(segment);
      final org.rocksdb.Transaction[] txArray =
          transactions.computeIfAbsent(segment, ignored -> new org.rocksdb.Transaction[shardCount]);
      if (txArray[shard] == null) {
        final WriteOptions[] optionsArray =
            writeOptions.computeIfAbsent(segment, ignored -> new WriteOptions[shardCount]);
        final WriteOptions wo = new WriteOptions();
        wo.setIgnoreMissingColumnFamilies(true);
        optionsArray[shard] = wo;
        txArray[shard] = getDatabase(segment, shard).beginTransaction(wo);
      }
      return txArray[shard];
    }

    @Override
    public void put(final SegmentIdentifier segment, final byte[] key, final byte[] value) {
      try {
        org.rocksdb.Transaction tx = getTransaction(segment, key);
        ColumnFamilyHandle handle = getColumnHandle(segment, shardForSegment(segment, key));
        tx.put(handle, key, value);
      } catch (RocksDBException e) {
        throw new StorageException(e);
      }
    }

    @Override
    public void remove(final SegmentIdentifier segment, final byte[] key) {
      try {
        org.rocksdb.Transaction tx = getTransaction(segment, key);
        ColumnFamilyHandle handle = getColumnHandle(segment, shardForSegment(segment, key));
        tx.delete(handle, key);
      } catch (RocksDBException e) {
        throw new StorageException(e);
      }
    }

    @Override
    public void commit() throws StorageException {
      try {
        // Commit all transactions
        for (org.rocksdb.Transaction[] txArray : transactions.values()) {
          for (org.rocksdb.Transaction tx : txArray) {
            if (tx != null) {
              tx.commit();
            }
          }
        }
      } catch (RocksDBException e) {
        throw new StorageException(e);
      } finally {
        close();
      }
    }

    @Override
    public void rollback() {
      try {
        // Rollback all transactions
        for (org.rocksdb.Transaction[] txArray : transactions.values()) {
          for (org.rocksdb.Transaction tx : txArray) {
            if (tx != null) {
              tx.rollback();
            }
          }
        }
      } catch (RocksDBException e) {
        LOG.error("Failed to rollback transaction", e);
      } finally {
        close();
      }
    }

    @Override
    public void close() {
      transactions
          .values()
          .forEach(
              txArray -> {
                for (org.rocksdb.Transaction tx : txArray) {
                  if (tx != null) {
                    tx.close();
                  }
                }
              });
      writeOptions
          .values()
          .forEach(
              optionsArray -> {
                for (WriteOptions wo : optionsArray) {
                  if (wo != null) {
                    wo.close();
                  }
                }
              });
      transactions.clear();
      writeOptions.clear();
    }
  }

  private Stream<Pair<byte[], byte[]>> streamAllShards(final SegmentIdentifier segment) {
    Stream<Pair<byte[], byte[]>> stream = Stream.empty();
    for (int shard = 0; shard < shardCountForSegment(segment); shard++) {
      final TransactionDB db = getDatabase(segment, shard);
      final ColumnFamilyHandle handle = getColumnHandle(segment, shard);
      final RocksIterator rocksIterator = db.newIterator(handle);
      rocksIterator.seekToFirst();
      stream = Stream.concat(stream, RocksDbIterator.create(rocksIterator).toStream());
    }
    return stream;
  }

  private static int shardIndex(final byte[] key) {
    if (key == null || key.length == 0) {
      return 0;
    }
    return (key[0] & 0xFF) >>> 4;
  }

  private static boolean isShardedSegment(final SegmentIdentifier segment) {
    return SHARDED_SEGMENT_NAME.equals(segment.getName());
  }

  private static int shardCountForSegment(final SegmentIdentifier segment) {
    return isShardedSegment(segment) ? KEY_RANGE_SHARDS : 1;
  }

  private static int shardForSegment(final SegmentIdentifier segment, final byte[] key) {
    return isShardedSegment(segment) ? shardIndex(key) : 0;
  }

  private static String shardDirectory(final int shard) {
    final int from = shard << 4;
    final int to = from | 0x0F;
    return String.format("keyrange-%02X-%02X", from, to);
  }

  private static String segmentShardKey(final SegmentIdentifier segment, final int shard) {
    return segment.getName() + "#" + shard;
  }
}
