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
package org.hyperledger.besu.plugin.services.storage.rocksdb;

import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;
import org.hyperledger.besu.services.kvstore.ComposedSegmentedKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bonsai layout with separate RocksDB instances:
 *
 * <ul>
 *   <li>{@code database/} — blockchain, trie logs, snapsync, variables, …
 *   <li>{@code bonsai_flat/} — account, storage slots, code
 *   <li>{@code bonsai_trie_hot/} — live trie branch nodes (location-keyed)
 * </ul>
 *
 * Frozen snap-sync trie nodes stay in {@code frozen_snap_trie_nodes/} (separate store).
 *
 * <p>Existing monolithic {@code database/} deployments without split directories keep using a
 * single RocksDB until a fresh data dir or explicit split paths are present.
 */
public class BonsaiSplitRocksDBKeyValueStorageFactory implements KeyValueStorageFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(BonsaiSplitRocksDBKeyValueStorageFactory.class);

  /** {@link org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier} ids. */
  private static final Set<Byte> FLAT_SEGMENT_IDS = Set.of((byte) 6, (byte) 7, (byte) 8);

  private static final byte TRIE_BRANCH_SEGMENT_ID = 9;

  private static final String FLAT_DB_DIR = "bonsai_flat";
  private static final String TRIE_HOT_DB_DIR = "bonsai_trie_hot";

  private final Supplier<RocksDBFactoryConfiguration> configuration;
  private final List<SegmentIdentifier> allSegments;
  private final List<SegmentIdentifier> ignorableSegments;
  private final RocksDBMetricsFactory rocksDBMetricsFactory;

  private final List<SegmentIdentifier> flatSegments;
  private final List<SegmentIdentifier> trieHotSegments;
  private final List<SegmentIdentifier> mainSegments;

  private volatile boolean splitLayoutActive;
  private volatile RocksDBKeyValueStorageFactory monolithicFactory;
  private volatile PathOverrideRocksDBKeyValueStorageFactory mainFactory;
  private volatile PathOverrideRocksDBKeyValueStorageFactory flatFactory;
  private volatile PathOverrideRocksDBKeyValueStorageFactory trieHotFactory;

  public BonsaiSplitRocksDBKeyValueStorageFactory(
      final Supplier<RocksDBFactoryConfiguration> configuration,
      final List<SegmentIdentifier> allSegments,
      final List<SegmentIdentifier> ignorableSegments,
      final RocksDBMetricsFactory rocksDBMetricsFactory) {
    this.configuration = configuration;
    this.allSegments = List.copyOf(allSegments);
    this.ignorableSegments = List.copyOf(ignorableSegments);
    this.rocksDBMetricsFactory = rocksDBMetricsFactory;
    this.flatSegments = allSegments.stream().filter(BonsaiSplitRocksDBKeyValueStorageFactory::isFlatSegment).toList();
    this.trieHotSegments =
        allSegments.stream().filter(BonsaiSplitRocksDBKeyValueStorageFactory::isTrieHotSegment).toList();
    this.mainSegments =
        allSegments.stream()
            .filter(
                segment ->
                    !isFlatSegment(segment)
                        && !isTrieHotSegment(segment))
            .toList();
  }

  @Override
  public String getName() {
    return "rocksdb";
  }

  @Override
  public KeyValueStorage create(
      final SegmentIdentifier segment,
      final BesuConfiguration commonConfiguration,
      final MetricsSystem metricsSystem)
      throws StorageException {
    return new SegmentedKeyValueStorageAdapter(
        segment, create(List.of(segment), commonConfiguration, metricsSystem));
  }

  @Override
  public SegmentedKeyValueStorage create(
      final List<SegmentIdentifier> segments,
      final BesuConfiguration commonConfiguration,
      final MetricsSystem metricsSystem)
      throws StorageException {
    initLayout(commonConfiguration);

    if (!splitLayoutActive) {
      return monolithicFactory.create(segments, commonConfiguration, metricsSystem);
    }

    final List<SegmentIdentifier> flat = segments.stream().filter(BonsaiSplitRocksDBKeyValueStorageFactory::isFlatSegment).toList();
    final List<SegmentIdentifier> trieHot =
        segments.stream().filter(BonsaiSplitRocksDBKeyValueStorageFactory::isTrieHotSegment).toList();
    final List<SegmentIdentifier> main =
        segments.stream()
            .filter(
                segment ->
                    !isFlatSegment(segment) && !isTrieHotSegment(segment))
            .toList();

    final Map<SegmentIdentifier, SegmentedKeyValueStorage> routing = new HashMap<>();
    if (!main.isEmpty()) {
      final SegmentedKeyValueStorage storage =
          mainFactory.create(main, commonConfiguration, metricsSystem);
      main.forEach(segment -> routing.put(segment, storage));
    }
    if (!flat.isEmpty()) {
      final SegmentedKeyValueStorage storage =
          flatFactory.create(flat, commonConfiguration, metricsSystem);
      flat.forEach(segment -> routing.put(segment, storage));
    }
    if (!trieHot.isEmpty()) {
      final SegmentedKeyValueStorage storage =
          trieHotFactory.create(trieHot, commonConfiguration, metricsSystem);
      trieHot.forEach(segment -> routing.put(segment, storage));
    }

    final List<SegmentedKeyValueStorage> uniqueBackends =
        routing.values().stream().distinct().toList();
    if (uniqueBackends.size() == 1) {
      return uniqueBackends.get(0);
    }
    return new ComposedSegmentedKeyValueStorage(routing);
  }

  @Override
  public void close() throws IOException {
    IOException first = null;
    for (final RocksDBKeyValueStorageFactory factory :
        List.of(monolithicFactory, mainFactory, flatFactory, trieHotFactory)) {
      if (factory == null) {
        continue;
      }
      try {
        factory.close();
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
  public boolean isSegmentIsolationSupported() {
    return true;
  }

  @Override
  public boolean isSnapshotIsolationSupported() {
    return true;
  }

  /** Resets underlying factories for Ephemery automatic restart. */
  public void reset() {
    if (monolithicFactory != null) {
      monolithicFactory.reset();
    }
    if (mainFactory != null) {
      mainFactory.reset();
    }
    if (flatFactory != null) {
      flatFactory.reset();
    }
    if (trieHotFactory != null) {
      trieHotFactory.reset();
    }
  }

  private synchronized void initLayout(final BesuConfiguration commonConfiguration) {
    if (monolithicFactory != null || mainFactory != null) {
      return;
    }

    splitLayoutActive = shouldUseSplitLayout(commonConfiguration);
    if (splitLayoutActive) {
      LOG.info(
          "Using split Bonsai RocksDB layout: main={}, flat={}, trie_hot={}",
          commonConfiguration.getStoragePath(),
          commonConfiguration.getDataPath().resolve(FLAT_DB_DIR),
          commonConfiguration.getDataPath().resolve(TRIE_HOT_DB_DIR));
      mainFactory =
          new PathOverrideRocksDBKeyValueStorageFactory(
              configuration,
              mainSegments,
              ignorableSegments,
              rocksDBMetricsFactory,
              BesuConfiguration::getStoragePath);
      flatFactory =
          new PathOverrideRocksDBKeyValueStorageFactory(
              configuration,
              flatSegments,
              ignorableSegments,
              rocksDBMetricsFactory,
              config -> config.getDataPath().resolve(FLAT_DB_DIR));
      trieHotFactory =
          new PathOverrideRocksDBKeyValueStorageFactory(
              configuration,
              trieHotSegments,
              ignorableSegments,
              rocksDBMetricsFactory,
              config -> config.getDataPath().resolve(TRIE_HOT_DB_DIR));
    } else {
      LOG.info("Using monolithic RocksDB layout at {}", commonConfiguration.getStoragePath());
      monolithicFactory =
          new RocksDBKeyValueStorageFactory(
              configuration, allSegments, ignorableSegments, rocksDBMetricsFactory);
    }
  }

  private static boolean shouldUseSplitLayout(final BesuConfiguration config) {
    final DataStorageFormat format = config.getDataStorageConfiguration().getDatabaseFormat();
    if (format != DataStorageFormat.BONSAI && format != DataStorageFormat.X_BONSAI_ARCHIVE) {
      return false;
    }
    final Path dataPath = config.getDataPath();
    if (Files.exists(dataPath.resolve(FLAT_DB_DIR))
        || Files.exists(dataPath.resolve(TRIE_HOT_DB_DIR))) {
      return true;
    }
    return !config.getStoragePath().toFile().exists();
  }

  private static boolean isFlatSegment(final SegmentIdentifier segment) {
    final byte[] id = segment.getId();
    return id.length == 1 && FLAT_SEGMENT_IDS.contains(id[0]);
  }

  private static boolean isTrieHotSegment(final SegmentIdentifier segment) {
    final byte[] id = segment.getId();
    return id.length == 1 && id[0] == TRIE_BRANCH_SEGMENT_ID;
  }
}
