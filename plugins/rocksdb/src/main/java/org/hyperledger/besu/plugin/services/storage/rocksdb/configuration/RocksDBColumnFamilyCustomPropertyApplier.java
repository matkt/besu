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
package org.hyperledger.besu.plugin.services.storage.rocksdb.configuration;

import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies RocksDB column-family custom properties (RocksDB {@code ConfigOptions} / INI-style keys).
 */
public final class RocksDBColumnFamilyCustomPropertyApplier {

  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBColumnFamilyCustomPropertyApplier.class);

  /** RocksDB property key for index/filter open prefetch (not registered in rocksdbjni 10.6.2). */
  public static final String PREFETCH_INDEX_AND_FILTER_IN_CACHE_KEY =
      "block_based_table_factory.prefetch_index_and_filter_in_cache";

  private static final String CACHE_INDEX_AND_FILTER_BLOCKS_KEY =
      "block_based_table_factory.cache_index_and_filter_blocks";

  private RocksDBColumnFamilyCustomPropertyApplier() {}

  /**
   * Merges configured custom properties into the column family and block-based table options.
   *
   * @param segment column family segment
   * @param tableConfig block-based table configuration for the segment
   * @param cfOptions column family options for the segment
   * @param prefetchIndexAndFilterInCache value for {@link #PREFETCH_INDEX_AND_FILTER_IN_CACHE_KEY}
   * @param customProperties additional {@code key=value} entries; optional {@code segment:key=value}
   *     limits a property to one column family (segment name or {@code 0xNN} id)
   */
  public static void apply(
      final SegmentIdentifier segment,
      final BlockBasedTableConfig tableConfig,
      final ColumnFamilyOptions cfOptions,
      final boolean prefetchIndexAndFilterInCache,
      final List<String> customProperties) {
    final Properties properties = new Properties();
    properties.setProperty(
        PREFETCH_INDEX_AND_FILTER_IN_CACHE_KEY, Boolean.toString(prefetchIndexAndFilterInCache));

    for (final String entry : customProperties) {
      final Properties scoped = parsePropertyEntry(entry, segment);
      for (final String key : scoped.stringPropertyNames()) {
        properties.setProperty(key, scoped.getProperty(key));
      }
    }

    applyPrefetchIndexAndFilterInCache(segment, tableConfig, properties);
    applyCacheIndexAndFilterBlocks(tableConfig, properties);
    applyRemainingColumnFamilyProperties(cfOptions, properties);
  }

  private static void applyPrefetchIndexAndFilterInCache(
      final SegmentIdentifier segment,
      final BlockBasedTableConfig tableConfig,
      final Properties properties) {
    final String raw = properties.remove(PREFETCH_INDEX_AND_FILTER_IN_CACHE_KEY).toString();
    if (parseBoolean(raw)) {
      return;
    }
    if (segment.isCacheIndexAndFilterBlocks()) {
      return;
    }
    // rocksdbjni 10.6.2 does not expose prefetch_index_and_filter_in_cache on BlockBasedTableOptions
    // and TableCache::Get always passes prefetch=true. Besu maps "false" to caching index/filter
    // blocks so preload_all is disabled on table open (see RocksDB BlockBasedTable::Open).
    LOG.info(
        "Column family {}: {}=false; enabling cache_index_and_filter_blocks (Besu mapping for "
            + "rocksdbjni 10.6.2)",
        segment.getName(),
        PREFETCH_INDEX_AND_FILTER_IN_CACHE_KEY);
    tableConfig.setCacheIndexAndFilterBlocks(true);
  }

  private static void applyCacheIndexAndFilterBlocks(
      final BlockBasedTableConfig tableConfig, final Properties properties) {
    final Object raw = properties.remove(CACHE_INDEX_AND_FILTER_BLOCKS_KEY);
    if (raw == null) {
      return;
    }
    tableConfig.setCacheIndexAndFilterBlocks(parseBoolean(raw.toString()));
  }

  private static void applyRemainingColumnFamilyProperties(
      final ColumnFamilyOptions cfOptions, final Properties properties) {
    if (properties.isEmpty()) {
      return;
    }
    try (final ConfigOptions configOptions = new ConfigOptions().setIgnoreUnknownOptions(true);
        final ColumnFamilyOptions overlay =
            ColumnFamilyOptions.getColumnFamilyOptionsFromProps(configOptions, properties)) {
      if (overlay.tableFactoryName() != null
          && overlay.tableFactoryName().equals(cfOptions.tableFactoryName())) {
        final var overlayTableConfig = overlay.tableFormatConfig();
        if (overlayTableConfig instanceof BlockBasedTableConfig overlayBlockConfig
            && cfOptions.tableFormatConfig() instanceof BlockBasedTableConfig blockConfig) {
          blockConfig.setCacheIndexAndFilterBlocks(overlayBlockConfig.cacheIndexAndFilterBlocks());
          blockConfig.setPinTopLevelIndexAndFilter(
              overlayBlockConfig.pinTopLevelIndexAndFilter());
          blockConfig.setPartitionFilters(overlayBlockConfig.partitionFilters());
          blockConfig.setBlockSize(overlayBlockConfig.blockSize());
        }
      }
    }
  }

  private static Properties parsePropertyEntry(
      final String entry, final SegmentIdentifier segment) {
    final Properties properties = new Properties();
    final int segmentSeparator = entry.indexOf(':');
    if (segmentSeparator > 0 && entry.indexOf('=') > segmentSeparator) {
      final String segmentSelector = entry.substring(0, segmentSeparator).trim();
      if (!matchesSegment(segment, segmentSelector)) {
        return properties;
      }
      putKeyValue(properties, entry.substring(segmentSeparator + 1));
      return properties;
    }
    putKeyValue(properties, entry);
    return properties;
  }

  private static boolean matchesSegment(
      final SegmentIdentifier segment, final String segmentSelector) {
    final String normalized = segmentSelector.toLowerCase(Locale.ROOT);
    if (normalized.equals(segment.getName().toLowerCase(Locale.ROOT))) {
      return true;
    }
    if (normalized.startsWith("0x")) {
      final byte[] id = segment.getId();
      if (id.length == 1) {
        return normalized.equals(String.format("0x%02x", id[0]));
      }
    }
    return false;
  }

  private static void putKeyValue(final Properties properties, final String keyValue) {
    final int equals = keyValue.indexOf('=');
    if (equals <= 0 || equals == keyValue.length() - 1) {
      throw new IllegalArgumentException(
          "Invalid RocksDB column family custom property (expected key=value): " + keyValue);
    }
    properties.setProperty(
        keyValue.substring(0, equals).trim(), keyValue.substring(equals + 1).trim());
  }

  private static boolean parseBoolean(final String raw) {
    return Boolean.parseBoolean(raw) || "1".equals(raw);
  }
}
