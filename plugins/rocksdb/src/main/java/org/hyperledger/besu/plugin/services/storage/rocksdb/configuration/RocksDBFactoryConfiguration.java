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

import java.util.Optional;

/** The RocksDb factory configuration. */
public class RocksDBFactoryConfiguration {

  private final int maxOpenFiles;
  private final int backgroundThreadCount;
  private final long cacheCapacity;
  private final boolean isHighSpec;
  private final boolean enableReadCacheForSnapshots;
  private final boolean isBlockchainGarbageCollectionEnabled;
  private final Optional<Double> blobGarbageCollectionAgeCutoff;
  private final Optional<Double> blobGarbageCollectionForceThreshold;
  private final Optional<Integer> numLevels;
  private final int maxOpenRocksDbSnapshots;
  private final boolean compactStateColumnFamiliesAfterFcu;
  private final boolean stateLsmExperimentEnabled;

  /**
   * Instantiates a new RocksDb factory configuration.
   *
   * @param maxOpenFiles the max open files
   * @param backgroundThreadCount the background thread count
   * @param cacheCapacity the cache capacity
   * @param isHighSpec the is high spec
   * @param enableReadCacheForSnapshots whether read caching is enabled for snapshots
   * @param isBlockchainGarbageCollectionEnabled is garbage collection enabled for the BLOCKCHAIN
   *     column family
   * @param blobGarbageCollectionAgeCutoff the blob garbage collection age cutoff
   * @param blobGarbageCollectionForceThreshold the blob garbage collection force threshold
   * @param numLevels optional RocksDB num_levels override for Bonsai state column families
   * @param maxOpenRocksDbSnapshots max concurrent RocksDB snapshots ({@code 0} = unlimited)
   * @param compactStateColumnFamiliesAfterFcu compact state CFs after successful FCU
   * @param stateLsmExperimentEnabled whether the experimental state LSM bundle is enabled
   */
  public RocksDBFactoryConfiguration(
      final int maxOpenFiles,
      final int backgroundThreadCount,
      final long cacheCapacity,
      final boolean isHighSpec,
      final boolean enableReadCacheForSnapshots,
      final boolean isBlockchainGarbageCollectionEnabled,
      final Optional<Double> blobGarbageCollectionAgeCutoff,
      final Optional<Double> blobGarbageCollectionForceThreshold,
      final Optional<Integer> numLevels,
      final int maxOpenRocksDbSnapshots,
      final boolean compactStateColumnFamiliesAfterFcu,
      final boolean stateLsmExperimentEnabled) {
    this.backgroundThreadCount = backgroundThreadCount;
    this.maxOpenFiles = maxOpenFiles;
    this.cacheCapacity = cacheCapacity;
    this.isHighSpec = isHighSpec;
    this.enableReadCacheForSnapshots = enableReadCacheForSnapshots;
    this.isBlockchainGarbageCollectionEnabled = isBlockchainGarbageCollectionEnabled;
    this.blobGarbageCollectionAgeCutoff = blobGarbageCollectionAgeCutoff;
    this.blobGarbageCollectionForceThreshold = blobGarbageCollectionForceThreshold;
    this.numLevels = numLevels;
    this.maxOpenRocksDbSnapshots = maxOpenRocksDbSnapshots;
    this.compactStateColumnFamiliesAfterFcu = compactStateColumnFamiliesAfterFcu;
    this.stateLsmExperimentEnabled = stateLsmExperimentEnabled;
  }

  public int getMaxOpenFiles() {
    return maxOpenFiles;
  }

  public int getBackgroundThreadCount() {
    return backgroundThreadCount;
  }

  public long getCacheCapacity() {
    return cacheCapacity;
  }

  public boolean isHighSpec() {
    return isHighSpec;
  }

  public boolean isReadCacheEnabledForSnapshots() {
    return enableReadCacheForSnapshots;
  }

  public boolean isBlockchainGarbageCollectionEnabled() {
    return isBlockchainGarbageCollectionEnabled;
  }

  public Optional<Double> getBlobGarbageCollectionAgeCutoff() {
    return blobGarbageCollectionAgeCutoff;
  }

  public Optional<Double> getBlobGarbageCollectionForceThreshold() {
    return blobGarbageCollectionForceThreshold;
  }

  public Optional<Integer> getNumLevels() {
    return numLevels;
  }

  public int getMaxOpenRocksDbSnapshots() {
    return maxOpenRocksDbSnapshots;
  }

  public boolean isCompactStateColumnFamiliesAfterFcu() {
    return compactStateColumnFamiliesAfterFcu;
  }

  public boolean isStateLsmExperimentEnabled() {
    return stateLsmExperimentEnabled;
  }
}
