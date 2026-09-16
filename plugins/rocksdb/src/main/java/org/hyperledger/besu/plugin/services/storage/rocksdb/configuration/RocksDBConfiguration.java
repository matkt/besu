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

import java.nio.file.Path;
import java.util.Optional;

/** The Rocks db configuration. */
public class RocksDBConfiguration {

  private final Path databaseDir;
  private final int maxOpenFiles;
  private final String label;
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

  public RocksDBConfiguration(
      final Path databaseDir,
      final int maxOpenFiles,
      final int backgroundThreadCount,
      final long cacheCapacity,
      final String label,
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
    this.databaseDir = databaseDir;
    this.maxOpenFiles = maxOpenFiles;
    this.cacheCapacity = cacheCapacity;
    this.label = label;
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

  public Path getDatabaseDir() {
    return databaseDir;
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

  public String getLabel() {
    return label;
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
