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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import java.time.Duration;

/**
 * Tuning of the SNAP2 sync.
 *
 * @param pivotDistance blocks between the chain head and a newly selected pivot
 * @param pivotMoveThreshold blocks the head may advance past the pivot distance before moving
 * @param accountChunks parallel ranges the account key space is split into
 * @param responseBytes soft response size of account, storage and bytecode requests
 * @param balResponseBytes soft response size of block access list requests
 * @param balBatchSize block access lists per request
 * @param storageBatchSize accounts per multi-account storage request
 * @param maxStorageChunks parallel ranges a large storage is split into
 * @param codeBatchSize bytecodes per request
 * @param storageBacklog pending storages above which account requests slow down
 * @param maxSnapRequestsPerPeer most concurrent snap requests a peer can be given
 * @param maxEthRequestsPerPeer most concurrent eth requests a peer can be given
 * @param requestTimeout time a peer has to answer a request
 * @param headersPerRequest block headers per request
 * @param bodiesPerRequest most blocks per body or receipt request
 * @param chainParallelism concurrent header requests
 * @param maxBodyRanges most block ranges whose bodies and receipts are downloaded at once
 * @param workerThreads threads decoding and verifying responses
 * @param trieBuildThreads threads building the tries at the end
 * @param minPeers snap/2 peers to wait for before starting
 * @param pollInterval how often the pivot and the chain download check for progress
 */
public record Snap2Configuration(
    int pivotDistance,
    int pivotMoveThreshold,
    int accountChunks,
    int responseBytes,
    int balResponseBytes,
    int balBatchSize,
    int storageBatchSize,
    int maxStorageChunks,
    int codeBatchSize,
    int storageBacklog,
    int maxSnapRequestsPerPeer,
    int maxEthRequestsPerPeer,
    Duration requestTimeout,
    int headersPerRequest,
    int bodiesPerRequest,
    int chainParallelism,
    int maxBodyRanges,
    int workerThreads,
    int trieBuildThreads,
    int minPeers,
    Duration pollInterval) {

  /** Returns the default configuration. */
  public static Snap2Configuration getDefault() {
    final int cores = Runtime.getRuntime().availableProcessors();
    return new Snap2Configuration(
        64,
        32,
        128,
        512 * 1024,
        2 * 1024 * 1024,
        32,
        256,
        64,
        64,
        50_000,
        6,
        6,
        Duration.ofSeconds(15),
        192,
        128,
        64,
        512,
        Math.max(2, cores),
        Math.max(2, cores),
        1,
        Duration.ofSeconds(2));
  }

  /** Returns a builder starting from this configuration. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  /** Builder of configurations, mostly for tests. */
  public static final class Builder {
    private int accountChunks;
    private int responseBytes;
    private int storageBatchSize;
    private int maxStorageChunks;
    private int codeBatchSize;
    private int pivotDistance;
    private int pivotMoveThreshold;
    private Duration requestTimeout;
    private int workerThreads;
    private int trieBuildThreads;
    private Duration pollInterval;
    private final Snap2Configuration base;

    private Builder(final Snap2Configuration base) {
      this.base = base;
      this.accountChunks = base.accountChunks();
      this.responseBytes = base.responseBytes();
      this.storageBatchSize = base.storageBatchSize();
      this.maxStorageChunks = base.maxStorageChunks();
      this.codeBatchSize = base.codeBatchSize();
      this.pivotDistance = base.pivotDistance();
      this.pivotMoveThreshold = base.pivotMoveThreshold();
      this.requestTimeout = base.requestTimeout();
      this.workerThreads = base.workerThreads();
      this.trieBuildThreads = base.trieBuildThreads();
      this.pollInterval = base.pollInterval();
    }

    public Builder accountChunks(final int value) {
      accountChunks = value;
      return this;
    }

    public Builder responseBytes(final int value) {
      responseBytes = value;
      return this;
    }

    public Builder storageBatchSize(final int value) {
      storageBatchSize = value;
      return this;
    }

    public Builder maxStorageChunks(final int value) {
      maxStorageChunks = value;
      return this;
    }

    public Builder codeBatchSize(final int value) {
      codeBatchSize = value;
      return this;
    }

    public Builder pivotDistance(final int value) {
      pivotDistance = value;
      return this;
    }

    public Builder pivotMoveThreshold(final int value) {
      pivotMoveThreshold = value;
      return this;
    }

    public Builder requestTimeout(final Duration value) {
      requestTimeout = value;
      return this;
    }

    public Builder workerThreads(final int value) {
      workerThreads = value;
      return this;
    }

    public Builder trieBuildThreads(final int value) {
      trieBuildThreads = value;
      return this;
    }

    public Builder pollInterval(final Duration value) {
      pollInterval = value;
      return this;
    }

    public Snap2Configuration build() {
      return new Snap2Configuration(
          pivotDistance,
          pivotMoveThreshold,
          accountChunks,
          responseBytes,
          base.balResponseBytes(),
          base.balBatchSize(),
          storageBatchSize,
          maxStorageChunks,
          codeBatchSize,
          base.storageBacklog(),
          base.maxSnapRequestsPerPeer(),
          base.maxEthRequestsPerPeer(),
          requestTimeout,
          base.headersPerRequest(),
          base.bodiesPerRequest(),
          base.chainParallelism(),
          base.maxBodyRanges(),
          workerThreads,
          trieBuildThreads,
          base.minPeers(),
          pollInterval);
    }
  }
}
