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
package org.hyperledger.besu.ethereum.eth.sync.worldstate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.task.GetAccountRangeFromPeerTask;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.services.tasks.CachingTaskCollection;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntSupplier;

public class SnapWorldStateDownloader implements WorldStateDownloadStatus {


  private final EthContext ethContext;
  private final MetricsSystem metricsSystem;

  public SnapWorldStateDownloader(
      final EthContext ethContext,
      final WorldStateStorage worldStateStorage,
      final MetricsSystem metricsSystem) {
    this.ethContext = ethContext;
    System.out.println(worldStateStorage.getClass().getName());
    this.metricsSystem = metricsSystem;
  }


  public CompletableFuture<Void> run(final BlockHeader header) {
    synchronized (this) {
      GetAccountRangeFromPeerTask.forAccountRange(ethContext,header.getStateRoot(), Hash.fromHexString("0000000000000000000000000000000000000000000000000000000000000000"),
              Hash.fromHexString("0fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"), header.getNumber(), metricsSystem)
              .run();
      return CompletableFuture.completedFuture(null);
    }
  }

  public void cancel() {

  }

  @Override
  public Optional<Long> getPulledStates() {
    return Optional.of(0L);
  }

  @Override
  public Optional<Long> getKnownStates() {
    return Optional.of(0L);
  }
}
