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

import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BalFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.ChainHeadSource;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.HeaderFetcher;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;

/**
 * What the attempts of a SNAP2 sync share.
 *
 * @param stopped whether the whole sync stopped
 */
record SyncContext(
    Snap2Configuration config,
    ProtocolSchedule protocolSchedule,
    MutableBlockchain blockchain,
    SyncState syncState,
    SyncStateStorage storage,
    ChainHeadSource heads,
    MetricsSystem metricsSystem,
    SyncDurationMetrics syncDurationMetrics,
    boolean headersToCheckpointOnly,
    boolean indexTransactions,
    RequestSender scheduler,
    HeaderFetcher fetcher,
    BalFetcher balFetcher,
    ExecutorService executor,
    ScheduledExecutorService control,
    BooleanSupplier stopped,
    Snap2Metrics metrics) {}
