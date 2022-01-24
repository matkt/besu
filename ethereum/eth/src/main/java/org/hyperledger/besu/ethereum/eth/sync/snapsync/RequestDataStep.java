/*
 * Copyright contributors to Hyperledger Besu
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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.exceptions.EthTaskException;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetAccountRangeFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetBytecodeFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetStorageRangeFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.snap.RetryingGetTrieNodeFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.task.EthTask;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldDownloadState;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.services.tasks.Task;
import org.hyperledger.besu.util.ExceptionUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tuweni.bytes.Bytes;
import org.immutables.value.Value;

public class RequestDataStep {
  private static final Logger LOG = LogManager.getLogger();
  private final BiFunction<SnapDataRequest, BlockHeader, EthTask<? extends AbstractSnapMessageData>>
      requestTaskFactory;

  private final EthContext ethContext;
  private final MetricsSystem metricsSystem;
  private final WorldStateProofProvider worldStateProofProvider;

  RequestDataStep(
      final EthContext ethContext,
      final MetricsSystem metricsSystem,
      final WorldStateStorage worldStateStorage) {
    this(
        getSnapDataFactory(ethContext, metricsSystem),
        ethContext,
        metricsSystem,
        worldStateStorage);
  }

  RequestDataStep(
      final BiFunction<SnapDataRequest, BlockHeader, EthTask<? extends AbstractSnapMessageData>>
          requestTaskFactory,
      final EthContext ethContext,
      final MetricsSystem metricsSystem,
      final WorldStateStorage worldStateStorage) {
    this.ethContext = ethContext;
    this.metricsSystem = metricsSystem;
    this.requestTaskFactory = requestTaskFactory;
    this.worldStateProofProvider = new WorldStateProofProvider(worldStateStorage);
  }

  public CompletableFuture<Task<SnapDataRequest>> requestData(
      final Task<SnapDataRequest> requestTask,
      final SnapSyncState fastSyncState,
      final WorldDownloadState<SnapDataRequest> downloadState) {
    return sendRequest(
            requestTask.getData(), fastSyncState.getPivotBlockHeader().get(), downloadState)
        .thenApply(
            maybeResponse -> {
              maybeResponse.ifPresent(
                  response -> {
                    final SnapDataRequest req = response.snapDataRequest();
                    req.setData(response.abstractSnapMessageData().getData());
                    if (!req.isTaskCompleted(
                        downloadState,
                        fastSyncState,
                        ethContext.getEthPeers(),
                        worldStateProofProvider)) {
                      LOG.trace("Invalid response received retry later");
                      req.setData(Optional.empty()); // invalid response mark Failed
                      downloadState.requestComplete(false, 0);
                    } else {
                      downloadState.requestComplete(true);
                    }
                  });
              return requestTask;
            });
  }

  public CompletableFuture<List<Task<SnapDataRequest>>> requestTrieNodeByPath(
      final List<Task<SnapDataRequest>> requestTasks, final SnapSyncState fastSyncState) {
    final List<Hash> hashes =
        requestTasks.stream()
            .map(Task::getData)
            .map(TrieNodeDataHealRequest.class::cast)
            .map(TrieNodeDataHealRequest::getNodeHash)
            .collect(Collectors.toList());
    final List<List<Bytes>> paths =
        requestTasks.stream()
            .map(Task::getData)
            .map(TrieNodeDataHealRequest.class::cast)
            .map(TrieNodeDataHealRequest::getTrieNodePath)
            .collect(Collectors.toList());
    return RetryingGetTrieNodeFromPeerTask.forTrieNodes(
            ethContext,
            hashes,
            Optional.of(paths),
            fastSyncState.getPivotBlockHeader().get(),
            metricsSystem)
        .run()
        .thenApply(
            data -> {
              for (final Task<SnapDataRequest> task : requestTasks) {
                final TrieNodeDataHealRequest request = (TrieNodeDataHealRequest) task.getData();
                final Bytes matchingData = data.get(request.getNodeHash());
                if (matchingData != null) {
                  request.setData(matchingData);
                }
              }
              return requestTasks;
            });
  }

  private CompletableFuture<Optional<SendRequestResult>> sendRequest(
      final SnapDataRequest request,
      final BlockHeader blockHeader,
      final WorldDownloadState<SnapDataRequest> downloadState) {
    final EthTask<? extends AbstractSnapMessageData> task =
        requestTaskFactory.apply(request, blockHeader);
    downloadState.addOutstandingTask(task);
    return task.run()
        .handle(
            (result, error) -> {
              downloadState.removeOutstandingTask(task);
              if (error != null) {
                final Throwable rootCause = ExceptionUtils.rootCause(error);
                if (!(rootCause instanceof TimeoutException
                    || rootCause instanceof InterruptedException
                    || rootCause instanceof CancellationException
                    || rootCause instanceof EthTaskException)) {}
                return Optional.empty();
              }
              return Optional.of(
                  ImmutableSendRequestResult.builder()
                      .abstractSnapMessageData(result)
                      .snapDataRequest(request)
                      .build());
            });
  }

  public static BiFunction<SnapDataRequest, BlockHeader, EthTask<? extends AbstractSnapMessageData>>
      getSnapDataFactory(final EthContext ethContext, final MetricsSystem metricsSystem) {
    return (request, blockHeader) -> {
      request.clear();
      switch (request.getRequestType()) {
        case ACCOUNT_RANGE:
        default:
          return RetryingGetAccountRangeFromPeerTask.forAccountRange(
              ethContext, request, blockHeader, metricsSystem);
        case STORAGE_RANGE:
          return RetryingGetStorageRangeFromPeerTask.forStorageRange(
              ethContext, request, blockHeader, metricsSystem);
        case BYTECODES:
          return RetryingGetBytecodeFromPeerTask.forStorageRange(
              ethContext, request, blockHeader, metricsSystem);
      }
    };
  }

  @Value.Immutable
  public interface SendRequestResult {
    SnapDataRequest snapDataRequest();

    AbstractSnapMessageData abstractSnapMessageData();
  }
}
