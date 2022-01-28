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

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.hyperledger.besu.ethereum.bonsai.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.fastsync.FastSyncActions;
import org.hyperledger.besu.ethereum.eth.sync.fastsync.FastSyncState;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldDownloadState;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;
import org.hyperledger.besu.services.tasks.InMemoryTaskQueue;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;
import org.hyperledger.besu.services.tasks.Task;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class SnapWorldDownloadState extends WorldDownloadState<SnapDataRequest> {

  private static final Logger LOG = getLogger(SnapWorldDownloadState.class);


  private final FastSyncActions fastSyncActions;
  private final SnapSyncState snapSyncState;

  private final InMemoryTaskQueue<SnapDataRequest> codeBlocksCollection = new InMemoryTaskQueue<>();
  private final InMemoryTasksPriorityQueues<SnapDataRequest> trieNodes =
          new InMemoryTasksPriorityQueues<>();

  public SnapWorldDownloadState(
          final FastSyncActions fastSyncActions,
      final SnapSyncState snapSyncState,
      final InMemoryTasksPriorityQueues<SnapDataRequest> pendingRequests,
      final int maxRequestsWithoutProgress,
      final long minMillisBeforeStalling,
      final Clock clock) {
    super(pendingRequests, maxRequestsWithoutProgress, minMillisBeforeStalling, clock);
    this.fastSyncActions = fastSyncActions;
    this.snapSyncState = snapSyncState;
  }

  @Override
  protected synchronized void markAsStalled(final int maxNodeRequestRetries) {
    if (!snapSyncState.isResettingPivotBlock()) {
      super.markAsStalled(maxNodeRequestRetries);
    }
  }

  @Override
  public synchronized boolean checkCompletion(
      final WorldStateStorage worldStateStorage, final BlockHeader header) {
    if (!internalFuture.isDone()
        && pendingRequests.allTasksCompleted()
        && codeBlocksCollection.allTasksCompleted()
        && trieNodes.allTasksCompleted()) {
      if (!snapSyncState.isHealInProgress()) {
        LOG.info("Starting heal process on state root {}", header.getStateRoot());
        if (worldStateStorage instanceof BonsaiWorldStateKeyValueStorage) {
          ((BonsaiWorldStateKeyValueStorage) worldStateStorage).clearReadAccessDatabase();
        }
        snapSyncState.setSnapHealInProgress(true);
        enqueueRequest(
            SnapDataRequest.createAccountDataRequest(header.getStateRoot(), Optional.empty()));
      } else {
        final WorldStateStorage.Updater updater = worldStateStorage.updater();
        updater.saveWorldState(header.getHash(), header.getStateRoot(), rootNodeData);
        updater.commit();
        LOG.info("Finished downloading world state from peers");
        internalFuture.complete(null);
        return true;
      }
    }
    return false;
  }

  public void checkNewPivotBlock(final SnapSyncState currentPivotBlock) {
    final long currentPivotBlockNumber = currentPivotBlock.getPivotBlockNumber().orElseThrow();
    final long distance =
        fastSyncActions.getSyncState().bestChainHeight() - currentPivotBlockNumber;
    if (distance > 126) {
      if (snapSyncState.lockResettingPivotBlock()) {
        fastSyncActions
            .selectPivotBlock(FastSyncState.EMPTY_SYNC_STATE)
            .thenCompose(fastSyncActions::downloadPivotBlockHeader)
            .thenAccept(
                fss ->
                    fss.getPivotBlockHeader()
                        .ifPresent(
                            newPivotBlockHeader -> {
                              if (currentPivotBlockNumber != newPivotBlockHeader.getNumber()) {
                                LOG.info(
                                    "Select new pivot block {} {}",
                                    newPivotBlockHeader.getNumber(),
                                    newPivotBlockHeader.getStateRoot());
                                snapSyncState.setCurrentHeader(newPivotBlockHeader);
                                if(snapSyncState.isHealInProgress()){
                                  clearQueues();
                                  enqueueRequest(
                                          SnapDataRequest.createAccountDataRequest(newPivotBlockHeader.getStateRoot(), Optional.empty()));
                                }
                              }
                              requestComplete(true);
                              snapSyncState.unlockResettingPivotBlock();
                            }))
            .orTimeout(10, TimeUnit.MINUTES)
            .whenComplete(
                (unused, throwable) -> {
                  snapSyncState.unlockResettingPivotBlock();
                });
      }
    }
  }

  public synchronized Task<SnapDataRequest> dequeueCodeRequestBlocking() {
    while (!internalFuture.isDone()) {
      Task<SnapDataRequest> task = codeBlocksCollection.remove();
      if (task != null) {
        return task;
      }
      try {
        wait();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    }
    return null;
  }

  public synchronized Task<SnapDataRequest> dequeueTrieRequestBlocking() {
    while (!internalFuture.isDone()) {
      Task<SnapDataRequest> task = trieNodes.remove();
      if (task != null) {
        return task;
      }
      try {
        wait();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    }
    return null;
  }

  @Override
  public synchronized void enqueueRequest(final SnapDataRequest request) {
    if (!internalFuture.isDone()) {
      if (request instanceof GetBytecodeRequest) {
        codeBlocksCollection.add(request);
      } else if (request instanceof TrieNodeDataHealRequest) {
        if (snapSyncState.isValidTask(request)) { //TODO: Before merge move me somewhere else
          trieNodes.add(request);
        }
      } else {
        pendingRequests.add(request);
      }
      notifyAll();
    }
  }

  @Override
  public synchronized void enqueueRequests(final Stream<SnapDataRequest> requests) {
    if (!internalFuture.isDone()) {
      requests.forEach(this::enqueueRequest);
      notifyAll();
    }
  }

  private void clearQueues(){
    pendingRequests.clear();
    trieNodes.clear();
    codeBlocksCollection.clear();
    outstandingRequests.clear();
  }
}
