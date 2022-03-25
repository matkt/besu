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
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.AccountTrieNodeDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.StorageTrieNodeDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.TrieNodeDataRequest;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.services.pipeline.Pipe;
import org.hyperledger.besu.services.tasks.Task;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;

public class LoadLocalDataStep {

  private final WorldStateStorage worldStateStorage;
  private final SnapWorldDownloadState downloadState;
  private final SnapSyncState snapSyncState;
  private final Counter existingNodeCounter;
  private final AtomicInteger nbAccount, nbStorageCp;
  private Map<Hash,AtomicInteger> nbStorage = new ConcurrentHashMap<>();
  public LoadLocalDataStep(
      final WorldStateStorage worldStateStorage,
      final SnapWorldDownloadState downloadState,
      final MetricsSystem metricsSystem,
      final SnapSyncState snapSyncState) {
    this.worldStateStorage = worldStateStorage;
    this.downloadState = downloadState;
    existingNodeCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "snap_world_state_existing_nodes_total",
            "Total number of node data requests completed using existing data");
    this.snapSyncState = snapSyncState;
    nbAccount = new AtomicInteger();
    nbStorageCp = new AtomicInteger();
  }

  public Stream<Task<SnapDataRequest>> loadLocalDataTrieNode(
      final Task<SnapDataRequest> task, final Pipe<Task<SnapDataRequest>> completedTasks) {
    final TrieNodeDataRequest request = (TrieNodeDataRequest) task.getData();
    // check if node is already stored in the worldstate
    if(snapSyncState.hasPivotBlockHeader()){
      Optional<Bytes> existingData = request.getExistingData(worldStateStorage);
      if (existingData.isPresent()) {
        existingNodeCounter.inc();
        request.setData(existingData.get());
        request.setRequiresPersisting(false);
        request.setRootHash(snapSyncState.getPivotBlockHeader().get().getStateRoot());
        final WorldStateStorage.Updater updater = worldStateStorage.updater();
        request.persist(worldStateStorage, updater, downloadState, snapSyncState);
        updater.commit();
        downloadState.enqueueRequests(request.getRootStorageRequests(worldStateStorage));
        completedTasks.put(task);
        return Stream.empty();
      } else {
        if(task.getData() instanceof AccountTrieNodeDataRequest){
          if(((AccountTrieNodeDataRequest) task.getData()).isRoot()){
            nbAccount.set(0);
            nbStorageCp.set(0);
            nbStorage = new ConcurrentHashMap<>();
          }
          nbAccount.incrementAndGet();
        } else if(task.getData() instanceof StorageTrieNodeDataRequest){
          nbStorageCp.incrementAndGet();
          final AtomicInteger orDefault = nbStorage.getOrDefault(((StorageTrieNodeDataRequest) task.getData()).getAccountHash(), new AtomicInteger());
          orDefault.incrementAndGet();
          nbStorage.put(((StorageTrieNodeDataRequest) task.getData()).getAccountHash(),orDefault);
        }
      }
    }

    if((nbAccount.get()%10000==0) || (nbStorageCp.get()%10000==0)){
      System.out.println("nb missing account "+nbAccount.get()+" storage "+nbStorageCp.get());
      for (Map.Entry<Hash,AtomicInteger> entry :nbStorage.entrySet()) {
        System.out.println("storage account "+entry.getKey()+" "+entry.getValue());
      }
    }

    return Stream.of(task);
  }
}
