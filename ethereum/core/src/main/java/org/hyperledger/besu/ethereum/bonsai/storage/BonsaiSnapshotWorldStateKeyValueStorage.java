/*
 * Copyright Hyperledger Besu Contributors.
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
 *
 */
package org.hyperledger.besu.ethereum.bonsai.storage;

import org.hyperledger.besu.ethereum.bonsai.storage.BonsaiWorldStateKeyValueStorage.BonsaiStorageSubscriber;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappableKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;

public class BonsaiSnapshotWorldStateKeyValueStorage extends BonsaiWorldStateKeyValueStorage
    implements BonsaiStorageSubscriber {

  protected final BonsaiWorldStateKeyValueStorage parentWorldStateStorage;

  private final long subscribeParentId;

  public BonsaiSnapshotWorldStateKeyValueStorage(
      final BonsaiWorldStateKeyValueStorage parentWorldStateStorage,
      final SnappedKeyValueStorage accountStorage,
      final SnappedKeyValueStorage codeStorage,
      final SnappedKeyValueStorage storageStorage,
      final SnappedKeyValueStorage trieBranchStorage,
      final KeyValueStorage trieLogStorage) {
    super(accountStorage, codeStorage, storageStorage, trieBranchStorage, trieLogStorage);
    this.parentWorldStateStorage = parentWorldStateStorage;
    this.subscribeParentId = parentWorldStateStorage.subscribe(this);
  }

  public BonsaiSnapshotWorldStateKeyValueStorage(
      final BonsaiWorldStateKeyValueStorage worldStateStorage) {
    this(
        worldStateStorage,
        ((SnappableKeyValueStorage) worldStateStorage.accountStorage).takeSnapshot(),
        ((SnappableKeyValueStorage) worldStateStorage.codeStorage).takeSnapshot(),
        ((SnappableKeyValueStorage) worldStateStorage.storageStorage).takeSnapshot(),
        ((SnappableKeyValueStorage) worldStateStorage.trieBranchStorage).takeSnapshot(),
        worldStateStorage.trieLogStorage);
  }

  @Override
  public BonsaiUpdater updater() {
    return new Updater(
        ((SnappedKeyValueStorage) accountStorage).getSnapshotTransaction(),
        ((SnappedKeyValueStorage) codeStorage).getSnapshotTransaction(),
        ((SnappedKeyValueStorage) storageStorage).getSnapshotTransaction(),
        ((SnappedKeyValueStorage) trieBranchStorage).getSnapshotTransaction(),
        trieLogStorage.startTransaction());
  }

  @Override
  public void clear() {
    // snapshot storage does not implement clear
    throw new StorageException("Snapshot storage does not implement clear");
  }

  @Override
  public void clearFlatDatabase() {
    // snapshot storage does not implement clear
    throw new StorageException("Snapshot storage does not implement clear");
  }

  @Override
  public void clearTrieLog() {
    // snapshot storage does not implement clear
    throw new StorageException("Snapshot storage does not implement clear");
  }

  @Override
  protected synchronized void doClose() throws Exception {
    if (!isClosed.get()) {
      // alert any subscribers we are closing:
      subscribers.forEach(BonsaiStorageSubscriber::onCloseStorage);

      // close all of the SnappedKeyValueStorages:
      accountStorage.close();
      codeStorage.close();
      storageStorage.close();
      trieBranchStorage.close();

      // unsubscribe the parent worldstate
      parentWorldStateStorage.unSubscribe(subscribeParentId);

      // set storage closed
      isClosed.set(true);
    }
  }

  @Override
  public void onCloseStorage() {
    try {
      // when the parent storage close, close regardless of subscribers
      doClose();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onClearStorage() {
    try {
      // when the parent storage clears, close regardless of subscribers
      doClose();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onClearFlatDatabaseStorage() {
    // when the parent storage clears, close regardless of subscribers
    try {
      doClose();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onClearTrieLog() {
    // when the parent storage clears, close regardless of subscribers
    try {
      doClose();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public BonsaiWorldStateKeyValueStorage getParentWorldStateStorage() {
    return parentWorldStateStorage;
  }
}
