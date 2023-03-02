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
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.LayeredKeyValueStorage;

public class BonsaiWorldStateLayerStorage extends BonsaiWorldStateKeyValueStorage
    implements BonsaiStorageSubscriber {

  private final long subscribeParentId;
  private final BonsaiWorldStateKeyValueStorage parent;

  public BonsaiWorldStateLayerStorage(final BonsaiWorldStateKeyValueStorage parent) {
    this(
        new LayeredKeyValueStorage(parent.accountStorage),
        new LayeredKeyValueStorage(parent.codeStorage),
        new LayeredKeyValueStorage(parent.storageStorage),
        new LayeredKeyValueStorage(parent.trieBranchStorage),
        parent.trieLogStorage,
        parent);
  }

  public BonsaiWorldStateLayerStorage(
      final KeyValueStorage accountStorage,
      final KeyValueStorage codeStorage,
      final KeyValueStorage storageStorage,
      final KeyValueStorage trieBranchStorage,
      final KeyValueStorage trieLogStorage,
      final BonsaiWorldStateKeyValueStorage parent) {
    super(accountStorage, codeStorage, storageStorage, trieBranchStorage, trieLogStorage);
    this.parent = parent;
    this.subscribeParentId = parent.subscribe(this);
  }

  @Override
  public BonsaiWorldStateLayerStorage clone() {
    return new BonsaiWorldStateLayerStorage(
        ((LayeredKeyValueStorage) accountStorage).clone(),
        ((LayeredKeyValueStorage) codeStorage).clone(),
        ((LayeredKeyValueStorage) storageStorage).clone(),
        ((LayeredKeyValueStorage) trieBranchStorage).clone(),
        trieLogStorage,
        parent);
  }

  @Override
  public void close() throws Exception {
    parent.unSubscribe(subscribeParentId);
  }
}