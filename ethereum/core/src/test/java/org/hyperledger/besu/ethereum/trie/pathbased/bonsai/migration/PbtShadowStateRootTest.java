/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class PbtShadowStateRootTest {

  private final BonsaiWorldStateKeyValueStorage storage =
      new BonsaiWorldStateKeyValueStorage(
          new InMemoryKeyValueStorageProvider(),
          new NoOpMetricsSystem(),
          DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);

  @Test
  void everyBlockTheBinaryColumnReachedKeepsItsShadowRoot() {
    final Hash block1 = Hash.hash(Bytes.of(1));
    final Hash block2 = Hash.hash(Bytes.of(2));
    final Bytes32 root1 = Bytes32.leftPad(Bytes.of(0x11));
    final Bytes32 root2 = Bytes32.leftPad(Bytes.of(0x22));

    putCursor(root1, block1);
    putCursor(root2, block2);

    // the cursor moved to block 2, the record of block 1 stays
    assertThat(storage.getWorldStateBlockHash(TrieBranchType.BINARY)).contains(block2);
    assertThat(storage.getShadowStateRoot(block1)).contains(Hash.wrap(root1));
    assertThat(storage.getShadowStateRoot(block2)).contains(Hash.wrap(root2));
  }

  @Test
  void aBlockTheMigrationNeverReachedHasNoShadowRoot() {
    putCursor(Bytes32.leftPad(Bytes.of(0x11)), Hash.hash(Bytes.of(1)));

    assertThat(storage.getShadowStateRoot(Hash.hash(Bytes.of(3)))).isEmpty();
  }

  private void putCursor(final Bytes32 root, final Hash blockHash) {
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    PbtMigrator.putCursor(updater, root, blockHash);
    updater.commit();
  }
}
