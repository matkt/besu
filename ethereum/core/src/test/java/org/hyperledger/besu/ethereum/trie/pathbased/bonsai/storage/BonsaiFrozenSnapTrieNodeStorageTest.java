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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.nio.file.Path;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BonsaiFrozenSnapTrieNodeStorageTest {

  @TempDir Path tempDir;

  @Test
  void snapSyncWritesPlainTablePostSyncWritesRocksDbAndReadsPlainTableFirst() {
    final Path frozenDir = tempDir.resolve("frozen");
    final InMemoryKeyValueStorageProvider provider = new InMemoryKeyValueStorageProvider();
    final BonsaiWorldStateKeyValueStorage storage =
        new BonsaiWorldStateKeyValueStorage(
            provider,
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG,
            frozenDir);

    final Bytes location = Bytes.fromHexString("0x0102");
    final Bytes node = Bytes.fromHexString("0x820120");
    final Bytes32 nodeHash = Bytes32.wrap(Hash.hash(node).getBytes());

    storage.enableSnapSyncFrozenTrieNodeCapture();
    final BonsaiWorldStateKeyValueStorage.Updater snapUpdater = storage.updater();
    snapUpdater.putAccountStateTrieNode(location, nodeHash, node);
    snapUpdater.commit();
    assertThat(storage.getAccountStateTrieNode(location, nodeHash)).contains(node);
    assertThat(storage.getFrozenSnapTrieNodeStorage().flatMap(s -> s.get(nodeHash)))
        .contains(node);

    storage.freezeSnapSyncFrozenTrieNodes();
    final Bytes newLocation = Bytes.fromHexString("0x0304");
    final Bytes newNode = Bytes.fromHexString("0x820220");
    final Bytes32 newHash = Bytes32.wrap(Hash.hash(newNode).getBytes());
    final BonsaiWorldStateKeyValueStorage.Updater liveUpdater = storage.updater();
    liveUpdater.putAccountStateTrieNode(newLocation, newHash, newNode);
    liveUpdater.commit();

    assertThat(storage.getAccountStateTrieNode(newLocation, newHash)).contains(newNode);
    assertThat(storage.getAccountStateTrieNode(location, nodeHash)).contains(node);
    assertThat(storage.getFrozenSnapTrieNodeStorage()).isPresent();
    assertThat(storage.getFrozenSnapTrieNodeStorage().get().isFrozen()).isTrue();
  }
}
