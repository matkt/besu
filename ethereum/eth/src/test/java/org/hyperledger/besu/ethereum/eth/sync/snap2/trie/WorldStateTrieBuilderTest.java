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
package org.hyperledger.besu.ethereum.eth.sync.snap2.trie;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Format;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Storages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.World;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;

import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class WorldStateTrieBuilderTest {

  @ParameterizedTest
  @EnumSource(Format.class)
  void buildsTheStateTrieFromTheFlatState(final Format format) {
    final Storages storages = SyncStorageFixture.create(format);
    final World world = SyncStorageFixture.randomWorld(new Random(1), 2000);
    world.writeFlat(storages.sync());

    final WorldStateTrieBuilder.Result result =
        new WorldStateTrieBuilder(storages.sync(), 4).build();

    assertThat(result.root()).isEqualTo(world.root());
    assertThat(result.accounts()).isEqualTo(world.accounts.size());
    assertThat(result.slots())
        .isEqualTo(world.storage.values().stream().mapToLong(slots -> slots.size()).sum());

    // the written nodes can be read back by the storage format
    final MerkleTrie<Bytes32, Bytes> accountTrie = storages.accountTrie(result.root());
    world.accountLeaves().forEach((hash, leaf) -> assertThat(accountTrie.get(hash)).contains(leaf));
    world.storage.keySet().stream()
        .limit(100)
        .forEach(
            account -> {
              final MerkleTrie<Bytes32, Bytes> storageTrie =
                  storages.storageTrie(account, world.storageRoot(account));
              world
                  .storageLeaves(account)
                  .forEach((slot, leaf) -> assertThat(storageTrie.get(slot)).contains(leaf));
            });

    // the flat accounts now carry the right storage roots
    world.accounts.keySet().stream()
        .limit(200)
        .forEach(
            hash -> {
              final PmtStateTrieAccountValue flat =
                  SyncStorageFixture.decode(storages.sync().getAccount(hash)).orElseThrow();
              assertThat(flat.getStorageRoot().getBytes()).isEqualTo(world.storageRoot(hash));
            });
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void keepsTheStorageTriesWrittenAndRebuildsTheChangedOnes(final Format format) {
    final Storages storages = SyncStorageFixture.create(format);
    final World world = SyncStorageFixture.randomWorld(new Random(7), 500);
    world.writeFlat(storages.sync());
    new WorldStateTrieBuilder(storages.sync(), 2).build();
    final long withStorage =
        world.storage.values().stream().filter(slots -> !slots.isEmpty()).count();

    // a slot changes after its trie was written, as in a pivot move
    final Bytes32 changed =
        world.storage.entrySet().stream()
            .filter(entry -> !entry.getValue().isEmpty())
            .findFirst()
            .orElseThrow()
            .getKey();
    final Bytes32 slot = world.storage.get(changed).firstKey();
    world.storage.get(changed).put(slot, UInt256.valueOf(424242));
    final SyncStateStorage.Batch batch = storages.sync().batch();
    batch.putStorage(changed, slot, UInt256.valueOf(424242));
    batch.putAccount(
        changed,
        Accounts.encode(
            Accounts.withStorageRoot(
                SyncStorageFixture.decode(storages.sync().getAccount(changed)).orElseThrow(),
                Accounts.UNKNOWN_STORAGE_ROOT)));
    batch.commit();

    final WorldStateTrieBuilder.Result result =
        new WorldStateTrieBuilder(storages.sync(), 2).build();

    assertThat(result.root()).isEqualTo(world.root());
    assertThat(result.keptStorageTries()).isEqualTo(withStorage - 1);
    assertThat(result.slots()).isEqualTo(world.storage.get(changed).size());
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void buildsSmallStates(final Format format) {
    for (int size : new int[] {0, 1, 2, 5}) {
      final Storages storages = SyncStorageFixture.create(format);
      final World world = SyncStorageFixture.randomWorld(new Random(size), size);
      world.writeFlat(storages.sync());

      final WorldStateTrieBuilder.Result result =
          new WorldStateTrieBuilder(storages.sync(), 2).build();

      assertThat(result.root()).isEqualTo(world.root());
    }
  }
}
