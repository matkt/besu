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
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Format;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Storages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.World;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class StorageTriesTest {

  @Test
  void buildsTheLargestStoragesFirst() {
    final Storages storages = SyncStorageFixture.create(Format.BONSAI);
    final World world = SyncStorageFixture.randomWorld(new Random(5), 400);
    world.writeFlat(storages.sync());
    final SyncStateStorage storage = spy(storages.sync());
    final CountDownLatch queued = new CountDownLatch(1);
    final List<Bytes32> order = new CopyOnWriteArrayList<>();
    doAnswer(
            invocation -> {
              // the first build waits for every storage to be queued
              Uninterruptibles.awaitUninterruptibly(queued);
              order.add(invocation.getArgument(0));
              return invocation.callRealMethod();
            })
        .when(storage)
        .forEachSlotValue(any(), any());
    final StorageTries tries = new StorageTries(storage, 1);

    world.storage.forEach(
        (account, slots) -> tries.completed(account, world.storageRoot(account), slots.size()));
    queued.countDown();
    await().atMost(Duration.ofSeconds(20)).until(() -> tries.built() == world.storage.size());
    tries.stop();

    // after the one taken first, from the largest to the smallest
    assertThat(order.subList(1, order.size()).stream().map(a -> world.storage.get(a).size()))
        .isSortedAccordingTo(Comparator.reverseOrder());
    assertThat(tries.left()).isZero();
    world.storage.forEach(
        (account, slots) ->
            assertThat(storage.hasStorageTrie(account, world.storageRoot(account))).isTrue());
  }

  @Test
  void leavesAStorageThatNoLongerHasItsRootToTheFinalBuild() {
    final Storages storages = SyncStorageFixture.create(Format.BONSAI);
    final World world = SyncStorageFixture.randomWorld(new Random(6), 50);
    world.writeFlat(storages.sync());
    final Bytes32 account = world.storage.firstKey();
    final Bytes32 changedRoot = Bytes32.fromHexString("0x" + "11".repeat(32));
    final StorageTries tries = new StorageTries(storages.sync(), 1);

    tries.completed(account, changedRoot, world.storage.get(account).size());
    await().atMost(Duration.ofSeconds(20)).until(() -> tries.left() == 1);
    tries.stop();

    assertThat(tries.built()).isZero();
    assertThat(storages.sync().hasStorageTrie(account, world.storageRoot(account))).isFalse();
  }

  @Test
  void abandonsALargeStorageThatAMoveChangedWhileItWasBuilt() {
    final Storages storages = SyncStorageFixture.create(Format.BONSAI);
    final Random random = new Random(7);
    final World world = new World();
    final Bytes32 changed = Bytes32.random(random);
    final Bytes32 kept = Bytes32.random(random);
    // large enough to be checked while built
    for (final Bytes32 account : List.of(changed, kept)) {
      final TreeMap<Bytes32, UInt256> slots = new TreeMap<>();
      for (int i = 0; i < 70_000; i++) {
        slots.put(Bytes32.random(random), UInt256.valueOf(i + 1));
      }
      world.storage.put(account, slots);
      world.accounts.put(account, new PmtStateTrieAccountValue(1, Wei.ONE, Hash.ZERO, Hash.EMPTY));
    }
    // the move left the changed account with an unknown root, the other one keeps its own
    world.writeFlat(storages.sync());
    final Bytes32 changedRoot = world.storageRoot(changed);
    final Bytes32 keptRoot = world.storageRoot(kept);
    final SyncStateStorage.Batch batch = storages.sync().batch();
    batch.putAccount(
        kept,
        Accounts.encode(new PmtStateTrieAccountValue(1, Wei.ONE, Hash.wrap(keptRoot), Hash.EMPTY)));
    batch.commit();
    final StorageTries tries = new StorageTries(storages.sync(), 1);

    tries.completed(changed, changedRoot, 70_000);
    tries.completed(kept, keptRoot, 70_000);
    await().atMost(Duration.ofSeconds(30)).until(() -> tries.built() + tries.left() == 2);
    tries.stop();

    assertThat(tries.built()).isOne();
    assertThat(tries.left()).isOne();
    // the changed one stopped at its first check
    assertThat(tries.slots()).isEqualTo(65_536 + 70_000);
    assertThat(storages.sync().hasStorageTrie(kept, keptRoot)).isTrue();
    assertThat(storages.sync().hasStorageTrie(changed, changedRoot)).isFalse();
  }
}
