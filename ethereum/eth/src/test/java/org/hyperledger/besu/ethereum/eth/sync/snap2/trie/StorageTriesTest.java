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
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Format;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Storages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.World;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class StorageTriesTest {

  private final Storages storages = SyncStorageFixture.create(Format.BONSAI);

  @Test
  void buildsTheLargestStoragesFirst() {
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
    final StorageTries tries = new StorageTries(storage, StorageTriesTest::noMoves, 1);

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
  void leavesAStorageThatDoesNotGiveItsVerifiedRootToTheFinalBuild() {
    final World world = SyncStorageFixture.randomWorld(new Random(6), 50);
    world.writeFlat(storages.sync());
    final Bytes32 account = world.storage.firstKey();
    final Bytes32 otherRoot = Bytes32.fromHexString("0x" + "11".repeat(32));
    final StorageTries tries = new StorageTries(storages.sync(), StorageTriesTest::noMoves, 1);

    tries.completed(account, otherRoot, world.storage.get(account).size());
    await().atMost(Duration.ofSeconds(20)).until(() -> tries.left() == 1);
    tries.stop();

    assertThat(tries.built()).isZero();
    assertThat(storages.sync().hasStorageTrie(account, world.storageRoot(account))).isFalse();
  }

  @Test
  void appliesTheSlotsAMoveChangedToABuiltTrie() {
    final World world = SyncStorageFixture.randomWorld(new Random(8), 80);
    final Bytes32 account = largestStorage(world);
    final Bytes32 downloadedRoot = writeWithRoot(world, account);
    final StorageTries tries = new StorageTries(storages.sync(), StorageTriesTest::noMoves, 1);
    tries.completed(account, downloadedRoot, world.storage.get(account).size());
    await().atMost(Duration.ofSeconds(20)).until(() -> tries.built() == 1);

    final Map<Bytes32, UInt256> changes = move(world, account);
    tries.changed(account, Hash.wrap(downloadedRoot), changes);
    tries.finish();

    assertThat(tries.updated()).isOne();
    assertStorageTrie(world, account);
  }

  @Test
  void appliesTheSlotsAMoveChangedDuringABuildOnceItIsDone() {
    final World world = SyncStorageFixture.randomWorld(new Random(9), 80);
    final Bytes32 account = largestStorage(world);
    final Bytes32 downloadedRoot = writeWithRoot(world, account);
    final SyncStateStorage storage = spy(storages.sync());
    final CountDownLatch moved = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              // the move lands before the build reads the slots
              Uninterruptibles.awaitUninterruptibly(moved);
              return invocation.callRealMethod();
            })
        .when(storage)
        .forEachSlotValue(any(), any());
    final StorageTries tries = new StorageTries(storage, StorageTriesTest::noMoves, 1);
    tries.completed(account, downloadedRoot, world.storage.get(account).size());
    await().atMost(Duration.ofSeconds(20)).until(() -> tries.queued() == 0);

    final Map<Bytes32, UInt256> changes = move(world, account);
    tries.changed(account, Accounts.UNKNOWN_STORAGE_ROOT, changes);
    moved.countDown();
    await().atMost(Duration.ofSeconds(20)).until(() -> tries.updated() == 1);
    tries.finish();

    assertThat(tries.built()).isOne();
    assertThat(tries.left()).isZero();
    assertStorageTrie(world, account);
  }

  private static Bytes32 largestStorage(final World world) {
    return world.storage.entrySet().stream()
        .max(Comparator.comparingInt(entry -> entry.getValue().size()))
        .orElseThrow()
        .getKey();
  }

  // the flat state as downloaded, the account having its storage root
  private Bytes32 writeWithRoot(final World world, final Bytes32 account) {
    world.writeFlat(storages.sync());
    final SyncStateStorage.Batch batch = storages.sync().batch();
    batch.putAccount(account, Accounts.encode(world.account(account)));
    batch.commit();
    return world.storageRoot(account);
  }

  // changes, adds and removes slots as a move does, and returns the changes
  private Map<Bytes32, UInt256> move(final World world, final Bytes32 account) {
    final TreeMap<Bytes32, UInt256> slots = world.storage.get(account);
    final Map<Bytes32, UInt256> changes = new HashMap<>();
    changes.put(slots.firstKey(), UInt256.valueOf(7));
    changes.put(slots.lastKey(), UInt256.ZERO);
    changes.put(Bytes32.fromHexString("0x" + "ab".repeat(32)), UInt256.valueOf(9));
    final SyncStateStorage.Batch batch = storages.sync().batch();
    changes.forEach(
        (slot, value) -> {
          if (value.isZero()) {
            slots.remove(slot);
            batch.removeStorage(account, slot);
          } else {
            slots.put(slot, value);
            batch.putStorage(account, slot, value);
          }
        });
    batch.putAccount(
        account,
        Accounts.encode(
            Accounts.withStorageRoot(world.accounts.get(account), Accounts.UNKNOWN_STORAGE_ROOT)));
    batch.commit();
    return changes;
  }

  // the account has the root of its new slots, whose trie is stored whole
  private void assertStorageTrie(final World world, final Bytes32 account) {
    final Bytes32 root = world.storageRoot(account);
    assertThat(
            Accounts.decode(storages.sync().getAccount(account).orElseThrow())
                .getStorageRoot()
                .getBytes())
        .isEqualTo(root);
    assertThat(storages.storageTrie(account, root).entriesFrom(Bytes32.ZERO, Integer.MAX_VALUE))
        .isEqualTo(world.storageLeaves(account));
  }

  // no pivot move ever runs in these tests
  private static boolean noMoves(final Runnable action) {
    action.run();
    return true;
  }
}
