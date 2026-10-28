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
package org.hyperledger.besu.ethereum.eth.sync.snap2.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.slots;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Format;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStorageFixture.Storages;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** What every format of the sync storage does. */
class SyncStateStorageTest {

  private static final Bytes32 BEFORE = key(0x05);
  private static final Bytes32 FIRST = key(0x10);
  private static final Bytes32 ORPHAN = key(0x15);
  private static final Bytes32 SECOND = key(0x20);
  private static final Bytes32 THIRD = key(0x30);
  private static final Bytes32 LAST_ORPHAN = key(0x35);
  private static final Bytes32 AFTER = key(0x50);
  private static final Bytes ACCOUNT = Bytes.of(1);
  private static final Bytes CODE = Bytes.fromHexString("0x6001600155");
  private static final Bytes32 CODE_HASH = hash(CODE);
  private static final Bytes NODE = Bytes.fromHexString("0xc28080");
  private static final Bytes32 NODE_HASH = hash(NODE);
  private static final Bytes ABORTED_NODE = Bytes.fromHexString("0xc380c180");

  // slots of a missing account come after the previous account, an account may skip its slots
  @ParameterizedTest
  @EnumSource(Format.class)
  void scanVisitsTheAccountsOfTheRangeEachFollowedByItsSlots(final Format format) {
    final SyncStateStorage storage = SyncStorageFixture.create(format).sync();
    final SyncStateStorage.Batch batch = storage.batch();
    for (final Bytes32 account : List.of(BEFORE, FIRST, SECOND, THIRD, AFTER)) {
      batch.putAccount(account, ACCOUNT);
    }
    for (final Bytes32 account :
        List.of(BEFORE, FIRST, ORPHAN, SECOND, THIRD, LAST_ORPHAN, AFTER)) {
      batch.putStorage(account, key(0x02), value(2));
      batch.putStorage(account, key(0x01), value(1));
    }
    batch.commit();

    final List<String> visits = new ArrayList<>();
    storage.scan(
        FIRST,
        key(0x3f),
        new SyncStateStorage.ScanVisitor() {
          @Override
          public boolean onAccount(final Bytes32 accountHash, final Bytes account) {
            visits.add("account " + name(accountHash));
            return !accountHash.equals(SECOND);
          }

          @Override
          public void onSlot(final Bytes32 accountHash, final Bytes32 slotHash, final Bytes value) {
            visits.add("slot " + name(accountHash) + "/" + name(slotHash) + "=" + name(value));
          }
        });

    assertThat(visits)
        .containsExactly(
            "account 10",
            "slot 10/01=01",
            "slot 10/02=02",
            "slot 15/01=01",
            "slot 15/02=02",
            "account 20",
            "account 30",
            "slot 30/01=01",
            "slot 30/02=02",
            "slot 35/01=01",
            "slot 35/02=02");
  }

  // more slots than one delete batch holds
  @ParameterizedTest
  @EnumSource(Format.class)
  void deleteStorageRemovesEverySlotOfTheAccountOnly(final Format format) {
    final SyncStateStorage storage = SyncStorageFixture.create(format).sync();
    final SyncStateStorage.Batch batch = storage.batch();
    for (int i = 0; i < 10_001; i++) {
      batch.putStorage(FIRST, Bytes32.leftPad(Bytes.ofUnsignedInt(i)), value(1));
    }
    batch.putStorage(SECOND, key(0x01), value(1));
    batch.commit();

    storage.deleteStorage(FIRST);

    assertThat(slots(storage, FIRST)).isEmpty();
    assertThat(slots(storage, SECOND)).containsOnlyKeys(key(0x01));
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void deleteStorageAbortsTheBatchOfAFailedWriteButNotOneWhoseCommitFailed(final Format format) {
    final SyncStateStorage storage = SyncStorageFixture.create(format).sync();
    final SyncStateStorage.Batch slots = storage.batch();
    slots.putStorage(FIRST, key(0x01), value(1));
    slots.commit();
    final SyncStateStorage failing = spy(storage);
    final SyncStateStorage.Batch batch = mock(SyncStateStorage.Batch.class);
    doReturn(batch).when(failing).batch();

    // the commit of the transaction has started, it cannot be rolled back
    doThrow(new IllegalStateException("disk full")).when(batch).commit();
    assertThatThrownBy(() -> failing.deleteStorage(FIRST)).hasMessage("disk full");
    verify(batch, never()).abort();

    doThrow(new IllegalStateException("write failed")).when(batch).removeStorage(any(), any());
    assertThatThrownBy(() -> failing.deleteStorage(FIRST)).hasMessage("write failed");
    verify(batch).abort();
  }

  // the next batch fails to start once the first one is committed
  @ParameterizedTest
  @EnumSource(Format.class)
  void deleteStorageAbortsNoBatchItCommitted(final Format format) {
    final SyncStateStorage storage = SyncStorageFixture.create(format).sync();
    final SyncStateStorage.Batch slots = storage.batch();
    for (int i = 0; i < 10_001; i++) {
      slots.putStorage(FIRST, Bytes32.leftPad(Bytes.ofUnsignedInt(i)), value(1));
    }
    slots.commit();
    final SyncStateStorage failing = spy(storage);
    final SyncStateStorage.Batch batch = mock(SyncStateStorage.Batch.class);
    doReturn(batch).doThrow(new IllegalStateException("storage closed")).when(failing).batch();

    assertThatThrownBy(() -> failing.deleteStorage(FIRST)).hasMessage("storage closed");
    verify(batch).commit();
    verify(batch, never()).abort();
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void deleteStorageWritesNothingForAnAccountWithoutSlots(final Format format) {
    final SyncStateStorage spied = spy(SyncStorageFixture.create(format).sync());

    spied.deleteStorage(FIRST);

    verify(spied, never()).batch();
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void removesAccountsAndSlots(final Format format) {
    final SyncStateStorage storage = SyncStorageFixture.create(format).sync();
    final SyncStateStorage.Batch batch = storage.batch();
    batch.putAccount(FIRST, ACCOUNT);
    batch.putStorage(FIRST, key(0x01), value(1));
    batch.commit();

    final SyncStateStorage.Batch removal = storage.batch();
    removal.removeAccount(FIRST);
    removal.removeStorage(FIRST, key(0x01));
    removal.commit();

    assertThat(storage.getAccount(FIRST)).isEmpty();
    assertThat(slots(storage, FIRST)).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void abortDropsTheWritesOfABatch(final Format format) {
    final SyncStateStorage storage = SyncStorageFixture.create(format).sync();
    final SyncStateStorage.Batch batch = storage.batch();
    batch.putAccount(FIRST, ACCOUNT);
    batch.putStorage(FIRST, key(0x01), value(1));
    batch.putCode(CODE_HASH, CODE);
    batch.abort();

    assertThat(storage.getAccount(FIRST)).isEmpty();
    assertThat(slots(storage, FIRST)).isEmpty();
    assertThat(storage.hasCode(CODE_HASH)).isFalse();
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void storesTheCodeByItsHash(final Format format) {
    final SyncStateStorage storage = SyncStorageFixture.create(format).sync();
    final SyncStateStorage.Batch batch = storage.batch();
    batch.putCode(CODE_HASH, CODE);
    batch.commit();

    assertThat(storage.hasCode(CODE_HASH)).isTrue();
    assertThat(storage.hasCode(FIRST)).isFalse();
  }

  // a writer goes on after an abort, without the nodes it dropped
  @ParameterizedTest
  @EnumSource(Format.class)
  void writesTheNodesOfATrieOnCommitOnly(final Format format) {
    final Storages storages = SyncStorageFixture.create(format);
    final SyncStateStorage storage = storages.sync();
    final SyncStateStorage.TrieWriter writer = storage.trieWriter();
    writer.storageNode(FIRST, Bytes.of(2), hash(ABORTED_NODE), ABORTED_NODE);
    writer.abort();

    writer.storageNode(FIRST, Bytes.EMPTY, NODE_HASH, NODE);
    writer.accountNode(Bytes.of(1), NODE_HASH, NODE);
    assertThat(storage.hasStorageTrie(FIRST, NODE_HASH)).isFalse();
    writer.commit();

    assertThat(storage.storageTrieNode(FIRST, Bytes.of(2), hash(ABORTED_NODE))).isEmpty();
    assertThat(storage.hasStorageTrie(FIRST, NODE_HASH)).isTrue();
    assertThat(storage.storageTrieNode(FIRST, Bytes.EMPTY, NODE_HASH)).contains(NODE);
    assertThat(storages.accountNodes().getNode(Bytes.of(1), NODE_HASH)).contains(NODE);
  }

  // 9 MB of nodes, past the 8 MB a batch holds
  @ParameterizedTest
  @EnumSource(Format.class)
  void commitsTheNodesOnceTheyFillABatch(final Format format) {
    final SyncStateStorage storage = SyncStorageFixture.create(format).sync();
    final SyncStateStorage.TrieWriter writer = storage.trieWriter();
    final List<Bytes> nodes = new ArrayList<>();
    for (int i = 0; i < 9 * 1024; i++) {
      nodes.add(Bytes.concatenate(Bytes.ofUnsignedInt(i), Bytes.wrap(new byte[1020])));
      writer.storageNode(FIRST, location(i), hash(nodes.get(i)), nodes.get(i));
    }

    final int last = nodes.size() - 1;
    assertThat(storage.storageTrieNode(FIRST, location(0), hash(nodes.getFirst())))
        .contains(nodes.getFirst());
    assertThat(storage.storageTrieNode(FIRST, location(last), hash(nodes.getLast()))).isEmpty();
    writer.abort();
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void prepareForSyncWipesTheState(final Format format) {
    final SyncStateStorage storage = SyncStorageFixture.create(format).sync();
    final SyncStateStorage.Batch batch = storage.batch();
    batch.putAccount(FIRST, ACCOUNT);
    batch.putStorage(FIRST, key(0x01), value(1));
    batch.putCode(CODE_HASH, CODE);
    batch.commit();
    final SyncStateStorage.TrieWriter writer = storage.trieWriter();
    writer.storageNode(FIRST, Bytes.EMPTY, NODE_HASH, NODE);
    writer.commit();

    storage.prepareForSync();

    assertThat(storage.getAccount(FIRST)).isEmpty();
    assertThat(slots(storage, FIRST)).isEmpty();
    assertThat(storage.hasCode(CODE_HASH)).isFalse();
    assertThat(storage.hasStorageTrie(FIRST, NODE_HASH)).isFalse();
  }

  private static Bytes32 key(final int last) {
    return Bytes32.leftPad(Bytes.of(last));
  }

  private static Bytes location(final int index) {
    return Bytes.ofUnsignedInt(index);
  }

  private static Bytes32 hash(final Bytes node) {
    return Bytes32.wrap(Hash.hash(node).getBytes());
  }

  private static Bytes32 value(final int value) {
    return Bytes32.leftPad(Bytes.of(value));
  }

  // the last byte names a key or a value in the expected visits
  private static String name(final Bytes bytes) {
    return String.format("%02x", bytes.get(bytes.size() - 1));
  }
}
