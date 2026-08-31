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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.cache.VersionedFlatDbCacheManager;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.Closeable;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Cross-block (versioned) cache behaviour on {@link BonsaiWorldStateKeyValueStorage}: monotonic
 * versions, commit/rollback interaction with {@link VersionedFlatDbCacheManager}, and snapshot
 * views pinned to a cache epoch so they do not observe newer cache-only state after the head moves.
 */
public class BonsaiWorldStateKeyValueStorageCacheTest {

  private BonsaiWorldStateKeyValueStorage head;

  @AfterEach
  void tearDownCacheExecutor() throws Exception {
    disposeHead();
  }

  private void disposeHead() throws Exception {
    if (head != null) {
      if (head.getCacheManager() instanceof final Closeable closeable) {
        closeable.close();
      }
      head.close();
      head = null;
    }
  }

  private static ImmutableDataStorageConfiguration.Builder dataConfigBuilder(
      final boolean crossBlockEnabled) {
    return ImmutableDataStorageConfiguration.builder()
        .dataStorageFormat(DataStorageFormat.BONSAI)
        .pathBasedExtraStorageConfiguration(
            ImmutablePathBasedExtraStorageConfiguration.builder()
                .unstable(
                    ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                        .bonsaiCrossBlockCacheEnabled(crossBlockEnabled)
                        .build())
                .build());
  }

  private void newHead(final boolean crossBlockEnabled) throws Exception {
    disposeHead();
    head =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            dataConfigBuilder(crossBlockEnabled).build());
  }

  @Test
  void crossBlockDisabled_usesEmptyCacheManager() throws Exception {
    newHead(false);
    assertThat(head.getCacheManager()).isSameAs(FlatDbCacheManager.NO_OP_CACHE);

    final Hash account = Hash.hash(Bytes.of(1));
    commitAccount(account, Bytes.of(1, 2, 3));

    assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes())).isFalse();
    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isZero();
  }

  @Test
  void crossBlockEnabled_usesVersionedFlatDbCacheManager() throws Exception {
    newHead(true);
    assertThat(head.getCacheManager()).isInstanceOf(VersionedFlatDbCacheManager.class);
    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isZero();
    assertThat(head.getCacheSize(ACCOUNT_STORAGE_STORAGE)).isZero();
    assertThat(head.getCacheSize(TRIE_BRANCH_STORAGE)).isZero();
  }

  @Test
  void eachSuccessfulCommitAdvancesHeadAndGlobalVersion() throws Exception {
    newHead(true);
    assertThat(head.getCurrentVersion()).isZero();
    assertThat(head.getCacheManager().getCurrentVersion()).isZero();
    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isZero();

    final Hash acc1 = Hash.hash(Bytes.of(1));
    commitAccount(acc1, Bytes.of(1));
    assertThat(head.getCurrentVersion()).isEqualTo(1);
    assertThat(head.getCacheManager().getCurrentVersion()).isEqualTo(1);
    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(1);
    assertThat(head.isCached(ACCOUNT_INFO_STATE, acc1.getBytes())).isTrue();
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, acc1.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(1);
              assertThat(cv.getValue()).isEqualTo(Bytes.of(1));
              assertThat(cv.isRemoval()).isFalse();
            });

    final Hash acc2 = Hash.hash(Bytes.of(2));
    commitAccount(acc2, Bytes.of(2));
    assertThat(head.getCurrentVersion()).isEqualTo(2);
    assertThat(head.getCacheManager().getCurrentVersion()).isEqualTo(2);
    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(2);
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, acc1.getBytes()))
        .hasValueSatisfying(cv -> assertThat(cv.getVersion()).isEqualTo(1));
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, acc2.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(2);
              assertThat(cv.getValue()).isEqualTo(Bytes.of(2));
            });
  }

  @Test
  void rollbackDoesNotAdvanceVersionOrPopulateCache() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    final long v0 = head.getCurrentVersion();

    final var updater = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    updater.putAccountInfoState(account, Bytes.of(9, 9, 9));
    updater.rollback();

    assertThat(head.getCurrentVersion()).isEqualTo(v0);
    assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes())).isFalse();
    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isZero();
  }

  @Test
  void commitWritesVersionedAccountAndStorageSlotEntries() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    final StorageSlotKey slot = new StorageSlotKey(UInt256.valueOf(1));
    final Bytes slotKey = Bytes.concatenate(account.getBytes(), slot.getSlotHash().getBytes());
    final Bytes accBytes = Bytes.of(1, 2, 3);
    final Bytes slotBytes = Bytes.of(7);

    final var u = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u.putAccountInfoState(account, accBytes);
    u.putStorageValueBySlotHash(account, slot.getSlotHash(), slotBytes);
    u.commit();

    final long v = head.getCurrentVersion();
    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(1);
    assertThat(head.getCacheSize(ACCOUNT_STORAGE_STORAGE)).isEqualTo(1);
    assertThat(head.isCached(ACCOUNT_INFO_STATE, account.getBytes())).isTrue();
    assertThat(head.isCached(ACCOUNT_STORAGE_STORAGE, slotKey)).isTrue();
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(v);
              assertThat(cv.getValue()).isEqualTo(accBytes);
            });
    assertThat(head.getCachedValue(ACCOUNT_STORAGE_STORAGE, slotKey))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(v);
              assertThat(cv.getValue()).isEqualTo(slotBytes);
            });
  }

  @Test
  void removalCommitsTombstoneInCacheAtNewVersion() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    commitAccount(account, Bytes.of(1, 2, 3));

    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.isRemoval()).isFalse();
              assertThat(cv.getValue()).isEqualTo(Bytes.of(1, 2, 3));
              assertThat(cv.getVersion()).isEqualTo(1);
            });

    final var u = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u.removeAccountInfoState(account);
    u.commit();

    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.isRemoval()).isTrue();
              assertThat(cv.getVersion()).isEqualTo(head.getCurrentVersion());
            });
  }

  @Test
  void snapshotPinsCacheEpochWhileHeadKeepsAdvancing() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    commitAccount(account, Bytes.of(1));

    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(1);
              assertThat(cv.getValue()).isEqualTo(Bytes.of(1));
            });

    final long epochAtSnapshot;
    try (BonsaiSnapshotWorldStateKeyValueStorage snapshot =
        new BonsaiSnapshotWorldStateKeyValueStorage(head)) {
      epochAtSnapshot = snapshot.getCurrentVersion();
      assertThat(epochAtSnapshot).isEqualTo(head.getCurrentVersion());

      commitAccount(account, Bytes.of(2));

      assertThat(head.getCurrentVersion()).isGreaterThan(epochAtSnapshot);
      assertThat(snapshot.getCurrentVersion()).isEqualTo(epochAtSnapshot);
      assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
          .hasValueSatisfying(
              cv -> {
                assertThat(cv.getVersion()).isEqualTo(head.getCurrentVersion());
                assertThat(cv.getValue()).isEqualTo(Bytes.of(2));
              });
    }
  }

  @Test
  void snapshotReadsPersistedAccountNotNewerHeadCacheEntry() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    final Bytes onSnapshot = Bytes.of(10, 11, 12);
    final Bytes onHeadAfter = Bytes.of(20, 21, 22);

    commitAccount(account, onSnapshot);

    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(1);
              assertThat(cv.getValue()).isEqualTo(onSnapshot);
            });

    try (BonsaiSnapshotWorldStateKeyValueStorage snapshot =
        new BonsaiSnapshotWorldStateKeyValueStorage(head)) {
      commitAccount(account, onHeadAfter);

      assertThat(head.getAccount(account)).contains(onHeadAfter);
      assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
          .hasValueSatisfying(
              cv -> {
                assertThat(cv.getVersion()).isEqualTo(head.getCurrentVersion());
                assertThat(cv.getValue()).isEqualTo(onHeadAfter);
                assertThat(cv.isRemoval()).isFalse();
              });

      assertThat(snapshot.getAccount(account)).contains(onSnapshot);
    }
  }

  @Test
  void snapshotReadsPersistedStorageSlotNotNewerHeadCacheEntry() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    final StorageSlotKey slot = new StorageSlotKey(UInt256.valueOf(3));
    final Bytes slotKey = Bytes.concatenate(account.getBytes(), slot.getSlotHash().getBytes());
    final Bytes vSnapshot = Bytes.of(1);
    final Bytes vHead = Bytes.of(2);

    final var u0 = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u0.putAccountInfoState(account, Bytes.of(1, 2, 3));
    u0.putStorageValueBySlotHash(account, slot.getSlotHash(), vSnapshot);
    u0.commit();

    assertThat(head.getCachedValue(ACCOUNT_STORAGE_STORAGE, slotKey))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(1);
              assertThat(cv.getValue()).isEqualTo(vSnapshot);
              assertThat(cv.isRemoval()).isFalse();
            });

    try (BonsaiSnapshotWorldStateKeyValueStorage snapshot =
        new BonsaiSnapshotWorldStateKeyValueStorage(head)) {
      final var u1 = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
      u1.putStorageValueBySlotHash(account, slot.getSlotHash(), vHead);
      u1.commit();

      assertThat(head.getStorageValueByStorageSlotKey(account, slot)).contains(vHead);
      assertThat(head.getCachedValue(ACCOUNT_STORAGE_STORAGE, slotKey))
          .hasValueSatisfying(
              cv -> {
                assertThat(cv.getVersion()).isEqualTo(head.getCurrentVersion());
                assertThat(cv.getValue()).isEqualTo(vHead);
              });
      assertThat(snapshot.getStorageValueByStorageSlotKey(account, slot)).contains(vSnapshot);
    }
  }

  @Test
  void snapshotStillSeesAccountAfterHeadRemovesIt() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    commitAccount(account, Bytes.of(1, 2, 3));

    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.isRemoval()).isFalse();
              assertThat(cv.getValue()).isEqualTo(Bytes.of(1, 2, 3));
              assertThat(cv.getVersion()).isEqualTo(1);
            });

    try (BonsaiSnapshotWorldStateKeyValueStorage snapshot =
        new BonsaiSnapshotWorldStateKeyValueStorage(head)) {
      final var u = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
      u.removeAccountInfoState(account);
      u.commit();

      assertThat(head.getAccount(account)).isEmpty();
      assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
          .hasValueSatisfying(
              cv -> {
                assertThat(cv.isRemoval()).isTrue();
                assertThat(cv.getVersion()).isEqualTo(head.getCurrentVersion());
              });
      assertThat(snapshot.getAccount(account)).isPresent();
    }
  }

  @Test
  void sequentialCommitsAlongOneBranchKeepSingleLatestCachedValuePerKey() throws Exception {
    newHead(true);
    final Hash account = Hash.hash(Bytes.of(1));
    commitAccount(account, Bytes.of(1));
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(cv -> assertThat(cv.getVersion()).isEqualTo(1));

    commitAccount(account, Bytes.of(2));
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(2);
              assertThat(cv.getValue()).isEqualTo(Bytes.of(2));
            });

    commitAccount(account, Bytes.of(3));

    assertThat(head.getCacheSize(ACCOUNT_INFO_STATE)).isEqualTo(1);
    assertThat(head.getAccount(account)).contains(Bytes.of(3));
    assertThat(head.getCachedValue(ACCOUNT_INFO_STATE, account.getBytes()))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(3);
              assertThat(cv.getValue()).isEqualTo(Bytes.of(3));
              assertThat(cv.isRemoval()).isFalse();
            });
  }

  @Test
  void commitWritesVersionedAccountAndStorageTrieNodes() throws Exception {
    newHead(true);
    final Bytes accountLocation = Bytes.of(0x0a);
    final Bytes accountNode = Bytes.of(1, 2, 3);
    final Bytes32 accountNodeHash = Bytes32.wrap(Hash.hash(accountNode).getBytes());
    final Hash account = Hash.hash(Bytes.of(9));
    final Bytes storageLocation = Bytes.of(0x0b);
    final Bytes storageNode = Bytes.of(4, 5, 6);
    final Bytes32 storageNodeHash = Bytes32.wrap(Hash.hash(storageNode).getBytes());
    final Bytes storageCacheKey = Bytes.concatenate(account.getBytes(), storageLocation);

    final var u = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u.putAccountStateTrieNode(accountLocation, accountNodeHash, accountNode);
    u.putAccountStorageTrieNode(account, storageLocation, storageNodeHash, storageNode);
    u.commit();

    final long v = head.getCurrentVersion();
    assertThat(head.getCacheSize(TRIE_BRANCH_STORAGE)).isEqualTo(2);
    assertThat(head.getCachedValue(TRIE_BRANCH_STORAGE, accountLocation))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(v);
              assertThat(cv.getValue()).isEqualTo(accountNode);
              assertThat(cv.isRemoval()).isFalse();
            });
    assertThat(head.getCachedValue(TRIE_BRANCH_STORAGE, storageCacheKey))
        .hasValueSatisfying(
            cv -> {
              assertThat(cv.getVersion()).isEqualTo(v);
              assertThat(cv.getValue()).isEqualTo(storageNode);
            });
    assertThat(head.getAccountStateTrieNode(accountLocation, accountNodeHash)).contains(accountNode);
    assertThat(head.getAccountStorageTrieNode(account, storageLocation, storageNodeHash))
        .contains(storageNode);
  }

  @Test
  void trieNodeHashMismatchDoesNotReturnCachedBytesAtSamePath() throws Exception {
    newHead(true);
    final Bytes location = Bytes.of(0x01);
    final Bytes node = Bytes.of(7, 7, 7);
    commitAccountTrieNode(location, node);

    assertThat(head.getAccountStateTrieNode(location, Bytes32.wrap(Hash.hash(node).getBytes())))
        .contains(node);
    assertThat(
            head.getAccountStateTrieNode(
                location, Bytes32.wrap(Hash.hash(Bytes.of(8, 8, 8)).getBytes())))
        .isEmpty();
  }

  @Test
  void emptyTrieNodeIsNotInsertedInCache() throws Exception {
    newHead(true);
    final Bytes location = Bytes.of(0x02);
    final var u = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u.putAccountStateTrieNode(
        location, MerkleTrie.EMPTY_TRIE_NODE_HASH, MerkleTrie.EMPTY_TRIE_NODE);
    u.commit();

    assertThat(head.isCached(TRIE_BRANCH_STORAGE, location)).isFalse();
    assertThat(head.getAccountStateTrieNode(location, MerkleTrie.EMPTY_TRIE_NODE_HASH))
        .contains(MerkleTrie.EMPTY_TRIE_NODE);
  }

  @Test
  void snapshotReadsPersistedTrieNodeNotNewerHeadCacheEntry() throws Exception {
    newHead(true);
    final Bytes location = Bytes.of(0x03);
    final Bytes onSnapshot = Bytes.of(10, 11);
    final Bytes onHeadAfter = Bytes.of(20, 21);

    commitAccountTrieNode(location, onSnapshot);

    try (BonsaiSnapshotWorldStateKeyValueStorage snapshot =
        new BonsaiSnapshotWorldStateKeyValueStorage(head)) {
      commitAccountTrieNode(location, onHeadAfter);

      assertThat(
              head.getAccountStateTrieNode(
                  location, Bytes32.wrap(Hash.hash(onHeadAfter).getBytes())))
          .contains(onHeadAfter);
      assertThat(head.getCachedValue(TRIE_BRANCH_STORAGE, location))
          .hasValueSatisfying(
              cv -> {
                assertThat(cv.getVersion()).isEqualTo(head.getCurrentVersion());
                assertThat(cv.getValue()).isEqualTo(onHeadAfter);
              });
      assertThat(
              snapshot.getAccountStateTrieNode(
                  location, Bytes32.wrap(Hash.hash(onSnapshot).getBytes())))
          .contains(onSnapshot);
      assertThat(
              snapshot.getAccountStateTrieNode(
                  location, Bytes32.wrap(Hash.hash(onHeadAfter).getBytes())))
          .isEmpty();
    }
  }

  @Test
  void trieNodeRemovalDropsCacheEntryAndHeadReadMisses() throws Exception {
    newHead(true);
    final Bytes location = Bytes.of(0x04);
    final Bytes node = Bytes.of(1, 1, 1);
    commitAccountTrieNode(location, node);

    final var u = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u.removeAccountStateTrieNode(location);
    u.commit();

    assertThat(head.isCached(TRIE_BRANCH_STORAGE, location)).isFalse();
    assertThat(
            head.getAccountStateTrieNode(location, Bytes32.wrap(Hash.hash(node).getBytes())))
        .isEmpty();
  }

  @Test
  void missingTrieNodeReadDoesNotPopulateCache() throws Exception {
    newHead(true);
    final Bytes location = Bytes.of(0x05);
    assertThat(
            head.getAccountStateTrieNode(
                location, Bytes32.wrap(Hash.hash(Bytes.of(1)).getBytes())))
        .isEmpty();
    assertThat(head.isCached(TRIE_BRANCH_STORAGE, location)).isFalse();
  }

  @Test
  void shallowAndDeepTrieNodesAreBothCachedUnderSharedTrieSize() throws Exception {
    newHead(true);
    final Bytes shallowLocation = Bytes.of(0x01, 0x02);
    final Bytes deepLocation = Bytes.of(0x01, 0x02, 0x03, 0x04, 0x05);
    final Bytes shallowNode = Bytes.of(11);
    final Bytes deepNode = Bytes.of(22);

    commitAccountTrieNode(shallowLocation, shallowNode);
    commitAccountTrieNode(deepLocation, deepNode);

    assertThat(head.getCacheSize(TRIE_BRANCH_STORAGE)).isEqualTo(2);
    assertThat(head.isCached(TRIE_BRANCH_STORAGE, shallowLocation)).isTrue();
    assertThat(head.isCached(TRIE_BRANCH_STORAGE, deepLocation)).isTrue();
    assertThat(
            head.getAccountStateTrieNode(
                shallowLocation, Bytes32.wrap(Hash.hash(shallowNode).getBytes())))
        .contains(shallowNode);
    assertThat(
            head.getAccountStateTrieNode(
                deepLocation, Bytes32.wrap(Hash.hash(deepNode).getBytes())))
        .contains(deepNode);
  }

  @Test
  void storageTrieNodesAreCachedPerAccount() throws Exception {
    newHead(true);
    final Hash accountA = Hash.hash(Bytes.of(1));
    final Hash accountB = Hash.hash(Bytes.of(2));
    final Bytes location = Bytes.of(0x0a);
    final Bytes nodeA = Bytes.of(1, 1);
    final Bytes nodeB = Bytes.of(2, 2);

    final var u = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u.putAccountStorageTrieNode(
        accountA, location, Bytes32.wrap(Hash.hash(nodeA).getBytes()), nodeA);
    u.putAccountStorageTrieNode(
        accountB, location, Bytes32.wrap(Hash.hash(nodeB).getBytes()), nodeB);
    u.commit();

    assertThat(head.getAccountStorageTrieNode(accountA, location, Bytes32.wrap(Hash.hash(nodeA).getBytes())))
        .contains(nodeA);
    assertThat(head.getAccountStorageTrieNode(accountB, location, Bytes32.wrap(Hash.hash(nodeB).getBytes())))
        .contains(nodeB);
    assertThat(head.isCached(TRIE_BRANCH_STORAGE, Bytes.concatenate(accountA.getBytes(), location)))
        .isTrue();
    assertThat(head.isCached(TRIE_BRANCH_STORAGE, Bytes.concatenate(accountB.getBytes(), location)))
        .isTrue();
    assertThat(head.getCacheSize(TRIE_BRANCH_STORAGE)).isEqualTo(2);
  }

  private void commitAccount(final Hash accountHash, final Bytes value) {
    final var u = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u.putAccountInfoState(accountHash, value);
    u.commit();
  }

  private void commitAccountTrieNode(final Bytes location, final Bytes node) {
    final var u = (BonsaiWorldStateKeyValueStorage.CachedUpdater) head.updater();
    u.putAccountStateTrieNode(location, Bytes32.wrap(Hash.hash(node).getBytes()), node);
    u.commit();
  }
}
