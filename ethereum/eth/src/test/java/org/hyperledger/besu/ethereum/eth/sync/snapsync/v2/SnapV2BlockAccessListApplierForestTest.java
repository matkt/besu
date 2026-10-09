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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.RangeManager.MAX_RANGE;
import static org.hyperledger.besu.ethereum.trie.RangeManager.MIN_RANGE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapRequestContext;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2AccountRangeRequest;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/**
 * snap/2 BAL application and reorg corrections on Forest storage, where nodes are keyed by hash.
 */
class SnapV2BlockAccessListApplierForestTest {

  private static final Address ALICE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address BOB =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final Address CHARLIE =
      Address.fromHexString("0x3333333333333333333333333333333333333333");
  private static final Address FRANK =
      Address.fromHexString("0x6666666666666666666666666666666666666666");
  private static final Address GRACE =
      Address.fromHexString("0x7777777777777777777777777777777777777777");

  private static final UInt256 SLOT_1 = UInt256.valueOf(1);
  private static final UInt256 SLOT_2 = UInt256.valueOf(2);
  private static final UInt256 SLOT_3 = UInt256.valueOf(3);

  private final ForestWorldStateKeyValueStorage forestStorage =
      new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
  private final WorldStateStorageCoordinator coordinator =
      new WorldStateStorageCoordinator(forestStorage);

  @Test
  void appliesBalsOnDownloadedStateAndTracksTheNewRoot() {
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Bytes code = Bytes.of(0x60, 0x80);
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    final Block block2 =
        b.appendBlockWithBal(
            block1.getHeader(),
            b.merge(
                b.balWithBalances(Map.of(ALICE, Wei.of(80), GRACE, Wei.of(50))),
                b.balWithNonceChange(BOB, 7L),
                b.balWithCodeChange(CHARLIE, code),
                b.balWithStorageChanges(
                    FRANK, Map.of(SLOT_1, UInt256.valueOf(111), SLOT_2, UInt256.ZERO))),
            2L);
    b.appendBlockWithBal(block2.getHeader(), b.balWithBalances(Map.of(ALICE, Wei.of(90))), 3L);

    final Map<Address, TestAccount> state = new LinkedHashMap<>();
    state.put(ALICE, new TestAccount(1L, Wei.of(50)));
    state.put(BOB, new TestAccount(3L, Wei.of(100)));
    state.put(CHARLIE, new TestAccount(0L, Wei.ONE));
    state.put(
        FRANK,
        new TestAccount(1L, Wei.of(100))
            .withSlot(SLOT_1, UInt256.valueOf(200))
            .withSlot(SLOT_2, UInt256.valueOf(200))
            .withSlot(SLOT_3, UInt256.valueOf(300)));

    final SnapV2BlockAccessListApplier applier = applier(b);
    writeState(forestStorage, state, true);

    final SnapV2BlockAccessListApplier.BatchState batch =
        applier.applyBlockAccessLists(
            2L, 2L, fullAccountRange(), new DownloadedStorageRangeTracker());
    batch.commit();

    state.put(ALICE, state.get(ALICE).withBalance(Wei.of(80)));
    state.put(BOB, state.get(BOB).withNonce(7L));
    state.put(CHARLIE, state.get(CHARLIE).withCode(code));
    state.put(
        FRANK,
        new TestAccount(1L, Wei.of(100))
            .withSlot(SLOT_1, UInt256.valueOf(111))
            .withSlot(SLOT_3, UInt256.valueOf(300)));
    state.put(GRACE, new TestAccount(0L, Wei.of(50)));
    final Bytes32 expectedRootAtBlock2 = expectedRoot(state);
    assertThat(batch.accountTrie().getRootHash()).isEqualTo(expectedRootAtBlock2);
    assertStateFullyStored(expectedRootAtBlock2, state);
    // the root stays at the empty location, so the partial state is not reported as available
    assertThat(forestStorage.isWorldStateAvailable(expectedRootAtBlock2)).isFalse();

    // block 3 must open the trie at the root committed for block 2, not at the downloaded one
    applier
        .applyBlockAccessLists(3L, 3L, fullAccountRange(), new DownloadedStorageRangeTracker())
        .commit();

    state.put(ALICE, state.get(ALICE).withBalance(Wei.of(90)));
    final Bytes32 expectedRootAtBlock3 = expectedRoot(state);
    assertStateFullyStored(expectedRootAtBlock3, state);
    assertThat(applier.readLocalStorageRoot(FRANK.addressHash()))
        .contains(storageRoot(state.get(FRANK)));
  }

  @Test
  void appliesBalsOnAPartialAccountRangePersistedOnForest() {
    final Map<Address, TestAccount> sourceState = new LinkedHashMap<>();
    for (int i = 1; i <= 8; i++) {
      sourceState.put(
          Address.fromHexString(String.format("0x%040x", i)),
          new TestAccount(i, Wei.of(i * 10L)).withCode(Bytes.of(i)));
    }
    final ForestWorldStateKeyValueStorage sourceStorage =
        new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
    final Bytes32 sourceRoot = writeState(sourceStorage, sourceState, false);
    final BlockHeader pivot = pivotHeader(sourceRoot);

    // a peer serves the first three accounts of the trie, with the proofs of the range edges
    final MerkleTrie<Bytes, Bytes> sourceTrie = savedAccountTrie(sourceStorage, sourceRoot);
    final TreeMap<Bytes32, Bytes> served = new TreeMap<>(sourceTrie.entriesFrom(MIN_RANGE, 3));
    final WorldStateProofProvider proofProvider =
        new WorldStateProofProvider(new WorldStateStorageCoordinator(sourceStorage));
    final List<Bytes> proofs =
        new ArrayList<>(
            proofProvider.getAccountProofRelatedNodes(Hash.wrap(sourceRoot), MIN_RANGE));
    proofs.addAll(
        proofProvider.getAccountProofRelatedNodes(Hash.wrap(sourceRoot), served.lastKey()));

    final SnapV2AccountRangeRequest request =
        new SnapV2AccountRangeRequest(pivot, MIN_RANGE, MAX_RANGE);
    request.addResponse(proofProvider, served, proofs);
    assertThat(request.isResponseReceived()).isTrue();
    persist(request);
    assertThat(forestStorage.isWorldStateAvailable(sourceRoot)).isFalse();

    final Address persisted = addressOfHash(sourceState, served.firstKey());
    final Address notPersisted =
        sourceState.keySet().stream()
            .filter(address -> !served.containsKey(asBytes32(address.addressHash())))
            .findFirst()
            .orElseThrow();

    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Block block1 = b.appendBlockWithBal(b.header(0), b.emptyBal(), 1L);
    b.appendBlockWithBal(
        block1.getHeader(),
        b.balWithBalances(Map.of(persisted, Wei.of(999), notPersisted, Wei.of(777))),
        2L);

    final SnapV2BlockAccessListApplier applier = applier(b);
    final DownloadedAccountRangeTracker accountTracker = new DownloadedAccountRangeTracker();
    accountTracker.registerPending(MIN_RANGE, served.lastKey(), 0);

    final SnapV2BlockAccessListApplier.BatchState batch =
        applier.applyBlockAccessLists(2L, 2L, accountTracker, new DownloadedStorageRangeTracker());
    batch.commit();

    // only the persisted account changes; its nonce and code come from the downloaded trie leaf
    sourceState.put(persisted, sourceState.get(persisted).withBalance(Wei.of(999)));
    assertThat(batch.accountTrie().getRootHash()).isEqualTo(expectedRoot(sourceState));
  }

  @Test
  void appliesReorgCorrectionsOnForest() {
    final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
    final Map<Address, TestAccount> state = new LinkedHashMap<>();
    state.put(ALICE, new TestAccount(1L, Wei.of(50)));
    state.put(BOB, new TestAccount(1L, Wei.of(100)));
    state.put(
        FRANK,
        new TestAccount(1L, Wei.of(100))
            .withSlot(SLOT_1, UInt256.valueOf(200))
            .withSlot(SLOT_2, UInt256.valueOf(200)));

    final SnapV2BlockAccessListApplier applier = applier(b);
    writeState(forestStorage, state, true);

    final TestAccount canonicalAlice = new TestAccount(1L, Wei.of(70));
    final TestAccount canonicalFrank =
        new TestAccount(1L, Wei.of(100))
            .withSlot(SLOT_1, UInt256.valueOf(42))
            .withSlot(SLOT_2, UInt256.valueOf(200));
    final ReorgPlan plan =
        new ReorgPlan(
            b.header(0),
            b.header(0),
            b.header(0),
            Set.of(ALICE.addressHash(), BOB.addressHash()),
            Map.of(FRANK.addressHash(), Set.of(ReorgBlockchainBuilder.slotHash(SLOT_1))));
    final FetchedReorgState fetched =
        new FetchedReorgState(
            Map.of(
                ALICE.addressHash(), Optional.of(trieValue(canonicalAlice)),
                BOB.addressHash(), Optional.empty(),
                FRANK.addressHash(), Optional.of(trieValue(canonicalFrank))),
            Map.of(
                FRANK.addressHash(),
                Map.of(ReorgBlockchainBuilder.slotHash(SLOT_1), Optional.of(UInt256.valueOf(42)))),
            Map.of());

    final ReorgRecoveryResult result =
        applier.applyReorgCorrections(
            plan, fetched, fullAccountRange(), new DownloadedStorageRangeTracker());

    assertThat(result.deletedAccounts()).containsExactly(BOB.addressHash());
    state.remove(BOB);
    state.put(ALICE, canonicalAlice);
    state.put(FRANK, canonicalFrank);
    assertStateFullyStored(expectedRoot(state), state);
    assertThat(applier.readLocalStorageRoot(FRANK.addressHash()))
        .contains(storageRoot(canonicalFrank));
    assertThat(applier.readLocalStorageRoot(BOB.addressHash())).isEmpty();
  }

  private SnapV2BlockAccessListApplier applier(final ReorgBlockchainBuilder b) {
    return new SnapV2BlockAccessListApplier(
        coordinator, b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule());
  }

  private void persist(final SnapV2AccountRangeRequest request) {
    final SnapRequestContext downloadState = mock(SnapRequestContext.class);
    when(downloadState.getMetricsManager()).thenReturn(mock(SnapSyncMetricsManager.class));
    final ForestWorldStateKeyValueStorage.Updater updater = forestStorage.updater();
    request.persist(
        coordinator,
        updater,
        downloadState,
        mock(SnapSyncProcessState.class),
        SnapSyncConfiguration.getDefault());
    updater.commit();
  }

  private static DownloadedAccountRangeTracker fullAccountRange() {
    final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();
    tracker.registerPending(MIN_RANGE, MAX_RANGE, 0);
    return tracker;
  }

  private static BlockHeader pivotHeader(final Bytes32 stateRoot) {
    return new BlockHeaderTestFixture().number(1L).stateRoot(Hash.wrap(stateRoot)).buildHeader();
  }

  /**
   * Writes the tries and code of the state; its root goes to the empty location like snap/2, or
   * under its hash like a saved world state.
   */
  private static Bytes32 writeState(
      final ForestWorldStateKeyValueStorage storage,
      final Map<Address, TestAccount> state,
      final boolean rootAtEmptyLocation) {
    final ForestWorldStateKeyValueStorage.Updater updater = storage.updater();
    final MerkleTrie<Bytes, Bytes> accountTrie =
        savedAccountTrie(storage, MerkleTrie.EMPTY_TRIE_NODE_HASH);
    state.forEach(
        (address, account) -> {
          final MerkleTrie<Bytes, Bytes> storageTrie = storageTrie(account);
          storageTrie.commit(
              (location, hash, value) -> updater.putAccountStorageTrieNode(hash, value));
          updater.putCode(account.code);
          accountTrie.put(
              address.addressHash().getBytes(), RLP.encode(trieValue(account)::writeTo));
        });
    accountTrie.commit(
        (location, hash, value) -> {
          if (rootAtEmptyLocation) {
            updater.putAccountStateTrieNode(location, hash, value);
          } else {
            updater.putAccountStateTrieNode(hash, value);
          }
        });
    updater.commit();
    return accountTrie.getRootHash();
  }

  private static Bytes32 expectedRoot(final Map<Address, TestAccount> state) {
    return writeState(
        new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage()), state, false);
  }

  /** Checks that every account, slot and code of the state is readable from the given root. */
  private void assertStateFullyStored(final Bytes32 root, final Map<Address, TestAccount> state) {
    final Map<Bytes32, Bytes> accounts =
        new StoredMerklePatriciaTrie<Bytes, Bytes>(
                (location, hash) ->
                    location.isEmpty()
                        ? forestStorage
                            .getTrieNodeUnsafe(location)
                            .filter(node -> Hash.hash(node).getBytes().equals(hash))
                        : forestStorage.getAccountStateTrieNode(hash),
                root,
                b -> b,
                b -> b)
            .entriesFrom(MIN_RANGE, Integer.MAX_VALUE);
    assertThat(accounts).hasSize(state.size());
    state.forEach(
        (address, expected) -> {
          final PmtStateTrieAccountValue account =
              PmtStateTrieAccountValue.readFrom(
                  RLP.input(accounts.get(asBytes32(address.addressHash()))));
          assertThat(account).isEqualTo(trieValue(expected));
          final MerkleTrie<Bytes, Bytes> storageTrie =
              new StoredMerklePatriciaTrie<>(
                  (location, hash) -> forestStorage.getAccountStorageTrieNode(hash),
                  asBytes32(account.getStorageRoot()),
                  b -> b,
                  b -> b);
          assertThat(storageTrie.entriesFrom(MIN_RANGE, Integer.MAX_VALUE))
              .hasSize(expected.slots.size());
          assertThat(forestStorage.getCode(account.getCodeHash())).contains(expected.code);
        });
  }

  private static MerkleTrie<Bytes, Bytes> savedAccountTrie(
      final ForestWorldStateKeyValueStorage storage, final Bytes32 root) {
    return new StoredMerklePatriciaTrie<>(
        (location, hash) -> storage.getAccountStateTrieNode(hash), root, b -> b, b -> b);
  }

  private static MerkleTrie<Bytes, Bytes> storageTrie(final TestAccount account) {
    final MerkleTrie<Bytes, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) -> Optional.empty(), MerkleTrie.EMPTY_TRIE_NODE_HASH, b -> b, b -> b);
    account.slots.forEach(
        (key, value) ->
            trie.put(
                ReorgBlockchainBuilder.slotHash(key).getBytes(),
                RLP.encode(out -> out.writeBytes(value.toMinimalBytes()))));
    return trie;
  }

  private static Bytes32 storageRoot(final TestAccount account) {
    return storageTrie(account).getRootHash();
  }

  private static PmtStateTrieAccountValue trieValue(final TestAccount account) {
    return new PmtStateTrieAccountValue(
        account.nonce,
        account.balance,
        Hash.wrap(storageRoot(account)),
        account.code.isEmpty() ? Hash.EMPTY : Hash.hash(account.code));
  }

  private static Address addressOfHash(
      final Map<Address, TestAccount> state, final Bytes32 accountHash) {
    return state.keySet().stream()
        .filter(address -> asBytes32(address.addressHash()).equals(accountHash))
        .findFirst()
        .orElseThrow();
  }

  private static Bytes32 asBytes32(final Hash hash) {
    return Bytes32.wrap(hash.getBytes());
  }

  private record TestAccount(long nonce, Wei balance, Map<UInt256, UInt256> slots, Bytes code) {
    TestAccount(final long nonce, final Wei balance) {
      this(nonce, balance, new TreeMap<>(), Bytes.EMPTY);
    }

    TestAccount withSlot(final UInt256 key, final UInt256 value) {
      final Map<UInt256, UInt256> newSlots = new TreeMap<>(slots);
      newSlots.put(key, value);
      return new TestAccount(nonce, balance, newSlots, code);
    }

    TestAccount withBalance(final Wei newBalance) {
      return new TestAccount(nonce, newBalance, slots, code);
    }

    TestAccount withNonce(final long newNonce) {
      return new TestAccount(newNonce, balance, slots, code);
    }

    TestAccount withCode(final Bytes newCode) {
      return new TestAccount(nonce, balance, slots, newCode);
    }
  }
}
