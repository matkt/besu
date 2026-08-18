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
package org.hyperledger.besu.ethereum.trie.pathbased.pbt.rolling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary.DefaultBinaryStateRootCommitter;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BinaryTestSupport;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.PbtTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;

import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Binary-trie mirror of {@code LogRollingTests}. Exercises trie-log roll-forward / roll-back
 * against a {@link DataStorageFormat#BINARY} world state and asserts the binary state root,
 * recomputed via {@link DefaultBinaryStateRootCommitter}, matches the live-execution root after each roll.
 *
 * <p>This only works because the {@link PbtTrieLogFactory} carries the storage slot key preimage;
 * the legacy MPT factory serializes only the slot hash, which would leave {@code
 * StorageSlotKey.getSlotKey()} empty and make {@code BinaryStateRootCommitter.applyStorage} throw
 * after a roll.
 *
 * <p>The scenario tests below drive the REAL {@link DefaultBinaryStateRootCommitter} against a real BINARY
 * {@link BonsaiWorldState} for every root. Each scenario applies changes → persists (capturing a
 * PBT trie-log layer) → verifies the root → rolls back to the empty state (root {@link Hash#ZERO})
 * → rolls forward → verifies the root again. Multi-block scenarios chain several layers and roll
 * them back/forward in order. The rollback-to-ZERO assertion proves the binary trie fully reverts,
 * including reference-aware removal of shared CODE_ZONE chunks (EIP-8297).
 */
@ExtendWith(MockitoExtension.class)
class PbtLogRollingTests {

  private static final DataStorageConfiguration CONFIG =
      DataStorageConfiguration.DEFAULT_BINARY_CONFIG;
  private static final Address ADDRESS_ONE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address ADDRESS_TWO =
      Address.fromHexString("0x2222222222222222222222222222222222222222");
  private static final Address ADDRESS_THREE =
      Address.fromHexString("0x3333333333333333333333333333333333333333");
  private static final Address DELEGATION_TARGET =
      Address.fromHexString("0xcccccccccccccccccccccccccccccccccccccccc");

  private BonsaiWorldStateProvider archive;
  private InMemoryKeyValueStorageProvider provider;
  private KeyValueStorage trieLogStorage;

  private BonsaiWorldStateProvider secondArchive;
  private InMemoryKeyValueStorageProvider secondProvider;

  private final Blockchain blockchain = mock(Blockchain.class);
  private final DefaultBinaryStateRootCommitter binaryCommitter = new DefaultBinaryStateRootCommitter();

  @BeforeEach
  void createStorage() {
    provider = new InMemoryKeyValueStorageProvider();
    archive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(
            blockchain, DataStorageFormat.BINARY);
    trieLogStorage =
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE);

    secondProvider = new InMemoryKeyValueStorageProvider();
    secondArchive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(
            blockchain, DataStorageFormat.BINARY);
  }

  private BonsaiWorldState newWorldState(
      final BonsaiWorldStateProvider archiveProvider,
      final InMemoryKeyValueStorageProvider storageProvider) {
    final BonsaiWorldState worldState =
        new BonsaiWorldState(
            archiveProvider,
            new BonsaiWorldStateKeyValueStorage(storageProvider, new NoOpMetricsSystem(), CONFIG),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new BonsaiCodeCache());
    BinaryTestSupport.initializeEmptyBinaryTrieRoot(worldState);
    return worldState;
  }

  private Hash liveRoot(final BonsaiWorldState worldState) {
    return new DefaultBinaryStateRootCommitter()
        .compute(worldState, null, worldState.updater().copy())
        .root();
  }

  @Test
  void rollForwardTwiceThenRollBack_matchesLiveBinaryRoot() {
    // --- Live execution: build two blocks of state on worldState 1. ---
    final BonsaiWorldState worldState = newWorldState(archive, provider);

    WorldUpdater updater = worldState.updater();
    MutableAccount account = updater.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
    account.setCode(Bytes.of(0, 1, 2));
    account.setStorageValue(UInt256.ONE, UInt256.ONE);
    updater.commit();
    final Hash rootOne = liveRoot(worldState);
    final BlockHeader headerOne =
        new BlockHeaderTestFixture()
            .parentHash(Hash.ZERO)
            .number(1)
            .stateRoot(rootOne)
            .buildHeader();
    worldState.persist(headerOne, binaryCommitter);

    WorldUpdater updater2 = worldState.updater();
    MutableAccount account2 = updater2.getAccount(ADDRESS_ONE);
    account2.setStorageValue(UInt256.ONE, UInt256.valueOf(2));
    updater2.commit();
    final Hash rootTwo = liveRoot(worldState);
    final BlockHeader headerTwo =
        new BlockHeaderTestFixture()
            .parentHash(headerOne.getHash())
            .number(2)
            .stateRoot(rootTwo)
            .buildHeader();
    worldState.persist(headerTwo, binaryCommitter);

    // --- Replay on a fresh world state via PBT trie logs. ---
    final BonsaiWorldState secondWorldState = newWorldState(secondArchive, secondProvider);
    final BonsaiWorldStateUpdateAccumulator secondUpdater = secondWorldState.updater();

    final TrieLogLayer layerOne = readPbtTrieLog(trieLogStorage, headerOne.getHash());
    secondUpdater.rollForward(layerOne);
    secondUpdater.commit();
    secondWorldState.persist(null, binaryCommitter);
    assertThat(secondWorldState.rootHash()).isEqualTo(rootOne);

    final TrieLogLayer layerTwo = readPbtTrieLog(trieLogStorage, headerTwo.getHash());
    secondUpdater.rollForward(layerTwo);
    secondUpdater.commit();
    secondWorldState.persist(null, binaryCommitter);
    assertThat(secondWorldState.rootHash()).isEqualTo(rootTwo);

    // Roll back the second layer; the binary root must return to rootOne.
    secondUpdater.rollBack(layerTwo);
    secondUpdater.commit();
    secondWorldState.persist(null, binaryCommitter);
    assertThat(secondWorldState.rootHash()).isEqualTo(rootOne);
  }

  private static TrieLogLayer readPbtTrieLog(final KeyValueStorage storage, final Hash key) {
    return storage
        .get(key.getBytes().toArrayUnsafe())
        .map(bytes -> PbtTrieLogFactory.readFrom(new BytesValueRLPInput(Bytes.wrap(bytes), false)))
        .orElseThrow(() -> new IllegalStateException("Missing trie log for " + key));
  }

  // --------------------------------------------------------------------------------------------
  // Scenario-driven rolling tests against the REAL BinaryStateRootCommitter.
  //
  // Each scenario starts from a fresh empty binary trie (root Hash.ZERO), applies changes,
  // persists (capturing a PBT trie-log layer), and then rolls back to the empty state and
  // forward again. The rollback-to-ZERO assertion proves the binary trie fully reverts,
  // including reference-aware removal of shared CODE_ZONE chunks (EIP-8297).
  // --------------------------------------------------------------------------------------------

  @Test
  void scenarioAccountCreation_rollsBackToZero() {
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 = f.applyAndPersist(u -> u.createAccount(ADDRESS_ONE, 1, Wei.of(1L)));
      assertThat(b1.root).as("creation: non-zero root").isNotEqualTo(Hash.ZERO);

      assertThat(f.rollback(b1)).as("rollback creation -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1)).as("rollforward creation -> root1").isEqualTo(b1.root);
    }
  }

  @Test
  void scenarioAccountUpdate_rollsBackToPriorRoots() {
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 = f.applyAndPersist(u -> u.createAccount(ADDRESS_ONE, 1, Wei.of(1L)));
      final PersistedBlock b2 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.getAccount(ADDRESS_ONE);
                a.setNonce(5);
                a.setBalance(Wei.of(999L));
              });

      assertThat(f.rollback(b2)).as("rollback update -> root1").isEqualTo(b1.root);
      assertThat(f.rollback(b1)).as("rollback create -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1)).as("rollforward create -> root1").isEqualTo(b1.root);
      assertThat(f.rollforward(b2)).as("rollforward update -> root2").isEqualTo(b2.root);
    }
  }

  @Test
  void scenarioAccountDeletion_rollsBackToPriorRoots() {
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 = f.applyAndPersist(u -> u.createAccount(ADDRESS_ONE, 1, Wei.of(1L)));
      final PersistedBlock b2 = f.applyAndPersist(u -> u.deleteAccount(ADDRESS_ONE));

      assertThat(f.rollback(b2)).as("rollback deletion -> root1").isEqualTo(b1.root);
      assertThat(f.rollback(b1)).as("rollback creation -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1)).as("rollforward creation -> root1").isEqualTo(b1.root);
      assertThat(f.rollforward(b2)).as("rollforward deletion -> root2").isEqualTo(b2.root);
    }
  }

  @Test
  void scenarioEmptyEoa_zeroBasicData_rollsBackToZero() {
    try (final BinaryFixture f = new BinaryFixture()) {
      // Empty EOA: nonce=0, balance=0, no code -> basic-data leaf is zero-absent; only the
      // code-hash (EMPTY) leaf is written.
      final PersistedBlock b1 = f.applyAndPersist(u -> u.createAccount(ADDRESS_ONE, 0, Wei.ZERO));
      assertThat(b1.root)
          .as("empty EOA: non-zero root (code-hash leaf present)")
          .isNotEqualTo(Hash.ZERO);

      assertThat(f.rollback(b1)).as("rollback empty EOA -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1)).as("rollforward empty EOA -> root1").isEqualTo(b1.root);
    }
  }

  @Test
  void scenarioCodeSmallContract_rollsBackToZero() {
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                a.setCode(Bytes.fromHexString("0x6301" + "00".repeat(40) + "63aabbccdd"));
              });
      assertThat(b1.root).as("small contract: non-zero root").isNotEqualTo(Hash.ZERO);

      assertThat(f.rollback(b1)).as("rollback small contract -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1)).as("rollforward small contract -> root1").isEqualTo(b1.root);
    }
  }

  @Test
  void scenarioCodeAcrossGroupBoundary_257Chunks_rollsBackToZero() {
    // 257 chunks of code spans two code-group stems (group 0 holds chunks 0-255, group 1 holds
    // chunk 256). Each non-push byte contributes one byte to a chunk; a chunk holds 31 code bytes,
    // so 7937 bytes of 0x01 (ADD) chunkify to exactly 257 chunks.
    final Bytes code = Bytes.repeat((byte) 0x01, 7937);
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                a.setCode(code);
              });
      assertThat(b1.root).as("group-boundary code: non-zero root").isNotEqualTo(Hash.ZERO);

      assertThat(f.rollback(b1)).as("rollback group-boundary code -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1))
          .as("rollforward group-boundary code -> root1")
          .isEqualTo(b1.root);
    }
  }

  @Test
  void scenarioCodeZeroChunksAbsent_rollsBackToZero() {
    // Code that chunkifies to all-zero chunks (those chunks are never written; EIP-8297
    // zero-absent). Only the code-hash leaf and basic-data leaf are present.
    final Bytes code = Bytes.repeat((byte) 0x00, 64);
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                a.setCode(code);
              });
      assertThat(b1.root).as("zero-chunks code: non-zero root").isNotEqualTo(Hash.ZERO);

      assertThat(f.rollback(b1)).as("rollback zero-chunks code -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1)).as("rollforward zero-chunks code -> root1").isEqualTo(b1.root);
    }
  }

  @Test
  void scenarioCodeDelegation_eip7702_rollsBackToZero() {
    // EIP-7702 delegation: code = 0xef0100 + 20-byte address. Stored as a DELEGATION header leaf,
    // no CODE_ZONE chunks.
    final Bytes delegationCode =
        Bytes.concatenate(Bytes.fromHexString("0xef0100"), DELEGATION_TARGET.getBytes());
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                a.setCode(delegationCode);
              });
      assertThat(b1.root).as("delegation: non-zero root").isNotEqualTo(Hash.ZERO);

      assertThat(f.rollback(b1)).as("rollback delegation -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1)).as("rollforward delegation -> root1").isEqualTo(b1.root);
    }
  }

  @Test
  void scenarioCodeHashStartingWithDelegationMarker_rollsBackToZero() {
    // Contract whose code hash starts with 0xef0100 but whose code itself is not a delegation
    // (stored as real code in CODE_ZONE). Verifies the committer distinguishes the marker from an
    // actual delegation indicator.
    final Bytes code = Bytes.fromHexString("0x0000000000000000000000000000000000000000637401");
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                a.setCode(code);
              });
      assertThat(b1.root).as("marker-hash code: non-zero root").isNotEqualTo(Hash.ZERO);

      assertThat(f.rollback(b1)).as("rollback marker-hash code -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1)).as("rollforward marker-hash code -> root1").isEqualTo(b1.root);
    }
  }

  @Test
  void scenarioSharedCodeTwoAccounts_rollsBackToZeroAndRespectsSharing() {
    // Two accounts with identical 129-chunk code. 129 chunks ~= 129 * 31 = 3999 bytes of 0x01.
    final Bytes sharedCode = Bytes.repeat((byte) 0x01, 3999);
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                a.setCode(sharedCode);
                final MutableAccount b = u.createAccount(ADDRESS_TWO, 1, Wei.of(1L));
                b.setCode(sharedCode);
              });
      assertThat(b1.root).as("shared code: non-zero root").isNotEqualTo(Hash.ZERO);

      // Rollback both -> ZERO (chunks removed once the shared code hash becomes unreferenced).
      assertThat(f.rollback(b1)).as("rollback shared code -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1)).as("rollforward shared code -> root1").isEqualTo(b1.root);

      // Now: block 1 creates both, block 2 deletes ONE of them. Rolling back block 2 must return
      // to root1: the survivor keeps the chunks (EIP-8297 sharing respected), and rollback rewrites
      // the deleted account's chunks so the state matches root1 exactly.
      final PersistedBlock recreated =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                a.setCode(sharedCode);
                final MutableAccount b = u.createAccount(ADDRESS_TWO, 1, Wei.of(1L));
                b.setCode(sharedCode);
              });
      final PersistedBlock deleteOne = f.applyAndPersist(u -> u.deleteAccount(ADDRESS_ONE));

      assertThat(f.rollback(deleteOne))
          .as("rollback delete-one -> root1 (survivor keeps chunks)")
          .isEqualTo(recreated.root);
    }
  }

  @Test
  void scenarioSharedCodeWithUntouchedPreExistingAccount_rollbackKeepsChunks() {
    // The sharing+untouched case that the accumulator-scan reference check could not handle:
    // account B is pre-existing with code H and is NOT touched by the block that creates A with
    // the same code H. Rolling back A's creation must keep H's chunks (B still references them),
    // so the root after rollback equals the state with only B — NOT zero.
    final Bytes sharedCode = Bytes.repeat((byte) 0x01, 3999);
    try (final BinaryFixture f = new BinaryFixture()) {
      // Block 1: create B with code H.
      final PersistedBlock b1 =
          f.applyAndPersist(
              u -> {
                final MutableAccount b = u.createAccount(ADDRESS_TWO, 1, Wei.of(1L));
                b.setCode(sharedCode);
              });
      assertThat(b1.root).as("B only: non-zero root").isNotEqualTo(Hash.ZERO);

      // Block 2: create A with the same code H. B is untouched by this block.
      final PersistedBlock b2 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                a.setCode(sharedCode);
              });
      assertThat(b2.root).as("A and B: non-zero root").isNotEqualTo(Hash.ZERO);

      // Rolling back block 2 (A's creation) must return to root1 (B only): H's chunks are kept
      // because B — untouched, invisible to an accumulator scan — already referenced H before
      // block 2. The trie-log-carried presence signal records H as present-before-block-2, so the
      // committer does NOT drop the chunks.
      assertThat(f.rollback(b2))
          .as("rollback A's creation -> root1 (B's chunks kept)")
          .isEqualTo(b1.root);

      // Rolling forward block 2 recreates A; root returns to the A-and-B root.
      assertThat(f.rollforward(b2))
          .as("rollforward A's creation -> root2 (A and B)")
          .isEqualTo(b2.root);
    }
  }

  @Test
  void scenarioStorageHeaderZoneSlots_rollsBackToZero() {
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                a.setStorageValue(UInt256.ZERO, UInt256.ONE);
                a.setStorageValue(UInt256.ONE, UInt256.valueOf(2));
                a.setStorageValue(UInt256.valueOf(63), UInt256.valueOf(3));
              });
      assertThat(b1.root).as("header-zone storage: non-zero root").isNotEqualTo(Hash.ZERO);

      assertThat(f.rollback(b1)).as("rollback header-zone storage -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1))
          .as("rollforward header-zone storage -> root1")
          .isEqualTo(b1.root);
    }
  }

  @Test
  void scenarioStorageZoneSlots_rollsBackToZero() {
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                a.setStorageValue(UInt256.valueOf(64), UInt256.valueOf(4));
                a.setStorageValue(UInt256.valueOf(255), UInt256.valueOf(5));
                a.setStorageValue(UInt256.valueOf(256), UInt256.valueOf(6));
                a.setStorageValue(UInt256.valueOf(511), UInt256.valueOf(7));
                a.setStorageValue(UInt256.valueOf(512), UInt256.valueOf(8));
              });
      assertThat(b1.root).as("storage-zone storage: non-zero root").isNotEqualTo(Hash.ZERO);

      assertThat(f.rollback(b1)).as("rollback storage-zone storage -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1))
          .as("rollforward storage-zone storage -> root1")
          .isEqualTo(b1.root);
    }
  }

  @Test
  void scenarioStorageAcrossHeaderBoundary_rollsBackToZero() {
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                a.setStorageValue(UInt256.ZERO, UInt256.ONE);
                a.setStorageValue(UInt256.ONE, UInt256.valueOf(2));
                a.setStorageValue(UInt256.valueOf(63), UInt256.valueOf(3));
                a.setStorageValue(UInt256.valueOf(64), UInt256.valueOf(4));
                a.setStorageValue(UInt256.valueOf(255), UInt256.valueOf(5));
                a.setStorageValue(UInt256.valueOf(256), UInt256.valueOf(6));
                // 2^256 - 1 (max slot, storage zone).
                a.setStorageValue(
                    UInt256.fromHexString(
                        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
                    UInt256.valueOf(7));
              });
      assertThat(b1.root).as("cross-boundary storage: non-zero root").isNotEqualTo(Hash.ZERO);

      assertThat(f.rollback(b1)).as("rollback cross-boundary storage -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1))
          .as("rollforward cross-boundary storage -> root1")
          .isEqualTo(b1.root);
    }
  }

  @Test
  void scenarioStorageZeroSlotIsAbsent_rollsBackToZero() {
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                a.setStorageValue(UInt256.ZERO, UInt256.ONE);
                // Slot 1 = zero is zero-absent (no leaf written).
                a.setStorageValue(UInt256.ONE, UInt256.ZERO);
              });
      assertThat(b1.root).as("zero-absent storage: non-zero root").isNotEqualTo(Hash.ZERO);

      assertThat(f.rollback(b1)).as("rollback zero-absent storage -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1))
          .as("rollforward zero-absent storage -> root1")
          .isEqualTo(b1.root);
    }
  }

  @Test
  void scenarioStorageAndCodeCombined_rollsBackToZero() {
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                a.setCode(Bytes.repeat((byte) 0x01, 200));
                a.setStorageValue(UInt256.ZERO, UInt256.ONE);
                a.setStorageValue(UInt256.valueOf(63), UInt256.valueOf(2));
                a.setStorageValue(UInt256.valueOf(64), UInt256.valueOf(3));
                a.setStorageValue(UInt256.valueOf(256), UInt256.valueOf(4));
              });
      assertThat(b1.root).as("storage+code combined: non-zero root").isNotEqualTo(Hash.ZERO);

      assertThat(f.rollback(b1)).as("rollback storage+code combined -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1))
          .as("rollforward storage+code combined -> root1")
          .isEqualTo(b1.root);
    }
  }

  @Test
  void scenarioMultipleAccountsMixed_rollsBackToZero() {
    final Bytes delegationCode =
        Bytes.concatenate(Bytes.fromHexString("0xef0100"), DELEGATION_TARGET.getBytes());
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 =
          f.applyAndPersist(
              u -> {
                // EOA.
                u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                // Contract with code + storage across the header boundary.
                final MutableAccount contract = u.createAccount(ADDRESS_TWO, 1, Wei.of(1L));
                contract.setCode(Bytes.repeat((byte) 0x01, 200));
                contract.setStorageValue(UInt256.ZERO, UInt256.ONE);
                contract.setStorageValue(UInt256.valueOf(64), UInt256.valueOf(2));
                // Delegation account with storage.
                final MutableAccount delegated = u.createAccount(ADDRESS_THREE, 1, Wei.of(1L));
                delegated.setCode(delegationCode);
                delegated.setStorageValue(UInt256.valueOf(100), UInt256.valueOf(3));
              });
      assertThat(b1.root).as("mixed accounts: non-zero root").isNotEqualTo(Hash.ZERO);

      assertThat(f.rollback(b1)).as("rollback mixed accounts -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1)).as("rollforward mixed accounts -> root1").isEqualTo(b1.root);
    }
  }

  @Test
  void scenarioThreeBlockRollforwardThenRollbackToZero() {
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 = f.applyAndPersist(u -> u.createAccount(ADDRESS_ONE, 1, Wei.of(1L)));
      final PersistedBlock b2 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.getAccount(ADDRESS_ONE);
                a.setNonce(2);
                a.setBalance(Wei.of(10L));
                a.setStorageValue(UInt256.ONE, UInt256.ONE);
              });
      final PersistedBlock b3 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.getAccount(ADDRESS_ONE);
                a.setNonce(3);
                a.setBalance(Wei.of(100L));
                a.setStorageValue(UInt256.ONE, UInt256.valueOf(2));
                a.setStorageValue(UInt256.valueOf(64), UInt256.valueOf(9));
              });

      assertThat(f.rollback(b3)).as("rollback block 3 -> root2").isEqualTo(b2.root);
      assertThat(f.rollback(b2)).as("rollback block 2 -> root1").isEqualTo(b1.root);
      assertThat(f.rollback(b1)).as("rollback block 1 -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1)).as("rollforward block 1 -> root1").isEqualTo(b1.root);
      assertThat(f.rollforward(b2)).as("rollforward block 2 -> root2").isEqualTo(b2.root);
      assertThat(f.rollforward(b3)).as("rollforward block 3 -> root3").isEqualTo(b3.root);
    }
  }

  @Test
  void spamoorLikeManyNonceIncrements_rollForwardMatchesLiveRoot() {
    final int accountCount = 32;
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock parent =
          f.applyAndPersist(
              u -> {
                for (int i = 0; i < accountCount; i++) {
                  u.createAccount(
                      Address.fromHexString(String.format("0x%040x", i + 1)), i, Wei.of(i + 1));
                }
              });
      final PersistedBlock block =
          f.applyAndPersist(
              u -> {
                for (int i = 0; i < accountCount; i++) {
                  final Address address = Address.fromHexString(String.format("0x%040x", i + 1));
                  u.getAccount(address).setNonce(i + 1);
                }
              });
      assertThat(f.rollback(block)).isEqualTo(parent.root);
      assertThat(f.rollforward(block)).isEqualTo(block.root);
    }
  }

  @Test
  void scenarioAccountOnlyTrieLogRollForward_materializesSharedCodeZone() {
    // Devnet block-190 shape: trie log carries account + storage replay but code=0 after
    // deserialize because bytecode entries were unchanged reads at serialize time. FCU
    // rollForward must still materialize CODE_ZONE for new code-hash headers.
    final Bytes sharedCode = Bytes.repeat((byte) 0x02, 128);
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 =
          f.applyAndPersist(
              u -> {
                final MutableAccount b = u.createAccount(ADDRESS_TWO, 1, Wei.of(1L));
                b.setCode(sharedCode);
              });

      final PersistedBlock b2 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                a.setCode(sharedCode);
              });

      final TrieLogLayer accountOnlyLayer = new TrieLogLayer();
      accountOnlyLayer.setBlockHash(b2.layer.getBlockHash());
      b2.layer
          .getAccountChanges()
          .forEach(
              (address, change) ->
                  accountOnlyLayer.addAccountChange(
                      address, change.getPrior(), change.getUpdated()));
      accountOnlyLayer.freeze();
      assertThat(accountOnlyLayer.getCodeChanges()).isEmpty();

      assertThat(f.rollback(b2)).isEqualTo(b1.root);
      assertThat(f.rollforward(new PersistedBlock(accountOnlyLayer, b2.root)))
          .as("account-only trielog rollforward with shared bytecode")
          .isEqualTo(b2.root);
    }
  }

  // --------------------------------------------------------------------------------------------
  // Fixture: a fresh BINARY BonsaiWorldState backed by ExecutionContextTestFixture, reset to the
  // empty binary trie root (Hash.ZERO). Mirrors the setup of BinaryTrieVectorsTest.
  // --------------------------------------------------------------------------------------------

  private static final class BinaryFixture implements AutoCloseable {
    private final ExecutionContextTestFixture contextFixture;
    private final BonsaiWorldState worldState;
    private final DefaultBinaryStateRootCommitter committer = new DefaultBinaryStateRootCommitter();
    private BlockHeader previousHeader;
    private long blockNumber;

    BinaryFixture() {
      contextFixture =
          ExecutionContextTestFixture.builder(GenesisConfig.mainnet())
              .dataStorageFormat(DataStorageFormat.BINARY)
              .build();
      final BlockHeader chainHead = contextFixture.getBlockchain().getChainHeadHeader();
      try (BonsaiWorldState reset =
          (BonsaiWorldState)
              contextFixture
                  .getProtocolContext()
                  .getWorldStateArchive()
                  .getWorldState(
                      WorldStateQueryParams.newBuilder()
                          .withBlockHeader(chainHead)
                          .withShouldWorldStateUpdateHead(true)
                          .build())
                  .orElseThrow()) {
        BinaryTestSupport.initializeEmptyBinaryTrieRoot(reset);
      }
      final BlockHeader emptyHead =
          new BlockHeaderTestFixture()
              .parentHash(chainHead.getHash())
              .number(chainHead.getNumber())
              .stateRoot(Hash.ZERO)
              .buildHeader();
      contextFixture.getProtocolContext().getWorldStateArchive().resetArchiveStateTo(emptyHead);
      worldState =
          (BonsaiWorldState)
              contextFixture
                  .getProtocolContext()
                  .getWorldStateArchive()
                  .getWorldState(WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(emptyHead))
                  .orElseThrow();
      previousHeader = emptyHead;
      blockNumber = emptyHead.getNumber();
    }

    /** Apply {@code mutator} to a fresh updater, persist with a header, and capture the layer. */
    PersistedBlock applyAndPersist(final Consumer<WorldUpdater> mutator) {
      final BonsaiWorldStateUpdateAccumulator acc = worldState.updater();
      mutator.accept(acc);
      acc.commit();
      // Fire the copy BEFORE the computes so the clone captures the accumulator's pre-compute state
      // (including its introducedCodeHashes set) and the pre-compute does not pollute the
      // original accumulator passed to persist.
      final BonsaiWorldStateUpdateAccumulator clone = acc.copy();
      final Hash root = committer.compute(worldState, null, clone).root();
      blockNumber++;
      final BlockHeader header =
          new BlockHeaderTestFixture()
              .parentHash(previousHeader.getHash())
              .number(blockNumber)
              .stateRoot(root)
              .buildHeader();
      worldState.persist(header, committer);
      final TrieLogLayer layer = readLayer(header.getBlockHash());
      previousHeader = header;
      return new PersistedBlock(layer, root);
    }

    /** Roll back {@code block}; return the resulting root. */
    Hash rollback(final PersistedBlock block) {
      final BonsaiWorldStateUpdateAccumulator acc = worldState.updater();
      acc.rollBack(block.layer);
      acc.commit();
      worldState.persist(null, committer);
      return worldState.rootHash();
    }

    /** Roll forward {@code block}; return the resulting root. */
    Hash rollforward(final PersistedBlock block) {
      final BonsaiWorldStateUpdateAccumulator acc = worldState.updater();
      acc.rollForward(block.layer);
      acc.commit();
      worldState.persist(null, committer);
      return worldState.rootHash();
    }

    private TrieLogLayer readLayer(final Hash blockHash) {
      return worldState
          .getWorldStateStorage()
          .getTrieLog(blockHash)
          .map(
              bytes -> PbtTrieLogFactory.readFrom(new BytesValueRLPInput(Bytes.wrap(bytes), false)))
          .orElseThrow(() -> new IllegalStateException("Missing trie log for " + blockHash));
    }

    @Override
    public void close() {
      worldState.close();
    }
  }

  private static final class PersistedBlock {
    final TrieLogLayer layer;
    final Hash root;

    PersistedBlock(final TrieLogLayer layer, final Hash root) {
      this.layer = layer;
      this.root = root;
    }
  }
}
