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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.BinaryStateRootCommitter;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BinaryTestSupport;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.PbtTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Binary-trie mirror of {@code LogRollingTests}. Exercises trie-log roll-forward / roll-back
 * against a {@link DataStorageFormat#BINARY} world state and asserts the binary state root,
 * recomputed via {@link BinaryStateRootCommitter}, matches the live-execution root after each roll.
 *
 * <p>This only works because the {@link PbtTrieLogFactory} carries the storage slot key preimage;
 * the legacy MPT factory serializes only the slot hash, which would leave {@code
 * StorageSlotKey.getSlotKey()} empty and make {@code BinaryStateRootCommitter.applyStorage} throw
 * after a roll.
 */
@ExtendWith(MockitoExtension.class)
class PbtLogRollingTests {

  private static final DataStorageConfiguration CONFIG =
      DataStorageConfiguration.DEFAULT_BINARY_CONFIG;
  private static final Address ADDRESS_ONE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");

  private BonsaiWorldStateProvider archive;
  private InMemoryKeyValueStorageProvider provider;
  private KeyValueStorage trieLogStorage;

  private BonsaiWorldStateProvider secondArchive;
  private InMemoryKeyValueStorageProvider secondProvider;

  private final Blockchain blockchain = mock(Blockchain.class);
  private final BinaryStateRootCommitter binaryCommitter = new BinaryStateRootCommitter();

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
    return new BinaryStateRootCommitter()
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
}
