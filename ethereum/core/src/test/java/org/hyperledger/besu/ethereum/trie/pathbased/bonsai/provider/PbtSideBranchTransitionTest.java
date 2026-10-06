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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.WorldStateHealerHelper.throwingWorldStateHealerSupplier;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary.DefaultBinaryStateRootCommitter;
import org.hyperledger.besu.ethereum.trie.common.BinaryTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.MigrationScopedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.TrieBranchSegments;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.StateRootComputation;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/**
 * Two branches that each cross the binary-trie fork on their own block, as every island does in a
 * reorg across {@code binaryTrieTime}: the head branch H1 -> H2 and a side branch S1 -> S2 imported
 * by newPayload only. Parent world states are resolved the way {@code MainnetBlockValidator} does
 * for newPayload (no head update) and the way forkchoiceUpdated moves the head.
 */
class PbtSideBranchTransitionTest {

  private static final long PBT_TIME = 100L;
  private static final DataStorageConfiguration CONFIG =
      DataStorageConfiguration.DEFAULT_BONSAI_PBT_CONFIG;
  private static final Address CONTRACT = Address.fromHexString("0xc0");
  private static final Address OTHER = Address.fromHexString("0x0e");
  private static final UInt256 SLOT = UInt256.ONE;

  private final Map<Hash, BlockHeader> headers = new HashMap<>();
  private final Blockchain blockchain = mock(Blockchain.class);
  private BlockHeader chainHead;
  private BonsaiWorldStateKeyValueStorage storage;
  private BonsaiWorldStateProvider provider;

  @Test
  void childOfSideBranchForkBlockFindsItsParentWorldState() {
    final BlockHeader s2 = straddle();

    // newPayload(S3): S2's world state, resolved without moving the head off H2.
    final Optional<BonsaiWorldState> s2State =
        provider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(s2, PBT_TIME + 2))
            .map(BonsaiWorldState.class::cast);

    assertThat(s2State).isPresent();
    assertThat(s2State.get().rootHash()).isEqualTo(s2.getStateRoot());
  }

  @Test
  void forkchoiceUpdatedMovesTheHeadToTheOtherBranch() {
    final BlockHeader s2 = straddle();

    final BonsaiWorldState s2Head =
        (BonsaiWorldState)
            provider
                .getWorldState(WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(s2))
                .orElseThrow();
    assertThat(s2Head.rootHash()).isEqualTo(s2.getStateRoot());
    chainHead = s2;

    final BlockHeader s3 =
        block(
            head(s2, PBT_TIME + 3),
            s2,
            PBT_TIME + 3,
            u -> u.getAccount(OTHER).setBalance(Wei.of(4)));
    assertThat(s3.getStateRoot()).isEqualTo(binaryRootFromScratch(2, 4));
  }

  @Test
  void forkParentOnASingleChainResolvesToTheMigratedBinaryState() {
    final BlockHeader genesis = genesis();
    final BlockHeader h1 =
        block(head(genesis, 10), genesis, 10, u -> u.getAccount(OTHER).setBalance(Wei.of(5)));
    chainHead = h1;
    advanceMigratorColumn(h1);

    // newPayload(H2): H1's world state for the first binary block, without a head update.
    final Optional<BonsaiWorldState> h1State =
        provider
            .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(h1, PBT_TIME))
            .map(BonsaiWorldState.class::cast);

    assertThat(h1State).isPresent();
    assertThat(h1State.get().rootHash().getBytes())
        .isEqualTo(storage.getWorldStateRootHash(TrieBranchType.BINARY).orElseThrow());
  }

  /** Builds both branches and leaves H2 as the head; returns S2. */
  private BlockHeader straddle() {
    final BlockHeader genesis = genesis();

    final BlockHeader h1 =
        block(head(genesis, 10), genesis, 10, u -> u.getAccount(OTHER).setBalance(Wei.of(5)));
    chainHead = h1;
    advanceMigratorColumn(h1);
    final BlockHeader h2 =
        block(
            head(h1, PBT_TIME),
            h1,
            PBT_TIME,
            u -> u.getAccount(CONTRACT).setStorageValue(SLOT, UInt256.valueOf(2)));
    chainHead = h2;

    final BlockHeader s1 =
        block(sideParent(genesis, 11), genesis, 11, u -> u.getAccount(OTHER).setBalance(Wei.of(3)));
    final BlockHeader s2 =
        block(
            sideParent(s1, PBT_TIME + 1),
            s1,
            PBT_TIME + 1,
            u -> u.getAccount(CONTRACT).setStorageValue(SLOT, UInt256.valueOf(2)));
    chainHead = h2;
    return s2;
  }

  /** Merkle genesis (CONTRACT.slot = 1, OTHER), with the migrator's binary column seeded. */
  private BlockHeader genesis() {
    chainHead = new BlockHeaderTestFixture().number(0).timestamp(0).buildHeader();
    when(blockchain.getChainHeadHeader()).thenAnswer(i -> chainHead);
    when(blockchain.getBlockHeader(any(Hash.class)))
        .thenAnswer(i -> Optional.ofNullable(headers.get(i.<Hash>getArgument(0))));
    storage =
        (BonsaiWorldStateKeyValueStorage)
            new InMemoryKeyValueStorageProvider().createWorldStateStorage(CONFIG);
    provider = newProvider(storage, blockchain, PBT_TIME);

    final BlockHeader genesis =
        block(
            (BonsaiWorldState) provider.getWorldState(),
            null,
            0,
            u -> {
              u.createAccount(CONTRACT, 0, Wei.ONE).setStorageValue(SLOT, UInt256.ONE);
              u.createAccount(OTHER, 0, Wei.ONE);
            });
    chainHead = genesis;

    final TrieLogLayer genesisLog =
        new TrieLogLayer().setBlockHash(genesis.getBlockHash()).setBlockNumber(0);
    genesisLog.addAccountChange(CONTRACT, null, new BinaryTrieAccountValue(0, Wei.ONE, Hash.EMPTY));
    genesisLog.addAccountChange(OTHER, null, new BinaryTrieAccountValue(0, Wei.ONE, Hash.EMPTY));
    genesisLog.addStorageChange(CONTRACT, new StorageSlotKey(SLOT), null, UInt256.ONE);
    migrate(genesisLog, genesis);
    return genesis;
  }

  /** What PbtMigrator does for one pre-fork block: rolls its trie log onto the binary column. */
  private void advanceMigratorColumn(final BlockHeader block) {
    final TrieLog trieLog =
        provider.getTrieLogManager().getTrieLogLayer(block.getBlockHash()).orElseThrow();
    migrate(trieLog, block);
  }

  private void migrate(final TrieLog trieLog, final BlockHeader block) {
    final BonsaiWorldState migration = provider.getMigrationWorldState();
    migration.updater().rollForward(trieLog);
    final StateRootComputation computation =
        new DefaultBinaryStateRootCommitter().compute(migration, block, migration.updater());
    final BonsaiWorldStateKeyValueStorage.Updater updater =
        new MigrationScopedWorldStateKeyValueStorage(storage).updater();
    computation.applyTo(updater);
    updater
        .getWorldStateTransaction()
        .put(
            TrieBranchSegments.segmentFor(TrieBranchType.BINARY),
            WORLD_ROOT_HASH_KEY,
            computation.root().getBytes().toArrayUnsafe());
    updater
        .getWorldStateTransaction()
        .put(
            TrieBranchSegments.segmentFor(TrieBranchType.BINARY),
            WORLD_BLOCK_HASH_KEY,
            block.getBlockHash().getBytes().toArrayUnsafe());
    updater.commit();
    migration.updater().reset();
  }

  /** Parent state for a block extending the head. */
  private BonsaiWorldState head(final BlockHeader parent, final long timestamp) {
    return (BonsaiWorldState)
        provider
            .getWorldState(
                WorldStateQueryParams.newBuilder()
                    .withParentBlockHeader(parent)
                    .withShouldWorldStateUpdateHead(true)
                    .withTimeStamp(timestamp)
                    .build())
            .orElseThrow();
  }

  /** Parent state for a block imported by newPayload without a head update. */
  private BonsaiWorldState sideParent(final BlockHeader parent, final long timestamp) {
    return (BonsaiWorldState)
        provider
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(parent, timestamp))
            .orElseThrow();
  }

  /** Applies {@code changes}, seals a header carrying the resulting root and persists it. */
  private BlockHeader block(
      final BonsaiWorldState worldState,
      final BlockHeader parent,
      final long timestamp,
      final Consumer<WorldUpdater> changes) {
    final WorldUpdater updater = worldState.updater();
    changes.accept(updater);
    updater.commit();
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .number(parent == null ? 0 : parent.getNumber() + 1)
            .parentHash(parent == null ? Hash.ZERO : parent.getHash())
            .timestamp(timestamp)
            .stateRoot(worldState.frontierRootHash())
            .buildHeader();
    headers.put(header.getHash(), header);
    worldState.persist(header);
    return header;
  }

  /** The binary root of CONTRACT.slot / OTHER.balance built directly, binary from genesis. */
  private static Hash binaryRootFromScratch(final long contractSlot, final long otherBalance) {
    final Blockchain chain = mock(Blockchain.class);
    final BlockHeader genesis = new BlockHeaderTestFixture().number(0).timestamp(0).buildHeader();
    when(chain.getChainHeadHeader()).thenReturn(genesis);
    final BonsaiWorldState state =
        (BonsaiWorldState)
            newProvider(
                    (BonsaiWorldStateKeyValueStorage)
                        new InMemoryKeyValueStorageProvider().createWorldStateStorage(CONFIG),
                    chain,
                    0L)
                .getWorldState();
    final WorldUpdater updater = state.updater();
    updater
        .createAccount(CONTRACT, 0, Wei.ONE)
        .setStorageValue(SLOT, UInt256.valueOf(contractSlot));
    updater.createAccount(OTHER, 0, Wei.ONE).setBalance(Wei.of(otherBalance));
    updater.commit();
    return state.frontierRootHash();
  }

  private static BonsaiWorldStateProvider newProvider(
      final BonsaiWorldStateKeyValueStorage storage,
      final Blockchain blockchain,
      final long pbtTime) {
    return new BonsaiWorldStateProvider(
        storage,
        blockchain,
        CONFIG.getPathBasedExtraStorageConfiguration(),
        new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
        null,
        EvmConfiguration.DEFAULT,
        throwingWorldStateHealerSupplier(),
        new BonsaiCodeCache(),
        Optional.empty(),
        Optional.of(pbtTime));
  }
}
