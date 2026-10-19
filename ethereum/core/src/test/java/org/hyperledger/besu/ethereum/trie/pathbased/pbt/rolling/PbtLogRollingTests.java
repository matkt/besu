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
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.common.BinaryTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.BonsaiTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;

import java.util.Optional;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/** Rolls binary trie logs back and forward over several blocks and checks the binary roots. */
@ExtendWith(MockitoExtension.class)
class PbtLogRollingTests {

  private static final DataStorageConfiguration CONFIG =
      DataStorageConfiguration.DEFAULT_BONSAI_PBT_CONFIG;
  private static final Address ADDRESS_ONE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address ADDRESS_TWO =
      Address.fromHexString("0x2222222222222222222222222222222222222222");

  private BonsaiWorldStateProvider archive;
  private InMemoryKeyValueStorageProvider provider;
  private KeyValueStorage trieLogStorage;

  private BonsaiWorldStateProvider secondArchive;
  private InMemoryKeyValueStorageProvider secondProvider;

  private final Blockchain blockchain = mock(Blockchain.class);
  private final DefaultBinaryStateRootCommitter binaryCommitter =
      new DefaultBinaryStateRootCommitter();

  @BeforeEach
  void createStorage() {
    when(blockchain.getChainHeadHeader())
        .thenReturn(
            new BlockHeaderTestFixture().number(0).timestamp(0).stateRoot(Hash.ZERO).buildHeader());
    provider = new InMemoryKeyValueStorageProvider();
    archive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(
            blockchain,
            EvmConfiguration.DEFAULT,
            null,
            DataStorageFormat.BONSAI,
            Optional.of(0L),
            Optional.of(0L));
    trieLogStorage =
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE);

    secondProvider = new InMemoryKeyValueStorageProvider();
    secondArchive =
        InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(
            blockchain,
            EvmConfiguration.DEFAULT,
            null,
            DataStorageFormat.BONSAI,
            Optional.of(0L),
            Optional.of(0L));
  }

  private static final GenesisConfig EMPTY_BINARY_GENESIS =
      GenesisConfig.fromResource(
          "/org/hyperledger/besu/ethereum/trie/pathbased/pbt/empty-amsterdam-genesis.json");

  private BonsaiWorldState newWorldState(
      final BonsaiWorldStateProvider archiveProvider,
      final InMemoryKeyValueStorageProvider storageProvider) {
    final BlockHeader emptyGenesisHeader =
        new BlockHeaderTestFixture().number(0).timestamp(0).stateRoot(Hash.ZERO).buildHeader();
    final BonsaiWorldState worldState =
        new BonsaiWorldState(
            archiveProvider,
            new BonsaiWorldStateKeyValueStorage(storageProvider, new NoOpMetricsSystem(), CONFIG),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new BonsaiCodeCache(),
            emptyGenesisHeader);
    worldState.persist(emptyGenesisHeader, binaryCommitter);
    return worldState;
  }

  private Hash liveRoot(final BonsaiWorldState worldState) {
    return new DefaultBinaryStateRootCommitter()
        .compute(worldState, null, worldState.updater().copy())
        .root();
  }

  /** Rolling back a Patricia prior on the binary branch restores the 4-field flat account. */
  @Test
  void rollBackPatriciaPriorOnBinaryBranchRestoresFourFieldFlat() {
    final BonsaiWorldState worldState = newWorldState(archive, provider);
    final Hash storageRoot =
        Hash.fromHexString("0x1111111111111111111111111111111111111111111111111111111111111111");

    WorldUpdater updater = worldState.updater();
    updater.createAccount(ADDRESS_ONE, 2L, Wei.of(20L));
    updater.commit();
    final Hash tipRoot = liveRoot(worldState);
    final BlockHeader tipHeader =
        new BlockHeaderTestFixture()
            .parentHash(Hash.ZERO)
            .number(1)
            .stateRoot(tipRoot)
            .buildHeader();
    worldState.persist(tipHeader, binaryCommitter);

    assertThat(accountFieldCount(worldState, ADDRESS_ONE)).isEqualTo(3);

    final PatriciaTrieAccountValue prior =
        new PatriciaTrieAccountValue(1L, Wei.of(10L), storageRoot, Hash.EMPTY);
    final BinaryTrieAccountValue updated = new BinaryTrieAccountValue(2L, Wei.of(20L), Hash.EMPTY);
    final TrieLogLayer layer =
        new TrieLogLayer()
            .setBlockHash(tipHeader.getBlockHash())
            .setWireVersion(BonsaiTrieLogFactory.WIRE_VERSION_EXTENDED)
            .addAccountChange(ADDRESS_ONE, prior, updated);

    final BonsaiWorldStateUpdateAccumulator rollUpdater =
        (BonsaiWorldStateUpdateAccumulator) worldState.updater();
    rollUpdater.rollBack(layer);
    rollUpdater.commit();
    worldState.persist(null, binaryCommitter);

    final Bytes flat =
        worldState.getWorldStateStorage().getAccount(ADDRESS_ONE.addressHash()).orElseThrow();
    assertThat(accountFieldCount(flat)).isEqualTo(4);
    final PatriciaTrieAccountValue decoded = PatriciaTrieAccountValue.readFrom(RLP.input(flat));
    assertThat(decoded.getNonce()).isEqualTo(1L);
    assertThat(decoded.getBalance()).isEqualTo(Wei.of(10L));
    assertThat(decoded.getStorageRoot()).isEqualTo(storageRoot);
    assertThat(decoded.getCodeHash()).isEqualTo(Hash.EMPTY);
  }

  private static int accountFieldCount(final BonsaiWorldState worldState, final Address address) {
    return accountFieldCount(
        worldState.getWorldStateStorage().getAccount(address.addressHash()).orElseThrow());
  }

  private static int accountFieldCount(final Bytes accountRlp) {
    final RLPInput in = RLP.input(accountRlp);
    final int count = in.enterList();
    in.leaveListLenient();
    return count;
  }

  @Test
  void rollForwardTwiceThenRollBack_matchesLiveBinaryRoot() {
    // build two blocks on the first world state
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

    // replay them on a second world state from the trie logs
    final BonsaiWorldState secondWorldState = newWorldState(secondArchive, secondProvider);
    final BonsaiWorldStateUpdateAccumulator secondUpdater =
        (BonsaiWorldStateUpdateAccumulator) secondWorldState.updater();

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

    secondUpdater.rollBack(layerTwo);
    secondUpdater.commit();
    secondWorldState.persist(null, binaryCommitter);
    assertThat(secondWorldState.rootHash()).isEqualTo(rootOne);
  }

  private static TrieLogLayer readPbtTrieLog(final KeyValueStorage storage, final Hash key) {
    return storage
        .get(key.getBytes().toArrayUnsafe())
        .map(
            bytes ->
                BonsaiTrieLogFactory.readFrom(new BytesValueRLPInput(Bytes.wrap(bytes), false)))
        .orElseThrow(() -> new IllegalStateException("Missing trie log for " + key));
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
  void scenarioSharedCodeTwoAccounts_rollsBackToZeroAndRespectsSharing() {
    // 3999 bytes of 0x01 make 129 chunks
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

      assertThat(f.rollback(b1)).as("rollback shared code -> ZERO").isEqualTo(Hash.ZERO);
      assertThat(f.rollforward(b1)).as("rollforward shared code -> root1").isEqualTo(b1.root);

      // delete one of two accounts sharing the code, then roll the deletion back
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
    // B has the code before the block that creates A with it, and that block does not touch B
    final Bytes sharedCode = Bytes.repeat((byte) 0x01, 3999);
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock b1 =
          f.applyAndPersist(
              u -> {
                final MutableAccount b = u.createAccount(ADDRESS_TWO, 1, Wei.of(1L));
                b.setCode(sharedCode);
              });
      assertThat(b1.root).as("B only: non-zero root").isNotEqualTo(Hash.ZERO);

      final PersistedBlock b2 =
          f.applyAndPersist(
              u -> {
                final MutableAccount a = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                a.setCode(sharedCode);
              });
      assertThat(b2.root).as("A and B: non-zero root").isNotEqualTo(Hash.ZERO);

      assertThat(f.rollback(b2))
          .as("rollback A's creation -> root1 (B's chunks kept)")
          .isEqualTo(b1.root);

      assertThat(f.rollforward(b2))
          .as("rollforward A's creation -> root2 (A and B)")
          .isEqualTo(b2.root);
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
  void manyNonceIncrements_rollForwardMatchesLiveRoot() {
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
    // a trie log without code changes still writes the code chunks of a new code hash
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

  @Test
  void rollForward_materializesCodeZoneWhenCodeAlreadyExistsInFlatDb() {
    final Bytes code = Bytes.repeat((byte) 0x03, 128);
    try (final BinaryFixture f = new BinaryFixture()) {
      final PersistedBlock block =
          f.applyAndPersist(
              u -> {
                final MutableAccount account = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                account.setCode(code);
              });

      f.rollback(block);
      f.putFlatCode(ADDRESS_ONE, code);

      assertThat(f.rollforward(block))
          .as("trie roll after flat DB phase must restore CODE_ZONE")
          .isEqualTo(block.root);
    }
  }

  @Test
  void reorg_keepsCodeZoneWhenSameCodeMovesBetweenAccounts() {
    final Bytes code = Bytes.repeat((byte) 0x04, 128);
    try (final BinaryFixture oldFork = new BinaryFixture();
        final BinaryFixture newFork = new BinaryFixture()) {
      final PersistedBlock oldBlock =
          oldFork.applyAndPersist(
              u -> {
                final MutableAccount account = u.createAccount(ADDRESS_ONE, 1, Wei.of(1L));
                account.setCode(code);
              });
      final PersistedBlock newBlock =
          newFork.applyAndPersist(
              u -> {
                final MutableAccount account = u.createAccount(ADDRESS_TWO, 1, Wei.of(1L));
                account.setCode(code);
              });

      assertThat(oldFork.reorg(oldBlock, newBlock))
          .as("reorg must not delete code chunks still referenced on the new fork")
          .isEqualTo(newBlock.root);
    }
  }

  /** A head binary trie world state on an empty genesis. */
  private static final class BinaryFixture implements AutoCloseable {
    private final ExecutionContextTestFixture contextFixture;
    private final BonsaiWorldState worldState;
    private final DefaultBinaryStateRootCommitter committer = new DefaultBinaryStateRootCommitter();
    private BlockHeader previousHeader;
    private long blockNumber;

    BinaryFixture() {
      contextFixture =
          ExecutionContextTestFixture.builder(EMPTY_BINARY_GENESIS)
              .dataStorageFormat(DataStorageFormat.BONSAI)
              .build();
      final BlockHeader chainHead = contextFixture.getBlockchain().getChainHeadHeader();
      worldState =
          (BonsaiWorldState)
              contextFixture
                  .getProtocolContext()
                  .getWorldStateArchive()
                  .getWorldState(WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(chainHead))
                  .orElseThrow();
      previousHeader = chainHead;
      blockNumber = chainHead.getNumber();
    }

    /** Apply {@code mutator} to a fresh updater, persist with a header, and capture the layer. */
    PersistedBlock applyAndPersist(final Consumer<WorldUpdater> mutator) {
      final BonsaiWorldStateUpdateAccumulator acc =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();
      mutator.accept(acc);
      acc.commit();
      // compute on a copy so the accumulator given to persist is untouched
      final BonsaiWorldStateUpdateAccumulator clone =
          (BonsaiWorldStateUpdateAccumulator) acc.copy();
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
      final BonsaiWorldStateUpdateAccumulator acc =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();
      acc.rollBack(block.layer);
      acc.commit();
      worldState.persist(null, committer);
      return worldState.rootHash();
    }

    /** Roll forward {@code block}; return the resulting root. */
    Hash rollforward(final PersistedBlock block) {
      final BonsaiWorldStateUpdateAccumulator acc =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();
      acc.rollForward(block.layer);
      acc.commit();
      worldState.persist(null, committer);
      return worldState.rootHash();
    }

    Hash reorg(final PersistedBlock oldBlock, final PersistedBlock newBlock) {
      final BonsaiWorldStateUpdateAccumulator acc =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();
      acc.rollBack(oldBlock.layer);
      acc.rollForward(newBlock.layer);
      acc.commit();
      worldState.persist(null, committer);
      return worldState.rootHash();
    }

    void putFlatCode(final Address address, final Bytes code) {
      worldState
          .getWorldStateStorage()
          .updater()
          .putCode(address.addressHash(), Hash.hash(code), code)
          .commitComposedOnly();
    }

    private TrieLogLayer readLayer(final Hash blockHash) {
      return worldState
          .getWorldStateStorage()
          .getTrieLog(blockHash)
          .map(
              bytes ->
                  BonsaiTrieLogFactory.readFrom(new BytesValueRLPInput(Bytes.wrap(bytes), false)))
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
