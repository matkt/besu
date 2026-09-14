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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_CHECKPOINT_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_CHECKPOINT_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolScheduleBuilder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecAdapters;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.DefaultStateRootCommitter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/**
 * Simulates crash recovery: RocksDB durable state stays at the checkpoint while the blockchain and
 * trie logs have advanced. Reconstructing the provider must replay trie logs without throwing.
 */
class BonsaiLayeredHeadRecoveryTest {

  private static final Address EOA =
      Address.fromHexString("0x00000000000000000000000000000000000000e0");

  @Test
  void reconstructingProviderReplaysTrieLogsWithoutThrowing() throws Exception {
    final InMemoryKeyValueStorageProvider provider = new InMemoryKeyValueStorageProvider();
    final ImmutablePathBasedExtraStorageConfiguration pathConfig =
        ImmutablePathBasedExtraStorageConfiguration.builder()
            .unstable(
                ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                    .bonsaiLayeredHeadEnabled(true)
                    .bonsaiLayeredHeadCheckpointInterval(32)
                    .build())
            .build();
    final var dataConfig =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .pathBasedExtraStorageConfiguration(pathConfig)
            .build();

    final var protocolSchedule =
        new ProtocolScheduleBuilder(
                GenesisConfig.mainnet().getConfigOptions(),
                Optional.of(BigInteger.valueOf(42)),
                ProtocolSpecAdapters.create(0, Function.identity()),
                false,
                EvmConfiguration.DEFAULT,
                MiningConfiguration.MINING_DISABLED,
                new BadBlockManager(),
                false,
                ImmutableBalConfiguration.builder().build(),
                new NoOpMetricsSystem())
            .createProtocolSchedule();
    final GenesisState genesisState =
        GenesisState.fromConfig(
            GenesisConfig.mainnet(), protocolSchedule, new PathBasedCodeCache());
    final MutableBlockchain blockchain =
        InMemoryKeyValueStorageProvider.createInMemoryBlockchain(genesisState.getBlock());

    final BonsaiWorldStateKeyValueStorage kvStorage =
        new BonsaiWorldStateKeyValueStorage(provider, new NoOpMetricsSystem(), dataConfig);

    BonsaiWorldStateProvider archive =
        new BonsaiWorldStateProvider(
            kvStorage,
            blockchain,
            pathConfig,
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            new PathBasedCodeCache());
    genesisState.writeStateTo(archive.getWorldState());
    final BlockHeader genesisHeader = genesisState.getBlock().getHeader();
    archive.getWorldState().persist(genesisHeader);
    writeCheckpointMetadata(kvStorage, genesisHeader);

    // Payload-layer persist: durable RocksDB stays at genesis; trie log is written for block1.
    final BlockHeader block1 =
        persistBalanceChangeOnPayloadLayer(archive, genesisHeader, Wei.of(1_000), 1L);
    blockchain.appendBlock(new Block(block1, BlockBody.empty()), List.of(), Optional.empty());
    archive.close();

    archive =
        new BonsaiWorldStateProvider(
            kvStorage,
            blockchain,
            pathConfig,
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            new PathBasedCodeCache());

    assertThat(archive.getWorldState().rootHash()).isEqualTo(block1.getStateRoot());
    archive.close();
    kvStorage.close();
  }

  private static void writeCheckpointMetadata(
      final BonsaiWorldStateKeyValueStorage kvStorage, final BlockHeader header) {
    final var updater = kvStorage.updater();
    updater
        .getWorldStateTransaction()
        .put(
            TRIE_BRANCH_STORAGE,
            WORLD_CHECKPOINT_NUMBER_KEY,
            Bytes.ofUnsignedLong(header.getNumber()).toArrayUnsafe());
    updater
        .getWorldStateTransaction()
        .put(
            TRIE_BRANCH_STORAGE,
            WORLD_CHECKPOINT_HASH_KEY,
            header.getBlockHash().getBytes().toArrayUnsafe());
    updater
        .getWorldStateTransaction()
        .put(
            TRIE_BRANCH_STORAGE,
            WORLD_BLOCK_NUMBER_KEY,
            Bytes.ofUnsignedLong(header.getNumber()).toArrayUnsafe());
    updater
        .getWorldStateTransaction()
        .put(
            TRIE_BRANCH_STORAGE,
            WORLD_BLOCK_HASH_KEY,
            header.getBlockHash().getBytes().toArrayUnsafe());
    updater
        .getWorldStateTransaction()
        .put(
            TRIE_BRANCH_STORAGE,
            WORLD_ROOT_HASH_KEY,
            header.getStateRoot().getBytes().toArrayUnsafe());
    updater.commitComposedOnly();
  }

  private static BlockHeader persistBalanceChangeOnPayloadLayer(
      final BonsaiWorldStateProvider archive,
      final BlockHeader parent,
      final Wei balance,
      final long nonce) {
    try (BonsaiWorldState worldState =
        (BonsaiWorldState)
            archive
                .getWorldState(WorldStateQueryParams.withBlockHeaderAndPayloadLayer(parent))
                .orElseThrow()) {
      final MutableAccount account = worldState.updater().getOrCreate(EOA);
      account.setBalance(balance);
      account.setNonce(nonce);
      worldState.updater().commit();
      final BonsaiWorldStateUpdateAccumulator accumulator =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();
      final Hash root =
          new DefaultStateRootCommitter().compute(worldState, null, accumulator.copy()).root();
      final BlockHeader blockHeader =
          new BlockHeaderTestFixture()
              .parentHash(parent.getHash())
              .number(parent.getNumber() + 1L)
              .stateRoot(root)
              .buildHeader();
      worldState.persist(blockHeader, new DefaultStateRootCommitter());
      return blockHeader;
    }
  }
}
