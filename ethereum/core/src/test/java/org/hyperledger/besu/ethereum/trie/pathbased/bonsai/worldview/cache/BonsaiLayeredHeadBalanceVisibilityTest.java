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
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

/**
 * Regression: after payload → promote → next payload, account balances must remain visible. A prior
 * bug parented payload layers on cache RocksDB snapshots that were later closed, so reads returned
 * empty accounts (balance 0x0) and newPayload falsely rejected valid blocks.
 */
class BonsaiLayeredHeadBalanceVisibilityTest {

  private static final Address FUNDED =
      Address.fromHexString("0x00000000000000000000000000000000000000f1");

  @Test
  void balancesRemainVisibleAcrossPayloadPromoteAndNextPayload() {
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
    final InMemoryKeyValueStorageProvider provider = new InMemoryKeyValueStorageProvider();
    final BonsaiWorldStateKeyValueStorage kvStorage =
        new BonsaiWorldStateKeyValueStorage(provider, new NoOpMetricsSystem(), dataConfig);

    final BonsaiWorldStateProvider archive =
        new BonsaiWorldStateProvider(
            kvStorage,
            blockchain,
            pathConfig,
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            new PathBasedCodeCache());
    genesisState.writeStateTo(archive.getWorldState());
    final BlockHeader genesis = genesisState.getBlock().getHeader();
    archive.getWorldState().persist(genesis);

    // Fund an account at genesis head (durable).
    final Wei fundedBalance = Wei.of(1_000_000L);
    final BlockHeader block1 = persistFundedAccount(archive, genesis, FUNDED, fundedBalance, 1L);
    blockchain.appendBlock(new Block(block1, BlockBody.empty()), List.of(), Optional.empty());
    assertThat(archive.promoteCachedWorldState(block1)).isTrue();

    // Next payload must still see the funded balance through the live layered head.
    try (BonsaiWorldState payloadParent =
        (BonsaiWorldState)
            archive
                .getWorldState(WorldStateQueryParams.withBlockHeaderAndPayloadLayer(block1))
                .orElseThrow()) {
      final Account account = payloadParent.get(FUNDED);
      assertThat(account).isNotNull();
      assertThat(account.getBalance()).isEqualTo(fundedBalance);
    }

    // And after a second payload + promote, balances remain visible for the following parent.
    final BlockHeader block2 =
        persistBalanceDeltaOnPayloadLayer(archive, block1, FUNDED, Wei.of(1L), 2L);
    blockchain.appendBlock(new Block(block2, BlockBody.empty()), List.of(), Optional.empty());
    assertThat(archive.promoteCachedWorldState(block2)).isTrue();

    try (BonsaiWorldState payloadParent =
        (BonsaiWorldState)
            archive
                .getWorldState(WorldStateQueryParams.withBlockHeaderAndPayloadLayer(block2))
                .orElseThrow()) {
      final Account account = payloadParent.get(FUNDED);
      assertThat(account).isNotNull();
      assertThat(account.getBalance()).isEqualTo(fundedBalance.add(Wei.of(1L)));
    }
  }

  private static BlockHeader persistFundedAccount(
      final BonsaiWorldStateProvider archive,
      final BlockHeader parent,
      final Address address,
      final Wei balance,
      final long nonce) {
    try (BonsaiWorldState worldState =
        (BonsaiWorldState)
            archive
                .getWorldState(WorldStateQueryParams.withBlockHeaderAndPayloadLayer(parent))
                .orElseThrow()) {
      final MutableAccount account = worldState.updater().getOrCreate(address);
      account.setBalance(balance);
      account.setNonce(nonce);
      worldState.updater().commit();
      return persistComputedHeader(worldState, parent);
    }
  }

  private static BlockHeader persistBalanceDeltaOnPayloadLayer(
      final BonsaiWorldStateProvider archive,
      final BlockHeader parent,
      final Address address,
      final Wei delta,
      final long nonce) {
    try (BonsaiWorldState worldState =
        (BonsaiWorldState)
            archive
                .getWorldState(WorldStateQueryParams.withBlockHeaderAndPayloadLayer(parent))
                .orElseThrow()) {
      final MutableAccount account = worldState.updater().getOrCreate(address);
      account.setBalance(account.getBalance().add(delta));
      account.setNonce(nonce);
      worldState.updater().commit();
      return persistComputedHeader(worldState, parent);
    }
  }

  private static BlockHeader persistComputedHeader(
      final BonsaiWorldState worldState, final BlockHeader parent) {
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
