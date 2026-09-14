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
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.GenesisState;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
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
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;
import java.util.Optional;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

/**
 * Lightweight checklist for layered-head benchmarking: A/B runs should compare identical state roots
 * for the same block transition when layered head is enabled vs disabled.
 *
 * <p>Checklist:
 *
 * <ul>
 *   <li>Same genesis + block change payload
 *   <li>Standard Bonsai persist path (HEAD) vs payload-layer path (PAYLOAD_LAYER)
 *   <li>Criterion: {@code root(layered) == root(standard)}
 * </ul>
 */
class BonsaiLayeredHeadBenchmarkNotesTest {

  private static final Address EOA =
      Address.fromHexString("0x00000000000000000000000000000000000000e0");

  @Test
  void benchmarkCriterion_identicalRootsForOneBlockTransition() throws Exception {
    final Hash standardRoot = persistStandardHead(Wei.of(42_000), 3L);
    final Hash layeredRoot = persistLayeredPayload(Wei.of(42_000), 3L);
    assertThat(layeredRoot).isEqualTo(standardRoot);
  }

  private static Hash persistStandardHead(final Wei balance, final long nonce) throws Exception {
    return persistWithLayeredHeadEnabled(false, balance, nonce);
  }

  private static Hash persistLayeredPayload(final Wei balance, final long nonce) throws Exception {
    return persistWithLayeredHeadEnabled(true, balance, nonce);
  }

  private static Hash persistWithLayeredHeadEnabled(
      final boolean layeredHeadEnabled, final Wei balance, final long nonce) throws Exception {
    try (RootHarness harness = RootHarness.create(layeredHeadEnabled)) {
      return harness.applyBalanceAndNonceAndPersist(balance, nonce);
    }
  }

  private static final class RootHarness implements AutoCloseable {
    private final BonsaiWorldStateProvider archive;
    private final MutableBlockchain blockchain;
    private final boolean layeredHeadEnabled;

    private RootHarness(
        final BonsaiWorldStateProvider archive,
        final MutableBlockchain blockchain,
        final boolean layeredHeadEnabled) {
      this.archive = archive;
      this.blockchain = blockchain;
      this.layeredHeadEnabled = layeredHeadEnabled;
    }

    static RootHarness create(final boolean layeredHeadEnabled) {
      final ImmutablePathBasedExtraStorageConfiguration pathConfig =
          ImmutablePathBasedExtraStorageConfiguration.builder()
              .unstable(
                  ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                      .bonsaiLayeredHeadEnabled(layeredHeadEnabled)
                      .bonsaiLayeredHeadCheckpointInterval(32)
                      .build())
              .build();
      final DataStorageConfiguration dataConfig =
          ImmutableDataStorageConfiguration.builder()
              .dataStorageFormat(DataStorageFormat.BONSAI)
              .pathBasedExtraStorageConfiguration(pathConfig)
              .build();

      final InMemoryKeyValueStorageProvider provider = new InMemoryKeyValueStorageProvider();
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
      archive.getWorldState().persist(genesisState.getBlock().getHeader());
      return new RootHarness(archive, blockchain, layeredHeadEnabled);
    }

    Hash applyBalanceAndNonceAndPersist(final Wei balance, final long nonce) {
      final BlockHeader parent = blockchain.getChainHeadHeader();
      BonsaiWorldState worldState;
      if (layeredHeadEnabled) {
        worldState =
            (BonsaiWorldState)
                archive
                    .getWorldState(WorldStateQueryParams.withBlockHeaderAndPayloadLayer(parent))
                    .orElseThrow();
      } else {
        worldState = (BonsaiWorldState) archive.getWorldState();
      }

      try (worldState) {
        final WorldUpdater updater = worldState.updater();
        final MutableAccount account = updater.getOrCreate(EOA);
        account.setBalance(balance);
        account.setNonce(nonce);
        updater.commit();
        final BonsaiWorldStateUpdateAccumulator accumulator =
            (BonsaiWorldStateUpdateAccumulator) worldState.updater();
        final Hash root =
            new DefaultStateRootCommitter()
                .compute(worldState, null, accumulator.copy())
                .root();
        final BlockHeader blockHeader =
            new BlockHeaderTestFixture()
                .parentHash(parent.getHash())
                .number(parent.getNumber() + 1L)
                .stateRoot(root)
                .buildHeader();
        worldState.persist(blockHeader, new DefaultStateRootCommitter());
        return root;
      }
    }

    @Override
    public void close() throws Exception {
      archive.close();
    }
  }
}
