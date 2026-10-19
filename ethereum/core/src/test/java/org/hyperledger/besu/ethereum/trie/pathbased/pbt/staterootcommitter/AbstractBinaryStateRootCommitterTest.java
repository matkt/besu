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
package org.hyperledger.besu.ethereum.trie.pathbased.pbt.staterootcommitter;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.StateRootCommitterFactory;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary.DefaultBinaryStateRootCommitter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/** Binary trie fixture and helpers shared by the binary state root committer tests. */
abstract class AbstractBinaryStateRootCommitterTest {

  private static final GenesisConfig EMPTY_BINARY_GENESIS =
      GenesisConfig.fromResource(
          "/org/hyperledger/besu/ethereum/trie/pathbased/pbt/empty-amsterdam-genesis.json");

  private ExecutionContextTestFixture contextTestFixture;
  protected ProtocolContext protocolContext;
  protected BlockHeader chainHeadHeader;
  protected StateRootCommitterFactory factory;

  @BeforeEach
  void setUp() {
    contextTestFixture =
        ExecutionContextTestFixture.builder(EMPTY_BINARY_GENESIS)
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    protocolContext = contextTestFixture.getProtocolContext();
    chainHeadHeader = contextTestFixture.getBlockchain().getChainHeadHeader();
    factory = new StateRootCommitterFactory(ImmutableBalConfiguration.builder().build());
  }

  @AfterEach
  void tearDown() throws Exception {
    contextTestFixture.getStateArchive().close();
  }

  protected BlockHeader childHeader(final Hash stateRoot) {
    return new BlockHeaderTestFixture()
        .parentHash(chainHeadHeader.getHash())
        .number(chainHeadHeader.getNumber() + 1L)
        .stateRoot(stateRoot)
        .buildHeader();
  }

  protected static Address testAddress(final String suffix) {
    return Address.fromHexString("0x00000000000000000000000000000000000000" + suffix);
  }

  protected static BlockAccessList balanceAndNonceBal(
      final Address address, final Wei balance, final long nonce) {
    return new BlockAccessList(
        List.of(
            new AccountChanges(
                address,
                List.of(),
                List.of(),
                List.of(new BalanceChange(0, balance)),
                List.of(new NonceChange(0, nonce)),
                List.of())));
  }

  protected static void applyBalanceAndNonce(
      final BonsaiWorldState worldState,
      final Address address,
      final Wei balance,
      final long nonce) {
    final WorldUpdater updater = worldState.updater();
    final MutableAccount account = updater.getOrCreate(address);
    account.setBalance(balance);
    if (nonce > 0) {
      account.setNonce(nonce);
    }
    updater.commit();
  }

  protected BonsaiWorldState getWorldState(final boolean shouldUpdateHead) {
    return (BonsaiWorldState)
        protocolContext
            .getWorldStateArchive()
            .getWorldState(
                WorldStateQueryParams.newBuilder()
                    .withBlockHeader(chainHeadHeader)
                    .withShouldWorldStateUpdateHead(shouldUpdateHead)
                    .build())
            .orElseThrow();
  }

  /** Root of the chain head state plus the given changes, from the binary committer. */
  protected Hash computeBinaryRoot(
      final Consumer<BonsaiWorldStateUpdateAccumulator> accumulatorConsumer) {
    final BonsaiWorldState worldState =
        (BonsaiWorldState)
            protocolContext
                .getWorldStateArchive()
                .getWorldState(
                    WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(chainHeadHeader))
                .orElseThrow();
    try {
      final BonsaiWorldStateUpdateAccumulator accumulator =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();
      accumulatorConsumer.accept(accumulator);
      accumulator.commit();
      return new DefaultBinaryStateRootCommitter().compute(worldState, null, accumulator).root();
    } finally {
      worldState.close();
    }
  }
}
