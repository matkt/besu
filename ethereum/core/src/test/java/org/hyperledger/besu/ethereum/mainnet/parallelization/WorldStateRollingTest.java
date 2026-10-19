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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_2;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.MINING_BENEFICIARY;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.BonsaiTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogLayer;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import org.junit.jupiter.api.Test;

/** Rolling the head world state, which moves the flat DB and the trie together. */
class WorldStateRollingTest extends AbstractParallelBlockProcessorIntegrationTest {

  private static final Address SENDER = Address.fromHexStringStrict(ACCOUNT_GENESIS_2);

  @Override
  protected String getVariantName() {
    return "World state rolling";
  }

  @Test
  void headPersistedOnABlockMissingFromTheChainRollsBackToItsParent() {
    final ExecutionContextTestFixture ctx = createFreshContext();
    final BlockHeader parent = ctx.getBlockchain().getChainHeadHeader();
    final Hash stateRoot =
        discoverStateRootAtParent(ctx, parent, Wei.of(5), MINING_BENEFICIARY, transfer());
    final MutableWorldState head =
        ctx.getStateArchive().getWorldState(withBlockHeaderAndUpdateNodeHead(parent)).orElseThrow();
    final Block block =
        createBlock(ctx, parent, stateRoot, Wei.of(5), MINING_BENEFICIARY, transfer());
    // persisted on the head, as a block whose body then fails validation is, but never appended
    assertThat(process(createSequentialProcessor(ctx), ctx, head, block).isSuccessful()).isTrue();

    assertThat(
            ctx.getStateArchive()
                .getWorldState(withBlockHeaderAndUpdateNodeHead(parent))
                .map(MutableWorldState::rootHash))
        .contains(parent.getStateRoot());
    assertThat(storage(ctx).getFlatDbBlockHash()).contains(parent.getBlockHash());
  }

  @Test
  void rollFailingOnItsStateRootLeavesTheFlatDbAtTheHead() {
    final ExecutionContextTestFixture ctx = createFreshContext();
    final BonsaiWorldStateProvider archive = (BonsaiWorldStateProvider) ctx.getStateArchive();
    final BlockHeader genesis = ctx.getBlockchain().getChainHeadHeader();
    final BlockHeader head = importBlock(ctx, transfer()).getHeader();
    final Wei senderBalance = archive.getWorldState().get(SENDER).getBalance();
    saveWithWrongSenderPrior(archive, head);

    assertThat(archive.getWorldState(withBlockHeaderAndUpdateNodeHead(genesis))).isEmpty();
    assertThat(archive.getWorldState().rootHash()).isEqualTo(head.getStateRoot());
    assertThat(archive.getWorldState().get(SENDER).getBalance()).isEqualTo(senderBalance);
    assertThat(storage(ctx).getFlatDbBlockHash()).contains(head.getBlockHash());
  }

  private Transaction transfer() {
    return createTransferTransaction(
        0, 1_000L, 300_000L, 0L, 5L, ACCOUNT_2, ACCOUNT_GENESIS_2_KEYPAIR);
  }

  private static BonsaiWorldStateKeyValueStorage storage(final ExecutionContextTestFixture ctx) {
    return ((BonsaiWorldStateProvider) ctx.getStateArchive()).getWorldStateKeyValueStorage();
  }

  // replaces the trie log of the head block with one whose sender prior is off by one wei
  private static void saveWithWrongSenderPrior(
      final BonsaiWorldStateProvider archive, final BlockHeader head) {
    final TrieLog trieLog =
        archive.getTrieLogManager().getTrieLogLayer(head.getBlockHash()).orElseThrow();
    final TrieLogLayer corrupted =
        new TrieLogLayer().setBlockHash(head.getBlockHash()).setBlockNumber(head.getNumber());
    trieLog
        .getAccountChanges()
        .forEach(
            (address, change) -> {
              final AccountValue prior = change.getPrior();
              corrupted.addAccountChange(
                  address,
                  prior == null || !address.equals(SENDER)
                      ? prior
                      : new PatriciaTrieAccountValue(
                          prior.getNonce(),
                          prior.getBalance().add(Wei.ONE),
                          prior.getStorageRoot(),
                          prior.getCodeHash()),
                  change.getUpdated());
            });
    trieLog
        .getStorageChanges()
        .forEach(
            (address, slots) ->
                slots.forEach(
                    (slot, change) ->
                        corrupted.addStorageChange(
                            address, slot, change.getPrior(), change.getUpdated())));
    trieLog
        .getCodeChanges()
        .forEach(
            (address, change) ->
                corrupted.addCodeChange(
                    address, change.getPrior(), change.getUpdated(), head.getBlockHash()));
    corrupted.freeze();
    final BonsaiWorldStateKeyValueStorage.Updater updater =
        archive.getWorldStateKeyValueStorage().updater();
    updater
        .getTrieLogStorageTransaction()
        .put(
            head.getBlockHash().getBytes().toArrayUnsafe(),
            new BonsaiTrieLogFactory().serialize(corrupted));
    updater.commitTrieLogOnly();
  }
}
