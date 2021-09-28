/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider.createInMemoryWorldStateArchive;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.bonsai.BonsaiPersistedWorldState;
import org.hyperledger.besu.ethereum.bonsai.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.eth.manager.EthMessage;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetAccountRangeMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.worldstate.DefaultWorldStateArchive;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.Before;
import org.junit.Test;

public class SnapServerTest {

  private static final Hash START_HASH =
      Hash.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000000");
  private static final Hash END_HASH =
      Hash.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
  private final EthPeer ethPeer = mock(EthPeer.class);
  private final WorldStateArchive worldStateArchive = mock(WorldStateArchive.class);
  private final BonsaiPersistedWorldState mutableWorldState = mock(BonsaiPersistedWorldState.class);
  private final BonsaiWorldStateKeyValueStorage worldStateStorage =
      mock(BonsaiWorldStateKeyValueStorage.class);
  private final EthMessages ethMessages = new EthMessages();

  @Before
  public void setUp() {
    new SnapServer(ethMessages, worldStateArchive);
    when(worldStateArchive.getMutable()).thenReturn(mutableWorldState);
    when(mutableWorldState.getWorldStateStorage()).thenReturn(worldStateStorage);
  }

  @Test
  public void shouldHandleGetAccountRangeRequests() throws Exception {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final DefaultWorldStateArchive inMemoryWorldStateArchive = createInMemoryWorldStateArchive();
    final List<Address> accounts = new ArrayList<>();
    accounts.add(Address.extract(START_HASH));
    accounts.add(Address.extract(END_HASH));
    final List<Block> blocks =
        gen.blockSequence(10, inMemoryWorldStateArchive, accounts, new ArrayList<>());

    doAnswer(
            invocation ->
                inMemoryWorldStateArchive
                    .getWorldStateStorage()
                    .getAccountStateTrieNode(
                        invocation.getArgument(0, Bytes.class),
                        invocation.getArgument(1, Bytes32.class))
                    .map(Bytes::wrap))
        .when(worldStateStorage)
        .getAccountStateTrieNode(any(), any());

    doAnswer(
            invocation ->
                inMemoryWorldStateArchive.getAccountProofRelatedNodes(
                    invocation.getArgument(0, Hash.class), invocation.getArgument(1, Hash.class)))
        .when(worldStateArchive)
        .getAccountProofRelatedNodes(any(), any());

    final Hash rootHash = blocks.get(blocks.size() - 1).getHeader().getStateRoot();
    Optional<MessageData> response =
        ethMessages.dispatch(
            new EthMessage(
                ethPeer,
                GetAccountRangeMessage.create(
                        rootHash, START_HASH, END_HASH, BigInteger.valueOf(10000))
                    .wrapMessageData(BigInteger.ONE)));

    assertThat(response).containsInstanceOf(AccountRangeMessage.class);

    final AccountRangeMessage messageData = (AccountRangeMessage) response.get();
    final AccountRangeMessage.AccountRangeData accountData = messageData.accountData(false);
    assertThat(accountData.accounts())
        .containsKeys(Hash.hash(accounts.get(0)).copy(), Hash.hash(accounts.get(1)).copy());
    assertThat(accountData.proofs()).isNotEmpty();
  }

  @Test
  public void shouldHandleGetAccountRangeRequestsCorrectlyWhenNoAccountInRange() throws Exception {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final DefaultWorldStateArchive inMemoryWorldStateArchive = createInMemoryWorldStateArchive();
    final List<Address> accounts = new ArrayList<>();
    accounts.add(Address.extract(START_HASH));
    accounts.add(Address.extract(END_HASH));
    final List<Block> blocks =
        gen.blockSequence(10, inMemoryWorldStateArchive, accounts, new ArrayList<>());

    doAnswer(
            invocation ->
                inMemoryWorldStateArchive
                    .getWorldStateStorage()
                    .getAccountStateTrieNode(
                        invocation.getArgument(0, Bytes.class),
                        invocation.getArgument(1, Bytes32.class))
                    .map(Bytes::wrap))
        .when(worldStateStorage)
        .getAccountStateTrieNode(any(), any());

    doAnswer(
            invocation ->
                inMemoryWorldStateArchive.getAccountProofRelatedNodes(
                    invocation.getArgument(0, Hash.class), invocation.getArgument(1, Hash.class)))
        .when(worldStateArchive)
        .getAccountProofRelatedNodes(any(), any());

    final Hash endHash =
        Hash.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000001");

    final Hash rootHash = blocks.get(blocks.size() - 1).getHeader().getStateRoot();
    Optional<MessageData> response =
        ethMessages.dispatch(
            new EthMessage(
                ethPeer,
                GetAccountRangeMessage.create(
                        rootHash, START_HASH, endHash, BigInteger.valueOf(10000))
                    .wrapMessageData(BigInteger.ONE)));

    assertThat(response).containsInstanceOf(AccountRangeMessage.class);

    final AccountRangeMessage messageData = (AccountRangeMessage) response.get();
    final AccountRangeMessage.AccountRangeData accountData = messageData.accountData(false);
    assertThat(accountData.accounts()).containsOnlyKeys(Hash.hash(accounts.get(0)).copy());
    assertThat(accountData.proofs()).isNotEmpty();
  }

  @Test
  public void shouldHandleGetAccountRangeRequestsCorrectlyWithValidResponseSize() throws Exception {
    final BlockDataGenerator gen = new BlockDataGenerator();
    final DefaultWorldStateArchive inMemoryWorldStateArchive = createInMemoryWorldStateArchive();
    final List<Address> accounts = new ArrayList<>();
    accounts.add(Address.extract(START_HASH));
    accounts.add(Address.extract(END_HASH));
    final List<Block> blocks =
        gen.blockSequence(10, inMemoryWorldStateArchive, accounts, new ArrayList<>());

    doAnswer(
            invocation ->
                inMemoryWorldStateArchive
                    .getWorldStateStorage()
                    .getAccountStateTrieNode(
                        invocation.getArgument(0, Bytes.class),
                        invocation.getArgument(1, Bytes32.class))
                    .map(Bytes::wrap))
        .when(worldStateStorage)
        .getAccountStateTrieNode(any(), any());

    doAnswer(
            invocation ->
                inMemoryWorldStateArchive.getAccountProofRelatedNodes(
                    invocation.getArgument(0, Hash.class), invocation.getArgument(1, Hash.class)))
        .when(worldStateArchive)
        .getAccountProofRelatedNodes(any(), any());

    final Hash endHash =
        Hash.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000001");

    final Hash rootHash = blocks.get(blocks.size() - 1).getHeader().getStateRoot();
    Optional<MessageData> response =
        ethMessages.dispatch(
            new EthMessage(
                ethPeer,
                GetAccountRangeMessage.create(rootHash, START_HASH, endHash, BigInteger.ONE)
                    .wrapMessageData(BigInteger.ONE)));

    assertThat(response).containsInstanceOf(AccountRangeMessage.class);

    final AccountRangeMessage messageData = (AccountRangeMessage) response.get();
    final AccountRangeMessage.AccountRangeData accountData = messageData.accountData(false);
    assertThat(accountData.accounts()).isEmpty();
    assertThat(accountData.proofs()).isNotEmpty();
  }
}