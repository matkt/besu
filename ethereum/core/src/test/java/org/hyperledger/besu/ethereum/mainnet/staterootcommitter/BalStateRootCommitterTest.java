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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary.BinaryBalEngine;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class BalStateRootCommitterTest {

  private static final long PARENT_TIMESTAMP = 1_000L;
  private static final long BLOCK_TIMESTAMP = 2_000L;

  /**
   * The parent world state must be looked up with the timestamp of the block being processed, not
   * the parent's own timestamp: the world state provider uses it to detect the PBT transition and
   * therefore whether to serve the parent in PATRICIA or BINARY form.
   */
  @Test
  void parentWorldStateIsRequestedWithBlockTimestamp() {
    final BlockHeader parentHeader =
        new BlockHeaderTestFixture().number(10L).timestamp(PARENT_TIMESTAMP).buildHeader();
    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .number(11L)
            .parentHash(parentHeader.getHash())
            .timestamp(BLOCK_TIMESTAMP)
            .buildHeader();

    final MutableBlockchain blockchain = mock(MutableBlockchain.class);
    when(blockchain.getBlockHeader(parentHeader.getHash())).thenReturn(Optional.of(parentHeader));
    final BonsaiWorldState parentWorldState = mock(BonsaiWorldState.class);
    when(parentWorldState.getWorldStateRootHash()).thenReturn(Hash.EMPTY_TRIE_HASH);
    final WorldStateArchive worldStateArchive = mock(WorldStateArchive.class);
    when(worldStateArchive.getWorldState(any())).thenReturn(Optional.of(parentWorldState));
    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(protocolContext.getWorldStateArchive()).thenReturn(worldStateArchive);

    final BalStateRootCommitter committer =
        new BalStateRootCommitter(
            protocolContext,
            blockHeader,
            BlockAccessListAccountLookup.of(new BlockAccessList(List.of())),
            false,
            BinaryBalEngine.INSTANCE);
    // waits for the background computation to complete
    committer.compute(parentWorldState, null, mock(BonsaiWorldStateUpdateAccumulator.class));

    final ArgumentCaptor<WorldStateQueryParams> queryParams =
        ArgumentCaptor.forClass(WorldStateQueryParams.class);
    verify(worldStateArchive).getWorldState(queryParams.capture());
    assertThat(queryParams.getValue().getBlockHash()).isEqualTo(parentHeader.getHash());
    assertThat(queryParams.getValue().getTimeStamp()).contains(BLOCK_TIMESTAMP);
  }
}
