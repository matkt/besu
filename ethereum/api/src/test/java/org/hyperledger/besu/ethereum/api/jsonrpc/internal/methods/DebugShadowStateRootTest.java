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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DebugShadowStateRootTest {

  private static final Hash BLOCK_HASH = Hash.hash(Bytes.of(1));
  private static final Hash SHADOW_ROOT = Hash.hash(Bytes.of(2));

  private final MutableBlockchain blockchain = mock(MutableBlockchain.class);
  private final BonsaiWorldStateKeyValueStorage storage =
      mock(BonsaiWorldStateKeyValueStorage.class);
  private DebugShadowStateRoot method;

  @BeforeEach
  void setUp() {
    final BonsaiWorldStateProvider archive = mock(BonsaiWorldStateProvider.class);
    when(archive.getWorldStateKeyValueStorage()).thenReturn(storage);
    final ProtocolContext protocolContext = mock(ProtocolContext.class);
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(protocolContext.getWorldStateArchive()).thenReturn(archive);
    method = new DebugShadowStateRoot(protocolContext);
  }

  @Test
  void answersTheRootRecordedForTheBlock() {
    when(blockchain.getBlockHeader(BLOCK_HASH)).thenReturn(Optional.of(mock(BlockHeader.class)));
    when(storage.getShadowStateRoot(BLOCK_HASH)).thenReturn(Optional.of(SHADOW_ROOT));

    assertThat(call(BLOCK_HASH)).isEqualTo(SHADOW_ROOT);
  }

  @Test
  void answersNullForABlockWithoutRecord() {
    when(blockchain.getBlockHeader(BLOCK_HASH)).thenReturn(Optional.of(mock(BlockHeader.class)));
    when(storage.getShadowStateRoot(BLOCK_HASH)).thenReturn(Optional.empty());

    assertThat(call(BLOCK_HASH)).isNull();
  }

  @Test
  void answersNullForAnUnknownBlock() {
    when(blockchain.getBlockHeader(BLOCK_HASH)).thenReturn(Optional.empty());

    assertThat(call(BLOCK_HASH)).isNull();
    verifyNoInteractions(storage);
  }

  private Object call(final Hash blockHash) {
    final JsonRpcRequestContext request =
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0", "debug_shadowStateRoot", new Object[] {blockHash.toHexString()}));
    return ((JsonRpcSuccessResponse) method.response(request)).getResult();
  }
}
