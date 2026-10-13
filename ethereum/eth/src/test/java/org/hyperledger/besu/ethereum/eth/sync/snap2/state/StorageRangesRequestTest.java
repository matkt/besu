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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;

import java.math.BigInteger;
import java.util.List;
import java.util.NavigableMap;
import java.util.function.Function;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class StorageRangesRequestTest {

  private static final Bytes32 ACCOUNT = Bytes32.fromHexString("0x01");
  private final SimpleMerklePatriciaTrie<Bytes32, Bytes> storage =
      new SimpleMerklePatriciaTrie<>(Function.identity());

  StorageRangesRequestTest() {
    for (int i = 1; i <= 3; i++) {
      storage.put(slot(i * 0x40), RLP.encodeOne(UInt256.valueOf(i).trimLeadingZeros()));
    }
  }

  // geth answers a range without slots with no slot list, only the proof of the origin
  @Test
  void completesARangeWhoseProofShowsNoSlotFromTheOrigin() {
    final Bytes32 origin = slot(0xd0);

    final StorageRangesRequest.Result result = chunk(origin).decode(proofOnly(origin), peer());

    assertThat(result.accounts()).hasSize(1);
    assertThat(result.accounts().getFirst().hashes()).isEmpty();
    assertThat(result.accounts().getFirst().complete()).isTrue();
  }

  @Test
  void rejectsAProofThatHidesSlotsAfterTheOrigin() {
    final Bytes32 origin = slot(0x50);

    assertThatThrownBy(() -> chunk(origin).decode(proofOnly(origin), peer()))
        .isInstanceOf(SyncRequest.InvalidResponseException.class);
  }

  @Test
  void treatsAnAnswerWithoutSlotsOrProofAsEmpty() {
    final MessageData nothing =
        StorageRangeMessage.create(new ArrayDeque<>(), List.of()).wrapMessageData(BigInteger.ONE);

    assertThatThrownBy(() -> chunk(slot(0xd0)).decode(nothing, peer()))
        .isInstanceOf(SyncRequest.EmptyResponseException.class);
  }

  private StorageRangesRequest chunk(final Bytes32 origin) {
    final StateSync.Pivot pivot =
        new StateSync.Pivot(0, new BlockHeaderTestFixture().number(1).buildHeader());
    final StorageTask task = new StorageTask(ACCOUNT, storage.getRootHash());
    return new StorageRangesRequest(
        pivot,
        List.of(task),
        origin,
        RangeManager.MAX_RANGE,
        new RangeTask(origin, RangeManager.MAX_RANGE),
        4096);
  }

  private MessageData proofOnly(final Bytes32 origin) {
    final ArrayDeque<NavigableMap<Bytes32, Bytes>> noSlots = new ArrayDeque<>();
    return StorageRangeMessage.create(
            noSlots, storage.getValueWithProof(origin).getProofRelatedNodes())
        .wrapMessageData(BigInteger.ONE);
  }

  private static Bytes32 slot(final int firstByte) {
    return Bytes32.wrap(Bytes.concatenate(Bytes.of(firstByte), Bytes.wrap(new byte[31])));
  }

  private static EthPeer peer() {
    return mock(EthPeer.class);
  }
}
