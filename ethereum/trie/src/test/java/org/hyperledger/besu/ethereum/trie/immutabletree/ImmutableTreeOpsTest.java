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
package org.hyperledger.besu.ethereum.trie.immutabletree;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class ImmutableTreeOpsTest {

  private static final ImmutableTreeOps.NodeResolver IDENTITY = node -> node;

  @Test
  void putAndGetRoundTrip() {
    ImmutableTreeNode root = EmptyTreeNode.INSTANCE;
    root = ImmutableTreeOps.put(root, Bytes.fromHexString("0x01"), Bytes.fromHexString("0xaa"), IDENTITY);
    root = ImmutableTreeOps.put(root, Bytes.fromHexString("0x02"), Bytes.fromHexString("0xbb"), IDENTITY);
    root = ImmutableTreeOps.put(root, Bytes.fromHexString("0x0100"), Bytes.fromHexString("0xcc"), IDENTITY);

    assertThat(ImmutableTreeOps.get(root, Bytes.fromHexString("0x01"), IDENTITY))
        .contains(Bytes.fromHexString("0xaa"));
    assertThat(ImmutableTreeOps.get(root, Bytes.fromHexString("0x02"), IDENTITY))
        .contains(Bytes.fromHexString("0xbb"));
    assertThat(ImmutableTreeOps.get(root, Bytes.fromHexString("0x0100"), IDENTITY))
        .contains(Bytes.fromHexString("0xcc"));
    assertThat(ImmutableTreeOps.get(root, Bytes.fromHexString("0x03"), IDENTITY)).isEmpty();
  }

  @Test
  void overwriteCreatesNewRootHash() {
    final ImmutableTreeNode r1 =
        ImmutableTreeOps.put(
            EmptyTreeNode.INSTANCE,
            Bytes.fromHexString("0xabcd"),
            Bytes.fromHexString("0x01"),
            IDENTITY);
    final ImmutableTreeNode r2 =
        ImmutableTreeOps.put(r1, Bytes.fromHexString("0xabcd"), Bytes.fromHexString("0x02"), IDENTITY);

    assertThat(r1.hash()).isNotEqualTo(r2.hash());
    assertThat(ImmutableTreeOps.get(r1, Bytes.fromHexString("0xabcd"), IDENTITY))
        .contains(Bytes.fromHexString("0x01"));
    assertThat(ImmutableTreeOps.get(r2, Bytes.fromHexString("0xabcd"), IDENTITY))
        .contains(Bytes.fromHexString("0x02"));
  }

  @Test
  void encodeDecodeLeafPreservesValue() {
    final LeafTreeNode leaf =
        new LeafTreeNode(TreeCodec.bytesToPath(Bytes.fromHexString("0xabcd")), Bytes.of(1, 2, 3));
    final ImmutableTreeNode decoded =
        TreeNodeDecoder.decode(Bytes.EMPTY, leaf.hash(), leaf.rlp());
    assertThat(decoded).isInstanceOf(LeafTreeNode.class);
    assertThat(decoded.value()).isEqualTo(Optional.of(Bytes.of(1, 2, 3)));
    assertThat(decoded.hash()).isEqualTo(leaf.hash());
  }
}
