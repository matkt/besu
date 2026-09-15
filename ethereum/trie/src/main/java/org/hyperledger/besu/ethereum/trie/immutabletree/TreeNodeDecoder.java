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

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Decodes RLP payloads into immutable tree nodes. Hash child refs become {@link StoredTreeNode}. */
final class TreeNodeDecoder {
  private TreeNodeDecoder() {}

  static ImmutableTreeNode decode(final Bytes location, final Bytes32 expectedHash, final Bytes rlp) {
    if (rlp == null || rlp.isEmpty() || rlp.equals(TreeCodec.EMPTY_RLP)) {
      return EmptyTreeNode.INSTANCE;
    }
    final Bytes32 actual = TreeCodec.hashOf(rlp);
    if (!actual.equals(expectedHash)) {
      throw new IllegalStateException(
          "Loaded node hash mismatch at "
              + location
              + ": expected "
              + expectedHash
              + " got "
              + actual);
    }
    final RLPInput in = RLP.input(rlp);
    final int items = in.enterList();
    if (items == 2) {
      final Bytes compact = in.readBytes();
      final Bytes path = CompactEncoding.decode(compact);
      final boolean leaf =
          !path.isEmpty() && path.get(path.size() - 1) == CompactEncoding.LEAF_TERMINATOR;
      if (leaf) {
        final Bytes value = in.readBytes();
        in.leaveList();
        return new LeafTreeNode(path, value);
      }
      final ImmutableTreeNode child = readChild(in, Bytes.concatenate(location, path));
      in.leaveList();
      return new ExtensionTreeNode(path, child);
    }
    if (items == 17) {
      final ImmutableTreeNode[] children = new ImmutableTreeNode[16];
      for (int i = 0; i < 16; i++) {
        if (in.nextIsNull()) {
          in.skipNext();
          children[i] = null;
        } else {
          children[i] = readChild(in, Bytes.concatenate(location, Bytes.of((byte) i)));
        }
      }
      final Bytes value;
      if (in.nextIsNull()) {
        in.skipNext();
        value = null;
      } else {
        value = in.readBytes();
      }
      in.leaveList();
      return new BranchTreeNode(children, value);
    }
    in.leaveList();
    throw new IllegalArgumentException("Unsupported trie node list size " + items);
  }

  private static ImmutableTreeNode readChild(final RLPInput in, final Bytes childLocation) {
    if (in.nextIsNull()) {
      in.skipNext();
      return EmptyTreeNode.INSTANCE;
    }
    if (in.nextIsList()) {
      final Bytes nestedRlp = in.readAsRlp().raw();
      final Bytes32 hash = TreeCodec.hashOf(nestedRlp);
      return decode(childLocation, hash, nestedRlp);
    }
    final Bytes ref = in.readBytes();
    if (ref.size() == 32) {
      return new StoredTreeNode(childLocation, Bytes32.wrap(ref));
    }
    final Bytes32 hash = TreeCodec.hashOf(ref);
    return decode(childLocation, hash, ref);
  }
}
