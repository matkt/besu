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

import static org.hyperledger.besu.crypto.Hash.keccak256;

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;

/** Compact path encoding and RLP helpers for immutable tree nodes. */
final class TreeCodec {
  static final byte LEAF_TERMINATOR = 0x10;
  static final Bytes EMPTY_RLP = RLP.NULL;
  static final Bytes32 EMPTY_HASH = keccak256(EMPTY_RLP);

  private TreeCodec() {}

  static Bytes bytesToNibbles(final Bytes key) {
    final MutableBytes path = MutableBytes.create(key.size() * 2);
    for (int i = 0; i < key.size(); i++) {
      final byte b = key.get(i);
      path.set(i * 2, (byte) ((b >>> 4) & 0x0f));
      path.set(i * 2 + 1, (byte) (b & 0x0f));
    }
    return path;
  }

  static Bytes encodeLeafPath(final Bytes nibbles) {
    return encodePath(nibbles, true);
  }

  static Bytes encodeExtensionPath(final Bytes nibbles) {
    return encodePath(nibbles, false);
  }

  private static Bytes encodePath(final Bytes nibbles, final boolean leaf) {
    final int nibbleCount = nibbles.size();
    final boolean odd = (nibbleCount & 1) == 1;
    final int size = (nibbleCount / 2) + 1;
    final MutableBytes out = MutableBytes.create(size);
    int nibbleIndex = 0;
    int byteIndex = 0;
    int flag = leaf ? 0x20 : 0x00;
    if (odd) {
      flag |= 0x10;
      out.set(byteIndex++, (byte) (flag | (nibbles.get(nibbleIndex++) & 0x0f)));
    } else {
      out.set(byteIndex++, (byte) flag);
    }
    while (nibbleIndex < nibbleCount) {
      final int hi = nibbles.get(nibbleIndex++) & 0x0f;
      final int lo = nibbles.get(nibbleIndex++) & 0x0f;
      out.set(byteIndex++, (byte) ((hi << 4) | lo));
    }
    return out;
  }

  static Bytes decodePath(final Bytes compact, final boolean expectLeaf) {
    if (compact.isEmpty()) {
      return Bytes.EMPTY;
    }
    final int first = compact.get(0) & 0xff;
    final boolean leaf = (first & 0x20) != 0;
    if (leaf != expectLeaf) {
      throw new IllegalArgumentException("Unexpected path terminator flag");
    }
    final boolean odd = (first & 0x10) != 0;
    final int remaining = compact.size() - 1;
    final int nibbleCount = remaining * 2 + (odd ? 1 : 0);
    final MutableBytes nibbles = MutableBytes.create(nibbleCount);
    int out = 0;
    if (odd) {
      nibbles.set(out++, (byte) (first & 0x0f));
    }
    for (int i = 1; i < compact.size(); i++) {
      final byte b = compact.get(i);
      nibbles.set(out++, (byte) ((b >>> 4) & 0x0f));
      nibbles.set(out++, (byte) (b & 0x0f));
    }
    return nibbles;
  }

  static Bytes encodeLeaf(final Bytes pathNibbles, final Bytes value) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(encodeLeafPath(pathNibbles));
    out.writeBytes(value);
    out.endList();
    return out.encoded();
  }

  static Bytes encodeExtension(final Bytes pathNibbles, final Bytes childRef) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(encodeExtensionPath(pathNibbles));
    writeChildRef(out, childRef);
    out.endList();
    return out.encoded();
  }

  static Bytes encodeBranch(final Bytes[] childRefs, final Bytes value) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    for (int i = 0; i < 16; i++) {
      writeChildRef(out, childRefs[i]);
    }
    if (value == null || value.isEmpty()) {
      out.writeNull();
    } else {
      out.writeBytes(value);
    }
    out.endList();
    return out.encoded();
  }

  private static void writeChildRef(final BytesValueRLPOutput out, final Bytes childRef) {
    if (childRef == null || childRef.isEmpty() || childRef.equals(EMPTY_RLP)) {
      out.writeNull();
    } else {
      // Child refs are already RLP-encoded (hash string or inline node list).
      out.writeRaw(childRef);
    }
  }

  static Bytes reference(final ImmutableTreeNode node) {
    if (node instanceof EmptyTreeNode) {
      return EMPTY_RLP;
    }
    final Bytes rlp = node.rlp();
    if (rlp.size() < 32) {
      return rlp;
    }
    return RLP.encodeOne(node.hash());
  }

  static Bytes32 hashOf(final Bytes rlp) {
    if (rlp.isEmpty() || rlp.equals(EMPTY_RLP)) {
      return EMPTY_HASH;
    }
    return keccak256(rlp);
  }

  static int commonPrefixLength(final Bytes a, final Bytes b) {
    final int max = Math.min(a.size(), b.size());
    int i = 0;
    while (i < max && a.get(i) == b.get(i)) {
      i++;
    }
    return i;
  }
}
