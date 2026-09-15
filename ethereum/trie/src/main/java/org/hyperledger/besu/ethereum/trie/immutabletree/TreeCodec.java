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
import org.hyperledger.besu.ethereum.trie.CompactEncoding;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Compact path encoding and RLP helpers for immutable tree nodes. */
final class TreeCodec {
  static final Bytes EMPTY_RLP = RLP.NULL;
  static final Bytes32 EMPTY_HASH = keccak256(EMPTY_RLP);

  private TreeCodec() {}

  static Bytes bytesToPath(final Bytes key) {
    return CompactEncoding.bytesToPath(key);
  }

  static Bytes encodeLeaf(final Bytes pathWithTerminator, final Bytes value) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(CompactEncoding.encode(pathWithTerminator));
    out.writeBytes(value);
    out.endList();
    return out.encoded();
  }

  static Bytes encodeExtension(final Bytes pathNibbles, final Bytes childRef) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(CompactEncoding.encode(pathNibbles));
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
      out.writeRaw(childRef);
    }
  }

  static Bytes reference(final ImmutableTreeNode node) {
    if (node instanceof EmptyTreeNode) {
      return EMPTY_RLP;
    }
    // Stored placeholders are hash refs only — never ask them for RLP.
    if (node.isStored()) {
      return RLP.encodeOne(node.hash());
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
    return a.commonPrefixLength(b);
  }
}
