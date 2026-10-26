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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.convert;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.BasicDataEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.CodeChunkifier;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.CodeRefCountEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.DelegationEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.Leaf;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/** The PBT leaves of accounts, slots and codes, as EIP-8297 keys and encodes them. */
public final class Eip8347Leaves {

  /** Bytes of code per chunk. */
  private static final int CHUNK_BYTES = 31;

  private Eip8347Leaves() {}

  /**
   * Header leaves of an account: basic data, then code hash or EIP-7702 delegation target.
   *
   * @param code the account's code: empty, a contract's, or a delegation indicator
   */
  public static List<Leaf> header(
      final Bytes32 address32,
      final long nonce,
      final Wei balance,
      final Hash codeHash,
      final Bytes code) {
    final UInt256 value = balance.toUInt256();
    if (CodeDelegationHelper.hasCodeDelegation(code)) {
      return List.of(
          new Leaf(
              TrieKeyDerivation.getTreeKeyForBasicData(address32),
              BasicDataEncoder.encodeBasicData(
                  EmbeddingParameters.DELEGATION_CODE_SIZE, nonce, value)),
          new Leaf(
              TrieKeyDerivation.getTreeKeyForDelegation(address32),
              DelegationEncoder.encodeDelegation(
                  CodeDelegationHelper.getTargetAddress(code).getBytes())));
    }
    return List.of(
        new Leaf(
            TrieKeyDerivation.getTreeKeyForBasicData(address32),
            BasicDataEncoder.encodeBasicData(code.size(), nonce, value)),
        new Leaf(
            TrieKeyDerivation.getTreeKeyForCodeHash(address32), Bytes32.wrap(codeHash.getBytes())));
  }

  /** The leaf of a slot holding {@code value}, not zero. */
  public static Leaf storage(
      final Bytes32 address32,
      final Bytes32 addressKeyHash,
      final UInt256 slotKey,
      final UInt256 value) {
    return new Leaf(
        TrieKeyDerivation.getTreeKeyForStorageSlot(address32, addressKeyHash, slotKey),
        Bytes32.leftPad(value));
  }

  /** CODE_ZONE leaves of a code: its non-zero chunks. */
  public static List<Leaf> code(final Bytes32 codeHash, final Bytes code) {
    final List<Bytes32> chunks = CodeChunkifier.chunkifyCode(code);
    final List<Leaf> leaves = new ArrayList<>(chunks.size());
    for (int i = 0; i < chunks.size(); i++) {
      if (!chunks.get(i).isZero()) {
        leaves.add(new Leaf(TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash, i), chunks.get(i)));
      }
    }
    return leaves;
  }

  /** A code's reference count as the live trie keeps it: its references and chunk count. */
  public static Bytes32 referenceCount(final long references, final int codeSize) {
    return CodeRefCountEncoder.encode(references, (codeSize + CHUNK_BYTES - 1) / CHUNK_BYTES);
  }
}
