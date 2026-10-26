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
package org.hyperledger.besu.ethereum.trie.common;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import org.apache.tuweni.bytes.Bytes32;

/**
 * An {@link AccountValue} that knows the {@link StorageRootStrategy} reproducing its own RLP shape.
 *
 * <p>{@link AccountValue} lives in the {@code datatypes} module, which cannot see {@link
 * StorageRootStrategy}; this interface carries the mapping on the core side instead.
 */
public interface TrieAccountValue extends AccountValue {

  /**
   * The strategy matching this value's encoded shape, safe to hand to a new account: a strategy
   * owns a mutable storage root and is never shared between accounts.
   *
   * @return a strategy preserving this value's RLP shape.
   */
  StorageRootStrategy storageRootStrategy();

  /**
   * Reads an account of either trie: 3 fields for a binary trie account, 4 for a Patricia one.
   *
   * @param input the RLP of the value
   * @return the value
   */
  static TrieAccountValue readFrom(final RLPInput input) {
    final int fieldCount = input.enterList();
    final long nonce = input.readLongScalar();
    final Wei balance = Wei.of(input.readUInt256Scalar());
    final Hash storageRoot =
        fieldCount == 4 ? Hash.wrap(orEmpty(input, Hash.EMPTY_TRIE_HASH)) : null;
    final Hash codeHash = Hash.wrap(orEmpty(input, Hash.EMPTY));
    input.leaveList();
    return storageRoot == null
        ? new BinaryTrieAccountValue(nonce, balance, codeHash)
        : new PatriciaTrieAccountValue(nonce, balance, storageRoot, codeHash);
  }

  private static Bytes32 orEmpty(final RLPInput input, final Hash empty) {
    if (input.nextIsNull()) {
      input.skipNext();
      return Bytes32.wrap(empty.getBytes());
    }
    return input.readBytes32();
  }
}
