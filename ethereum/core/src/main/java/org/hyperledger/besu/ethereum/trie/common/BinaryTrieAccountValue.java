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
package org.hyperledger.besu.ethereum.trie.common;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;

/** Binary-trie account value for trie logs: {@code [nonce, balance, codeHash]}. */
public final class BinaryTrieAccountValue implements TrieAccountValue {

  private final long nonce;
  private final Wei balance;
  private final Hash codeHash;

  public BinaryTrieAccountValue(final long nonce, final Wei balance, final Hash codeHash) {
    this.nonce = nonce;
    this.balance = balance;
    this.codeHash = codeHash;
  }

  @Override
  public long getNonce() {
    return nonce;
  }

  @Override
  public Wei getBalance() {
    return balance;
  }

  @Override
  public Hash getCodeHash() {
    return codeHash;
  }

  /** Binary trie accounts have no storage root. */
  @Override
  public Hash getStorageRoot() {
    throw new UnsupportedOperationException("binary trie accounts have no storage root");
  }

  @Override
  public StorageRootStrategy storageRootStrategy() {
    return BinaryStorageRootStrategy.INSTANCE;
  }

  @Override
  public void writeTo(final RLPOutput out) {
    out.startList();
    out.writeLongScalar(nonce);
    out.writeUInt256Scalar(balance);
    out.writeBytes(codeHash.getBytes());
    out.endList();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof BinaryTrieAccountValue that)) return false;
    return nonce == that.nonce
        && Objects.equals(balance, that.balance)
        && Objects.equals(codeHash, that.codeHash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nonce, balance, codeHash);
  }
}
