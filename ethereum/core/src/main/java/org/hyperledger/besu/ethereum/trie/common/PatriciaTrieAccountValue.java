/*
 * Copyright ConsenSys AG.
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

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.MptAccountValue;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;

/** Patricia (MPT) account value: {@code [nonce, balance, storageRoot, codeHash]}. */
public class PatriciaTrieAccountValue extends AbstractStateTrieAccountValue
    implements MptAccountValue {

  protected final Hash storageRoot;

  public PatriciaTrieAccountValue(
      final long nonce, final Wei balance, final Hash storageRoot, final Hash codeHash) {
    super(nonce, balance, codeHash);
    checkNotNull(storageRoot, "storageRoot cannot be null");
    this.storageRoot = storageRoot;
  }

  @Override
  public Hash getStorageRoot() {
    return storageRoot;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof PatriciaTrieAccountValue that)) return false;
    return nonce == that.nonce
        && Objects.equals(balance, that.balance)
        && Objects.equals(storageRoot, that.storageRoot)
        && Objects.equals(codeHash, that.codeHash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nonce, balance, storageRoot, codeHash);
  }

  @Override
  public void writeTo(final RLPOutput out) {
    out.startList();
    out.writeLongScalar(nonce);
    out.writeUInt256Scalar(balance);
    out.writeBytes(storageRoot.getBytes());
    out.writeBytes(codeHash.getBytes());
    out.endList();
  }

  public static PatriciaTrieAccountValue readFrom(final RLPInput in) {
    in.enterList();
    final long nonce = in.readLongScalar();
    final Wei balance = Wei.of(in.readUInt256Scalar());
    final Hash storageRoot = Hash.wrap(in.readBytes32());
    final Hash codeHash = Hash.wrap(in.readBytes32());
    in.leaveList();
    return new PatriciaTrieAccountValue(nonce, balance, storageRoot, codeHash);
  }
}
