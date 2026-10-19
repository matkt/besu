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

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

/**
 * Storage root strategy of the binary trie, which has no storage root: the account RLP is the
 * 3-field {@code [nonce, balance, codeHash]}. Stateless, so one instance is shared.
 */
public final class BinaryStorageRootStrategy implements StorageRootStrategy {

  /** The shared instance. */
  public static final BinaryStorageRootStrategy INSTANCE = new BinaryStorageRootStrategy();

  private BinaryStorageRootStrategy() {}

  @Override
  public Hash getStorageRoot() {
    throw new UnsupportedOperationException(
        "getStorageRoot() is not applicable for binary trie accounts (no storage root)");
  }

  @Override
  public void setStorageRoot(final Hash root) {
    throw new UnsupportedOperationException(
        "setStorageRoot() is not applicable for binary trie accounts (no storage root)");
  }

  @Override
  public boolean hasStorageRoot() {
    return false;
  }

  @Override
  public void writeStorageRoot(final RLPOutput out) {
    // no storage root field in a binary account
  }

  @Override
  public void readStorageRoot(final RLPInput in) {
    // no storage root field in a binary account
  }

  @Override
  public void assertStorageRootMatches(final AccountValue other, final String context) {
    // a binary account has no storage root to compare
  }

  @Override
  public StorageRootStrategy copy() {
    return this;
  }
}
