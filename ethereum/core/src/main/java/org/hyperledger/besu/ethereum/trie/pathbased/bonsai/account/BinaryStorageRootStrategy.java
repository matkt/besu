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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Optional;

/**
 * Binary (partitioned binary trie) storage-root strategy. Stateless: the account RLP is the 3-field
 * {@code [nonce, balance, codeHash]} — there is no per-account storage root. MPT-specific accessors
 * throw {@link UnsupportedOperationException}.
 */
public final class BinaryStorageRootStrategy implements StorageRootStrategy {

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
  public boolean isStorageEmpty() {
    throw new UnsupportedOperationException(
        "isStorageEmpty() is not applicable for binary trie accounts (no storage root)");
  }

  @Override
  public boolean hasStorageRoot() {
    return false;
  }

  @Override
  public void writeStorageRoot(final RLPOutput out) {
    // No storage-root field in the binary account RLP.
  }

  @Override
  public Optional<Hash> readStorageRoot(final RLPInput in, final int listSize) {
    if (listSize == 4) {
      // Legacy MPT-encoded account: consume and discard the storageRoot field.
      in.skipNext();
    }
    return Optional.empty();
  }

  @Override
  public void assertStorageRootMatches(final AccountValue other, final String context) {
    // Binary accounts carry no storage root: nothing to compare. Binary account diffs do not
    // assert on storage root. Do not read the other account's storage root — it is not available
    // on the AccountValue interface and would throw for a binary other.
  }

  @Override
  public StorageRootStrategy copy() {
    return this;
  }
}
