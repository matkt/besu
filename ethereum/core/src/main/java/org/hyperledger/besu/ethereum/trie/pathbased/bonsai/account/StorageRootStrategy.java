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
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.Optional;

/**
 * Owns the storage-root state and behavior of a {@link BonsaiAccount}.
 *
 * <p>The strategy holds the root value (for MPT) and exposes the RLP field encoding/decoding. The
 * MPT strategy writes/reads a 4th {@code storageRoot} field; the binary strategy writes/reads
 * nothing (3-field list). {@link BonsaiAccount} holds only a {@code StorageRootStrategy} reference
 * — no root data field of its own — so the root state lives entirely in the strategy.
 *
 * <p>The strategy is injected by the caller (world state / accumulator / codec, which know the
 * active {@code DataStorageFormat}); {@code BonsaiAccount} never probes the format itself.
 */
public interface StorageRootStrategy {

  /**
   * The held storage root.
   *
   * @return the held root for MPT.
   * @throws UnsupportedOperationException for binary (no storage root).
   */
  Hash getStorageRoot();

  /**
   * Updates the held storage root. Used by the MPT state-root committers.
   *
   * @param root the new storage root.
   * @throws UnsupportedOperationException for binary.
   */
  void setStorageRoot(Hash root);

  /**
   * Whether the held storage root is the empty-trie hash.
   *
   * @return {@code true} if the held root equals {@link Hash#EMPTY_TRIE_HASH}.
   * @throws UnsupportedOperationException for binary.
   */
  boolean isStorageEmpty();

  /**
   * Whether accounts using this strategy carry an MPT storage trie root.
   *
   * @return {@code true} for MPT, {@code false} for binary.
   */
  boolean hasStorageRoot();

  /**
   * Encodes the storage-root field into the RLP account list. MPT writes the 4th field; binary
   * writes nothing (the list stays at 3 fields).
   *
   * @param out the RLP output, positioned between {@code balance} and {@code codeHash}.
   */
  void writeStorageRoot(RLPOutput out);

  /**
   * Decodes the storage-root field from the RLP account list, storing it in this strategy. MPT
   * reads the 4th field; binary reads nothing and the list has no 4th field.
   *
   * <p>The {@code listSize} is the element count of the enclosing RLP list (as returned by {@link
   * RLPInput#enterList()}). The binary strategy uses it to detect a migrated MPT account (4-field
   * list {@code [nonce, balance, storageRoot, codeHash]}) and consume+ignore the legacy storageRoot
   * so the following {@code codeHash} read stays aligned; a native binary account uses a 3-field
   * list {@code [nonce, balance, codeHash]} and the strategy consumes nothing. The MPT strategy
   * always reads the storageRoot field.
   *
   * @param in the RLP input, positioned between {@code balance} and {@code codeHash}.
   * @param listSize the element count of the enclosing account RLP list.
   * @return the decoded storage root, or empty for binary (never {@code null}).
   */
  Optional<Hash> readStorageRoot(RLPInput in, int listSize);

  /**
   * Returns an independent copy of this strategy (the MPT copy holds its own mutable root; the
   * binary copy is stateless and may return itself).
   *
   * @return a copy of this strategy.
   */
  StorageRootStrategy copy();

  /**
   * Asserts this strategy's storage root matches the other account's storage root, throwing an
   * {@link IllegalStateException} if they differ. Used by account diffing.
   *
   * <p>The MPT strategy compares its held root against the other account's storage root (read via
   * {@link org.hyperledger.besu.datatypes.MptAccountValue#getStorageRoot()} or {@link
   * BonsaiAccount#getStorageRoot()}) and throws on mismatch (including an MPT-vs-binary mismatch,
   * detected when {@code other} is a binary {@link BonsaiAccount} or a non-MPT {@link
   * org.hyperledger.besu.datatypes.AccountValue}). The binary strategy is a no-op: a binary account
   * carries no storage root, so there is nothing to compare (binary account diffs do not assert on
   * storage root).
   *
   * @param other the account value to compare against.
   * @param context a description added to the thrown exception message.
   * @throws IllegalStateException if the two MPT storage roots differ, or an MPT account is
   *     compared against a binary account.
   */
  void assertStorageRootMatches(AccountValue other, String context);

  /**
   * Selects the strategy for the given storage format.
   *
   * @param dataStorageFormat the active {@code DataStorageFormat}.
   * @return {@link BinaryStorageRootStrategy#INSTANCE} for {@code BINARY}, otherwise a fresh {@link
   *     MptStorageRootStrategy} holding {@link Hash#EMPTY_TRIE_HASH}.
   */
  static StorageRootStrategy forFormat(final DataStorageFormat dataStorageFormat) {
    return dataStorageFormat == DataStorageFormat.BINARY
        ? BinaryStorageRootStrategy.INSTANCE
        : new MptStorageRootStrategy(Hash.EMPTY_TRIE_HASH);
  }
}
