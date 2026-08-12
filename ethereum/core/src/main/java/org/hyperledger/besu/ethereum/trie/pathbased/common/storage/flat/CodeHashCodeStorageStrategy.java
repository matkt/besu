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
package org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public class CodeHashCodeStorageStrategy implements CodeStorageStrategy {

  /**
   * Whether {@link #removeFlatCode} actually deletes the codeHash-keyed entry. Code is
   * content-addressed (EIP-8297) and may be shared across accounts, so for the MPT (BONSAI) it is
   * never removed from the flat DB on a per-account delete (sharing would be broken). For the
   * partitioned binary trie the committer only signals removal for a code hash newly introduced by
   * the rolled-back block (no remaining account references it), so the flat-DB entry is dropped to
   * stay consistent with the trie (whose CODE_ZONE chunks were removed).
   */
  private final boolean removeOnDelete;

  /** MPT (BONSAI) default: never remove content-addressed code from the flat DB. */
  public CodeHashCodeStorageStrategy() {
    this(DataStorageFormat.BONSAI);
  }

  public CodeHashCodeStorageStrategy(final DataStorageFormat dataStorageFormat) {
    this.removeOnDelete = dataStorageFormat == DataStorageFormat.BINARY;
  }

  @Override
  public Optional<Bytes> getFlatCode(
      final Hash codeHash, final Hash accountHash, final SegmentedKeyValueStorage storage) {
    return storage.get(CODE_STORAGE, codeHash.getBytes().toArrayUnsafe()).map(Bytes::wrap);
  }

  @Override
  public void putFlatCode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash codeHash,
      final Bytes code) {
    transaction.put(CODE_STORAGE, codeHash.getBytes().toArrayUnsafe(), code.toArrayUnsafe());
  }

  @Override
  public void removeFlatCode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash codeHash) {
    if (removeOnDelete) {
      transaction.remove(CODE_STORAGE, codeHash.getBytes().toArrayUnsafe());
    }
  }

  public static boolean isCodeHashValue(final byte[] key, final byte[] value) {
    final Hash valueHash = Hash.hash(Bytes.wrap(value));
    return Bytes.wrap(key).equals(valueHash.getBytes());
  }
}
