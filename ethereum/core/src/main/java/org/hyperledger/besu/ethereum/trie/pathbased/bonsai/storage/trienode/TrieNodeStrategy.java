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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Defines the strategy for storing and retrieving account/storage trie branch nodes in key-value
 * storage. Implementations can use different key formats or storage segments; this is independent
 * of the flat-account/storage database strategies under {@code storage.flat}.
 */
public interface TrieNodeStrategy {

  Optional<Bytes> getFlatAccountTrieNode(
      Bytes location, Bytes32 nodeHash, SegmentedKeyValueStorage storage);

  Optional<Bytes> getFlatStorageTrieNode(
      Hash accountHash, Bytes location, Bytes32 nodeHash, SegmentedKeyValueStorage storage);

  /**
   * Batch variant of {@link #getFlatAccountTrieNode} / {@link #getFlatStorageTrieNode}. Results are
   * returned in request order and are not hash-verified.
   *
   * <p>The default loops over the single-node getters. Strategies whose keys are known up front
   * (path-keyed layouts) should override this with a single MultiGet.
   *
   * @param requests the nodes to load
   * @param storage the backing storage
   * @return the raw node bytes, in the same order as {@code requests}
   */
  default List<Optional<Bytes>> getFlatTrieNodes(
      final List<TrieNodeRequest> requests, final SegmentedKeyValueStorage storage) {
    final List<Optional<Bytes>> results = new ArrayList<>(requests.size());
    for (final TrieNodeRequest request : requests) {
      results.add(
          request.isAccountTrie()
              ? getFlatAccountTrieNode(request.location(), request.nodeHash(), storage)
              : getFlatStorageTrieNode(
                  request.accountHash(), request.location(), request.nodeHash(), storage));
    }
    return results;
  }

  void putFlatAccountTrieNode(
      SegmentedKeyValueStorage storage,
      SegmentedKeyValueStorageTransaction transaction,
      Bytes location,
      Bytes32 nodeHash,
      Bytes node);

  void putFlatStorageTrieNode(
      SegmentedKeyValueStorage storage,
      SegmentedKeyValueStorageTransaction transaction,
      Hash accountHash,
      Bytes location,
      Bytes32 nodeHash,
      Bytes node);

  void removeFlatAccountStateTrieNode(
      SegmentedKeyValueStorage storage,
      SegmentedKeyValueStorageTransaction transaction,
      Bytes location);

  default void onBeforeCommit(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction) {}

  default void onRollback(final SegmentedKeyValueStorageTransaction transaction) {}

  /**
   * A trie node to load.
   *
   * @param accountHash owning account for a storage-trie node, or {@code null} for the account trie
   * @param location the node's nibble path from its trie root
   * @param nodeHash the expected node hash
   */
  record TrieNodeRequest(Hash accountHash, Bytes location, Bytes32 nodeHash) {

    public static TrieNodeRequest account(final Bytes location, final Bytes32 nodeHash) {
      return new TrieNodeRequest(null, location, nodeHash);
    }

    public static TrieNodeRequest storage(
        final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
      return new TrieNodeRequest(accountHash, location, nodeHash);
    }

    public boolean isAccountTrie() {
      return accountHash == null;
    }
  }
}
