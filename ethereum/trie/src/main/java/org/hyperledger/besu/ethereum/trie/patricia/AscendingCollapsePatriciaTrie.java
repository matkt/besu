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
package org.hyperledger.besu.ethereum.trie.patricia;

import static org.hyperledger.besu.ethereum.trie.CompactEncoding.bytesToPath;

import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NullNode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Streaming Merkle Patricia trie for strictly ascending bulk inserts.
 *
 * <p>Inserts with {@link AscendingCollapsePutVisitor} so completed left siblings collapse to {@link
 * org.hyperledger.besu.ethereum.trie.StoredNode} hash stubs — same shape as partitionedbinarytrie's
 * {@code AscendingCollapseBinaryTrie}. Keys MUST arrive in strictly ascending lexicographic order
 * (typically {@code keccak256(address)} / {@code keccak256(slotKey)}).
 *
 * <p>On {@link #rootHash()}, only the children of the root branch (under an optional root
 * extension) are hashed in parallel — nothing deeper.
 *
 * <p>Public API: {@link #insert}, {@link #insertCount}, {@link #rootHash}.
 */
public final class AscendingCollapsePatriciaTrie {

  private final DefaultNodeFactory<Bytes> nodeFactory =
      new DefaultNodeFactory<>(Function.identity());
  private Node<Bytes> root = NullNode.instance();
  private Bytes lastKey;
  private long insertCount;
  private boolean sealed;

  /**
   * Inserts {@code (key, value)} in strictly ascending key order.
   *
   * @throws IllegalArgumentException if order is violated or value is empty
   * @throws IllegalStateException if the trie has already been sealed
   */
  public void insert(final Bytes key, final Bytes value) {
    if (sealed) {
      throw new IllegalStateException("ascending Patricia trie already sealed");
    }
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("Patricia trie rejects empty values");
    }
    if (lastKey != null && key.compareTo(lastKey) <= 0) {
      throw new IllegalArgumentException("keys must be inserted in strictly ascending order");
    }
    lastKey = key;
    insertCount++;
    root = root.accept(new AscendingCollapsePutVisitor(nodeFactory, value), bytesToPath(key));
  }

  /** Returns the number of successful {@link #insert} calls since construction. */
  public long insertCount() {
    return insertCount;
  }

  /**
   * Seals the trie and returns its root hash. Further {@link #insert} calls are rejected.
   *
   * <p>Hashes the root branch's children in parallel (one level only), then hashes the root.
   *
   * @return merkle root of the sealed trie
   */
  public Bytes32 rootHash() {
    sealed = true;
    if (insertCount == 0) {
      return MerkleTrie.EMPTY_TRIE_NODE_HASH;
    }
    prehashRootBranchChildren(root);
    return root.getHash();
  }

  /**
   * Pre-computes hashes of the root branch's direct children concurrently. Unwraps a leading
   * extension so the branch under it is the parallelization point. Deeper nodes are left to the
   * normal recursive {@link Node#getHash()}.
   */
  private static void prehashRootBranchChildren(final Node<Bytes> root) {
    Node<Bytes> maybeBranch = root;
    if (maybeBranch instanceof ExtensionNode) {
      maybeBranch = ((ExtensionNode<Bytes>) maybeBranch).getChild();
    }
    if (!(maybeBranch instanceof BranchNode)) {
      return;
    }
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (final Node<Bytes> child : maybeBranch.getChildren()) {
      if (child instanceof NullNode) {
        continue;
      }
      futures.add(CompletableFuture.runAsync(child::getHash));
    }
    for (final CompletableFuture<Void> future : futures) {
      future.join();
    }
  }
}
