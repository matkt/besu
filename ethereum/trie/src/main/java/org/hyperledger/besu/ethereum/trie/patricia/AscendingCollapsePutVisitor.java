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

import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeFactory;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.StoredNode;

import java.util.ArrayList;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Ascending-key Patricia {@link PutVisitor} that collapses finished left siblings to {@link
 * StoredNode} hash stubs so live memory stays O(depth).
 *
 * <p>Keys MUST arrive in strictly ascending order. When the insert path takes a rightward nibble
 * under a branch (or splits past an existing left sibling), every completed left sibling is
 * replaced by a hash stub — that subtree will never receive another insert.
 *
 * <p>Node-split / put rules come from {@link PutVisitor}; this subclass only overrides the
 * extension hooks ({@link #beforeDescendChild}, {@link #mapAttachedSibling}, {@link
 * #validateSplitDirection}) plus duplicate-key rejection. Small embedded nodes ({@code
 * !isReferencedByHash()}) are left as-is — they are already O(1) in encoded size.
 */
public final class AscendingCollapsePutVisitor extends PutVisitor<Bytes> {

  public AscendingCollapsePutVisitor(final NodeFactory<Bytes> nodeFactory, final Bytes value) {
    super(nodeFactory, value);
  }

  @Override
  public Node<Bytes> visit(final LeafNode<Bytes> leafNode, final Bytes path) {
    final Bytes leafPath = leafNode.getPath();
    final int commonPathLength = leafPath.commonPrefixLength(path);
    if (commonPathLength == leafPath.size() && commonPathLength == path.size()) {
      throw new IllegalArgumentException("duplicate key insert");
    }
    return super.visit(leafNode, path);
  }

  @Override
  protected BranchNode<Bytes> beforeDescendChild(
      final BranchNode<Bytes> branchNode, final byte childIndex) {
    // Ascending inserts: every nibble slot left of childIndex is a completed left sibling.
    final ArrayList<Node<Bytes>> children = new ArrayList<>(branchNode.getChildren());
    boolean changed = false;
    for (int i = 0; i < Byte.toUnsignedInt(childIndex); i++) {
      final Node<Bytes> collapsed = collapse(children.get(i));
      if (collapsed != children.get(i)) {
        children.set(i, collapsed);
        changed = true;
      }
    }
    if (!changed) {
      return branchNode;
    }
    @SuppressWarnings("unchecked")
    final BranchNode<Bytes> rebuilt =
        (BranchNode<Bytes>) nodeFactory.createBranch(children, branchNode.getValue());
    return rebuilt;
  }

  @Override
  protected Node<Bytes> mapAttachedSibling(final Node<Bytes> sibling) {
    return collapse(sibling);
  }

  @Override
  protected void validateSplitDirection(final byte newLeafIndex, final byte siblingIndex) {
    if (Byte.toUnsignedInt(newLeafIndex) < Byte.toUnsignedInt(siblingIndex)) {
      throw new IllegalArgumentException("keys must be inserted in strictly ascending order");
    }
  }

  /**
   * Replaces a completed Patricia subtree with a {@link StoredNode} hash stub when the node is
   * hash-referenced. Does not require a backing DB — {@link StoredNode#getEncodedBytesRef()} /
   * {@link StoredNode#getHash()} work without loading.
   */
  private Node<Bytes> collapse(final Node<Bytes> node) {
    if (node instanceof NullNode || node instanceof StoredNode) {
      return node;
    }
    if (!node.isReferencedByHash()) {
      return node;
    }
    final Bytes32 hash = node.getHash();
    return new StoredNode<>(nodeFactory, Bytes.EMPTY, hash);
  }
}
