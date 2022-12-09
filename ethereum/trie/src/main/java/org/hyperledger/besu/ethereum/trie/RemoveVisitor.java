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
package org.hyperledger.besu.ethereum.trie;

import org.apache.tuweni.bytes.Bytes;

public class RemoveVisitor<V> implements PathNodeVisitor<V> {
  private final Node<V> NULL_NODE_RESULT = NullNode.instance();

  private final boolean allowFlatten;

  public RemoveVisitor() {
    this(true);
  }

  public RemoveVisitor(final boolean allowFlatten) {
    this.allowFlatten = allowFlatten;
  }

  @Override
  public Node<V> visit(final ExtensionNode<V> extensionNode, final Bytes path) {
    final Bytes extensionPath = extensionNode.getPath();

    System.out.println("remove 5" + extensionNode.getLocation() + " " + path);
    final int commonPathLength = extensionPath.commonPrefixLength(path);
    assert commonPathLength < path.size()
        : "Visiting path doesn't end with a non-matching terminator";

    if (commonPathLength == extensionPath.size()) {
      final Node<V> newChild = extensionNode.getChild().accept(this, path.slice(commonPathLength));
      final Node<V> updatedNode = extensionNode.replaceChild(newChild);
      if (!(updatedNode instanceof ExtensionNode)) {
        remove(extensionNode.getChild());
      }
    }

    // path diverges before the end of the extension, so it cannot match

    return extensionNode;
  }

  @Override
  public Node<V> visit(final BranchNode<V> branchNode, final Bytes path) {
    assert path.size() > 0 : "Visiting path doesn't end with a non-matching terminator";

    System.out.println(
        "remove 4" + branchNode.getLocation() + " " + path + " " + branchNode.getRlp());
    final byte childIndex = path.get(0);
    if (childIndex == CompactEncoding.LEAF_TERMINATOR) {
      final Node<V> updatedNode = branchNode.removeValue();
      if (!(updatedNode instanceof BranchNode)) {
        branchNode
            .getChildren()
            .forEach(
                child -> {
                  if (!(child instanceof NullNode)) {
                    remove(child);
                  }
                });
      }
      return updatedNode;
    }

    final Node<V> updatedChild = branchNode.child(childIndex).accept(this, path.slice(1));
    final Node<V> updatedNode = branchNode.replaceChild(childIndex, updatedChild, allowFlatten);
    if (!(updatedNode instanceof BranchNode)) {
      branchNode
          .getChildren()
          .forEach(
              child -> {
                if (!(child instanceof NullNode)) {
                  remove(child);
                }
              });
    }
    return updatedNode;
  }

  @Override
  public Node<V> visit(final LeafNode<V> leafNode, final Bytes path) {
    final Bytes leafPath = leafNode.getPath();
    final int commonPathLength = leafPath.commonPrefixLength(path);
    if (commonPathLength == leafPath.size()) {
      remove(leafNode);
      return NULL_NODE_RESULT;
    }
    return leafNode;
  }

  @Override
  public Node<V> visit(final NullNode<V> nullNode, final Bytes path) {
    return NULL_NODE_RESULT;
  }

  public void remove(final Node<V> node) {
    // nothing to do
  }
}
