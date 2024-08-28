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

import org.hyperledger.besu.ethereum.trie.LocationNodeVisitor;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NullNode;

import org.apache.tuweni.bytes.Bytes;

public class NodeBatcherVisitor<V> implements LocationNodeVisitor<V> {

  private final MerkleTrieNodeBatcher<V> merkleTrieNodeBatcher;

  public NodeBatcherVisitor(final MerkleTrieNodeBatcher<V> merkleTrieNodeBatcher) {
    this.merkleTrieNodeBatcher = merkleTrieNodeBatcher;
  }

  @Override
  public void visit(final Bytes location, final ExtensionNode<V> extensionNode) {
    if (!extensionNode.isDirty()) {
      return;
    }

    final Node<V> child = extensionNode.getChild();
    if (child.isDirty()) {
      child.accept(Bytes.concatenate(location, extensionNode.getPath()), this);
    }

    maybeBatchNode(location, extensionNode);
  }

  @Override
  public void visit(final Bytes location, final BranchNode<V> branchNode) {
    if (!branchNode.isDirty()) {
      return;
    }

    for (int i = 0; i < branchNode.maxChild(); ++i) {
      Bytes index = Bytes.of(i);
      final Node<V> child = branchNode.child((byte) i);
      if (child.isDirty()) {
        child.accept(Bytes.concatenate(location, index), this);
      }
    }

    maybeBatchNode(location, branchNode);
  }

  @Override
  public void visit(final Bytes location, final LeafNode<V> leafNode) {
    if (!leafNode.isDirty()) {
      return;
    }

    maybeBatchNode(location, leafNode);
  }

  @Override
  public void visit(final Bytes location, final NullNode<V> nullNode) {}

  public void maybeBatchNode(final Bytes location, final Node<V> node) {
    merkleTrieNodeBatcher.addNodeToBatch(location, node);
  }
}
