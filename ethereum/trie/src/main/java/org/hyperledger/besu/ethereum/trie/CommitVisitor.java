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
package org.hyperledger.besu.ethereum.trie;

import org.hyperledger.besu.ethereum.trie.patricia.BranchNode;
import org.hyperledger.besu.ethereum.trie.patricia.ExtensionNode;
import org.hyperledger.besu.ethereum.trie.patricia.LeafNode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import org.apache.tuweni.bytes.Bytes;

public class CommitVisitor<V> implements LocationNodeVisitor<V> {
  private static final ForkJoinPool FORK_JOIN_POOL =
      new ForkJoinPool(Runtime.getRuntime().availableProcessors());

  protected final NodeUpdater nodeUpdater;

  public CommitVisitor(final NodeUpdater nodeUpdater) {
    this.nodeUpdater = nodeUpdater;
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

    maybeStoreNode(location, extensionNode);
  }

  @Override
  public void visit(final Bytes location, final BranchNode<V> branchNode) {
    if (!branchNode.isDirty()) {
      return;
    }
    if (location.isEmpty()) {
      FORK_JOIN_POOL.invoke(new ComputeNode(this, branchNode, location, null));
    } else {
      visitBranch(location, branchNode);
    }
  }

  private void visitBranch(final Bytes location, final BranchNode<V> branchNode) {
    final List<ComputeNode> tasks = new ArrayList<>();
    for (int i = 0; i < branchNode.maxChild(); ++i) {
      Bytes index = Bytes.of(i);
      final Node<V> child = branchNode.child((byte) i);
      if (child.isDirty()) {
        tasks.add(new ComputeNode(this, child, location, index));
      }
    }

    tasks.forEach(ComputeNode::fork);
    tasks.forEach(ComputeNode::join);

    maybeStoreNode(location, branchNode);
  }

  @Override
  public void visit(final Bytes location, final LeafNode<V> leafNode) {
    if (!leafNode.isDirty()) {
      return;
    }

    maybeStoreNode(location, leafNode);
  }

  @Override
  public void visit(final Bytes location, final NullNode<V> nullNode) {}

  public void maybeStoreNode(final Bytes location, final Node<V> node) {
    final Bytes nodeRLP = node.getEncodedBytes();
    if (nodeRLP.size() >= 32) {
      this.nodeUpdater.store(location, node.getHash(), nodeRLP);
    }
  }

  public class ComputeNode extends RecursiveAction {

    private final CommitVisitor<V> commitVisitor;
    private final Node<V> child;
    private final Bytes location;
    private final Bytes index;

    public ComputeNode(
        final CommitVisitor<V> commitVisitor,
        final Node<V> child,
        final Bytes location,
        final Bytes index) {
      this.commitVisitor = commitVisitor;
      this.child = child;
      this.location = location;
      this.index = index;
    }

    @Override
    protected void compute() {
      if (location.isEmpty() && index == null) {
        visitBranch(location, (BranchNode<V>) child);
      } else {
        child.accept(Bytes.concatenate(location, index), commitVisitor);
      }
    }
  }
}
