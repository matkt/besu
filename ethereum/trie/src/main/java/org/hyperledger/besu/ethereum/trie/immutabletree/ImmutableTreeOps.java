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
package org.hyperledger.besu.ethereum.trie.immutabletree;

import org.hyperledger.besu.ethereum.trie.CompactEncoding;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Copy-on-write Patricia operations aligned with Besu {@code PutVisitor}/{@code GetVisitor}/{@code
 * RemoveVisitor} semantics (paths include the leaf terminator).
 */
public final class ImmutableTreeOps {

  @FunctionalInterface
  public interface NodeResolver {
    ImmutableTreeNode resolve(ImmutableTreeNode node);
  }

  private ImmutableTreeOps() {}

  public static Optional<Bytes> get(
      final ImmutableTreeNode root, final Bytes key, final NodeResolver resolver) {
    return getAt(resolver.resolve(root), TreeCodec.bytesToPath(key), resolver);
  }

  private static Optional<Bytes> getAt(
      final ImmutableTreeNode node, final Bytes path, final NodeResolver resolver) {
    final ImmutableTreeNode current = resolver.resolve(node);
    return switch (current) {
      case EmptyTreeNode ignored -> Optional.empty();
      case LeafTreeNode leaf -> {
        if (leaf.path().commonPrefixLength(path) == leaf.path().size()) {
          yield leaf.value();
        }
        yield Optional.empty();
      }
      case ExtensionTreeNode ext -> {
        final Bytes extPath = ext.path();
        final int common = extPath.commonPrefixLength(path);
        if (common < extPath.size()) {
          yield Optional.empty();
        }
        yield getAt(ext.child(), path.slice(common), resolver);
      }
      case BranchTreeNode branch -> {
        final byte childIndex = path.get(0);
        if (childIndex == CompactEncoding.LEAF_TERMINATOR) {
          yield branch.value();
        }
        yield getAt(branch.child(childIndex & 0xff), path.slice(1), resolver);
      }
      case StoredTreeNode stored ->
          throw new IllegalStateException("Resolver failed to materialize " + stored.hash());
    };
  }

  public static ImmutableTreeNode put(
      final ImmutableTreeNode root, final Bytes key, final Bytes value, final NodeResolver resolver) {
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("value must be non-empty; use remove for deletions");
    }
    return putAt(resolver.resolve(root), TreeCodec.bytesToPath(key), value, resolver);
  }

  private static ImmutableTreeNode putAt(
      final ImmutableTreeNode node,
      final Bytes path,
      final Bytes value,
      final NodeResolver resolver) {
    final ImmutableTreeNode current = resolver.resolve(node);
    return switch (current) {
      case EmptyTreeNode ignored -> new LeafTreeNode(path, value);
      case LeafTreeNode leaf -> putIntoLeaf(leaf, path, value);
      case ExtensionTreeNode ext -> putIntoExtension(ext, path, value, resolver);
      case BranchTreeNode branch -> putIntoBranch(branch, path, value, resolver);
      case StoredTreeNode stored ->
          throw new IllegalStateException("Resolver failed to materialize " + stored.hash());
    };
  }

  private static ImmutableTreeNode putIntoLeaf(
      final LeafTreeNode leaf, final Bytes path, final Bytes value) {
    final Bytes leafPath = leaf.path();
    final int common = leafPath.commonPrefixLength(path);
    if (common == leafPath.size() && common == path.size()) {
      return new LeafTreeNode(leafPath, value);
    }

    final byte newLeafIndex = path.get(common);
    final Bytes newLeafPath = path.slice(common + 1);
    final byte updatedLeafIndex = leafPath.get(common);
    final ImmutableTreeNode updatedLeaf = leaf.replacePath(leafPath.slice(common + 1));
    final ImmutableTreeNode newLeaf = new LeafTreeNode(newLeafPath, value);

    final BranchTreeNode branch =
        createBranch(updatedLeafIndex, updatedLeaf, newLeafIndex, newLeaf);
    if (common > 0) {
      return new ExtensionTreeNode(leafPath.slice(0, common), branch);
    }
    return branch;
  }

  private static ImmutableTreeNode putIntoExtension(
      final ExtensionTreeNode ext,
      final Bytes path,
      final Bytes value,
      final NodeResolver resolver) {
    final Bytes extensionPath = ext.path();
    final int common = extensionPath.commonPrefixLength(path);
    if (common == extensionPath.size()) {
      final ImmutableTreeNode newChild = putAt(ext.child(), path.slice(common), value, resolver);
      return ext.replaceChild(newChild);
    }

    final byte leafIndex = path.get(common);
    final Bytes leafPath = path.slice(common + 1);
    final byte extensionIndex = extensionPath.get(common);
    final Bytes remainingExt = extensionPath.slice(common + 1);
    final ImmutableTreeNode updatedExtension =
        remainingExt.isEmpty() ? resolver.resolve(ext.child()) : ext.replacePath(remainingExt);
    final ImmutableTreeNode leaf = new LeafTreeNode(leafPath, value);

    final BranchTreeNode branch =
        createBranch(leafIndex, leaf, extensionIndex, updatedExtension);
    if (common > 0) {
      return new ExtensionTreeNode(extensionPath.slice(0, common), branch);
    }
    return branch;
  }

  /**
   * Mirrors {@code DefaultNodeFactory#createBranch}: index {@link
   * CompactEncoding#LEAF_TERMINATOR} (16) places the node's value onto the branch.
   */
  private static BranchTreeNode createBranch(
      final byte leftIndex,
      final ImmutableTreeNode left,
      final byte rightIndex,
      final ImmutableTreeNode right) {
    final ImmutableTreeNode[] children = new ImmutableTreeNode[16];
    if ((leftIndex & 0xff) == CompactEncoding.LEAF_TERMINATOR) {
      children[rightIndex & 0x0f] = right;
      return new BranchTreeNode(children, left.value().orElse(null));
    }
    if ((rightIndex & 0xff) == CompactEncoding.LEAF_TERMINATOR) {
      children[leftIndex & 0x0f] = left;
      return new BranchTreeNode(children, right.value().orElse(null));
    }
    children[leftIndex & 0x0f] = left;
    children[rightIndex & 0x0f] = right;
    return new BranchTreeNode(children, null);
  }

  private static ImmutableTreeNode putIntoBranch(
      final BranchTreeNode branch,
      final Bytes path,
      final Bytes value,
      final NodeResolver resolver) {
    final byte childIndex = path.get(0);
    if (childIndex == CompactEncoding.LEAF_TERMINATOR) {
      return branch.replaceValue(value);
    }
    final ImmutableTreeNode updatedChild =
        putAt(branch.child(childIndex & 0xff), path.slice(1), value, resolver);
    return branch.replaceChild(childIndex & 0x0f, updatedChild);
  }

  public static ImmutableTreeNode remove(
      final ImmutableTreeNode root, final Bytes key, final NodeResolver resolver) {
    return removeAt(resolver.resolve(root), TreeCodec.bytesToPath(key), resolver);
  }

  private static ImmutableTreeNode removeAt(
      final ImmutableTreeNode node, final Bytes path, final NodeResolver resolver) {
    final ImmutableTreeNode current = resolver.resolve(node);
    return switch (current) {
      case EmptyTreeNode ignored -> EmptyTreeNode.INSTANCE;
      case LeafTreeNode leaf -> {
        if (leaf.path().commonPrefixLength(path) == leaf.path().size()) {
          yield EmptyTreeNode.INSTANCE;
        }
        yield leaf;
      }
      case ExtensionTreeNode ext -> {
        final Bytes extensionPath = ext.path();
        final int common = extensionPath.commonPrefixLength(path);
        if (common == extensionPath.size()) {
          final ImmutableTreeNode newChild =
              removeAt(ext.child(), path.slice(common), resolver);
          if (newChild instanceof EmptyTreeNode) {
            yield EmptyTreeNode.INSTANCE;
          }
          yield ext.replaceChild(newChild);
        }
        yield ext;
      }
      case BranchTreeNode branch -> {
        final byte childIndex = path.get(0);
        if (childIndex == CompactEncoding.LEAF_TERMINATOR) {
          yield branch.removeValue();
        }
        final ImmutableTreeNode updatedChild =
            removeAt(branch.child(childIndex & 0xff), path.slice(1), resolver);
        yield branch.replaceChild(childIndex & 0x0f, updatedChild, true);
      }
      case StoredTreeNode stored ->
          throw new IllegalStateException("Resolver failed to materialize " + stored.hash());
    };
  }
}
