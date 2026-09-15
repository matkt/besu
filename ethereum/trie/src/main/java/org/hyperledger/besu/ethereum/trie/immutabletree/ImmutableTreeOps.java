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

import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;

/**
 * Copy-on-write Patricia operations. Existing nodes are never mutated; every put returns a new root
 * while sharing unchanged branches with the previous root.
 */
public final class ImmutableTreeOps {

  @FunctionalInterface
  public interface NodeResolver {
    ImmutableTreeNode resolve(ImmutableTreeNode node);
  }

  private ImmutableTreeOps() {}

  public static Optional<Bytes> get(
      final ImmutableTreeNode root, final Bytes key, final NodeResolver resolver) {
    return getAt(resolver.resolve(root), TreeCodec.bytesToNibbles(key), resolver);
  }

  private static Optional<Bytes> getAt(
      final ImmutableTreeNode node, final Bytes path, final NodeResolver resolver) {
    final ImmutableTreeNode current = resolver.resolve(node);
    return switch (current) {
      case EmptyTreeNode ignored -> Optional.empty();
      case LeafTreeNode leaf -> {
        if (leaf.pathNibbles().equals(path)) {
          yield leaf.value();
        }
        yield Optional.empty();
      }
      case ExtensionTreeNode ext -> {
        final Bytes extPath = ext.pathNibbles();
        if (path.size() < extPath.size() || !path.slice(0, extPath.size()).equals(extPath)) {
          yield Optional.empty();
        }
        yield getAt(ext.child(), path.slice(extPath.size()), resolver);
      }
      case BranchTreeNode branch -> {
        if (path.isEmpty()) {
          yield branch.value();
        }
        final int nibble = path.get(0) & 0x0f;
        final ImmutableTreeNode child = branch.child(nibble);
        if (child == null || child instanceof EmptyTreeNode) {
          yield Optional.empty();
        }
        yield getAt(child, path.slice(1), resolver);
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
    return putAt(resolver.resolve(root), TreeCodec.bytesToNibbles(key), value, resolver);
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
    final Bytes leafPath = leaf.pathNibbles();
    final int common = TreeCodec.commonPrefixLength(leafPath, path);
    if (common == leafPath.size() && common == path.size()) {
      return new LeafTreeNode(leafPath, value);
    }
    if (common == leafPath.size()) {
      final ImmutableTreeNode[] children = emptyChildren();
      children[path.get(common) & 0x0f] = new LeafTreeNode(path.slice(common + 1), value);
      final BranchTreeNode branch = new BranchTreeNode(children, leaf.value().orElse(null));
      return wrapExtension(leafPath.slice(0, common), branch);
    }
    if (common == path.size()) {
      final ImmutableTreeNode[] children = emptyChildren();
      children[leafPath.get(common) & 0x0f] =
          new LeafTreeNode(leafPath.slice(common + 1), leaf.value().orElseThrow());
      final BranchTreeNode branch = new BranchTreeNode(children, value);
      return wrapExtension(path.slice(0, common), branch);
    }
    final ImmutableTreeNode[] children = emptyChildren();
    children[leafPath.get(common) & 0x0f] =
        new LeafTreeNode(leafPath.slice(common + 1), leaf.value().orElseThrow());
    children[path.get(common) & 0x0f] = new LeafTreeNode(path.slice(common + 1), value);
    final BranchTreeNode branch = new BranchTreeNode(children, null);
    return wrapExtension(path.slice(0, common), branch);
  }

  private static ImmutableTreeNode putIntoExtension(
      final ExtensionTreeNode ext,
      final Bytes path,
      final Bytes value,
      final NodeResolver resolver) {
    final Bytes extPath = ext.pathNibbles();
    final int common = TreeCodec.commonPrefixLength(extPath, path);
    if (common == extPath.size()) {
      final ImmutableTreeNode newChild =
          putAt(ext.child(), path.slice(common), value, resolver);
      return new ExtensionTreeNode(extPath, newChild);
    }
    final ImmutableTreeNode[] children = emptyChildren();
    if (common < extPath.size()) {
      final Bytes remainingExt = extPath.slice(common + 1);
      final ImmutableTreeNode oldChild =
          remainingExt.isEmpty()
              ? resolver.resolve(ext.child())
              : new ExtensionTreeNode(remainingExt, ext.child());
      children[extPath.get(common) & 0x0f] = oldChild;
    }
    if (common == path.size()) {
      final BranchTreeNode branch = new BranchTreeNode(children, value);
      return wrapExtension(path.slice(0, common), branch);
    }
    children[path.get(common) & 0x0f] = new LeafTreeNode(path.slice(common + 1), value);
    final BranchTreeNode branch = new BranchTreeNode(children, null);
    return wrapExtension(path.slice(0, common), branch);
  }

  private static ImmutableTreeNode putIntoBranch(
      final BranchTreeNode branch,
      final Bytes path,
      final Bytes value,
      final NodeResolver resolver) {
    if (path.isEmpty()) {
      return new BranchTreeNode(branch.childrenArray(), value);
    }
    final int nibble = path.get(0) & 0x0f;
    final ImmutableTreeNode[] children = branch.childrenArray();
    final ImmutableTreeNode existing = children[nibble];
    final ImmutableTreeNode base =
        existing == null || existing instanceof EmptyTreeNode
            ? EmptyTreeNode.INSTANCE
            : existing;
    children[nibble] = putAt(base, path.slice(1), value, resolver);
    return new BranchTreeNode(children, branch.value().orElse(null));
  }

  private static ImmutableTreeNode wrapExtension(
      final Bytes prefix, final ImmutableTreeNode child) {
    if (prefix.isEmpty()) {
      return child;
    }
    return new ExtensionTreeNode(prefix, child);
  }

  private static ImmutableTreeNode[] emptyChildren() {
    return new ImmutableTreeNode[16];
  }

  /** Applies a function while walking; useful for lock acquisition around traversal. */
  public static <T> T withResolvedRoot(
      final ImmutableTreeNode root,
      final NodeResolver resolver,
      final Function<ImmutableTreeNode, T> fn) {
    return fn.apply(resolver.resolve(root));
  }
}
