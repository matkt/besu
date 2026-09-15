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

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Immutable extension node. */
public final class ExtensionTreeNode implements ImmutableTreeNode {
  private final Bytes path;
  private final ImmutableTreeNode child;
  private final Bytes rlp;
  private final Bytes32 hash;

  public ExtensionTreeNode(final Bytes path, final ImmutableTreeNode child) {
    this.path = path;
    this.child = child;
    this.rlp = TreeCodec.encodeExtension(path, TreeCodec.reference(child));
    this.hash = TreeCodec.hashOf(this.rlp);
  }

  public Bytes path() {
    return path;
  }

  /** @deprecated use {@link #path()} */
  @Deprecated
  public Bytes pathNibbles() {
    return path;
  }

  public ImmutableTreeNode child() {
    return child;
  }

  @Override
  public List<ImmutableTreeNode> children() {
    return List.of(child);
  }

  @Override
  public Bytes32 hash() {
    return hash;
  }

  @Override
  public Bytes rlp() {
    return rlp;
  }

  ExtensionTreeNode replacePath(final Bytes newPath) {
    if (newPath.isEmpty()) {
      throw new IllegalArgumentException("use replaceChild collapse for empty extension paths");
    }
    return new ExtensionTreeNode(newPath, child);
  }

  /**
   * Collapses this extension into the updated child (Besu {@code ExtensionNode#replaceChild}
   * semantics).
   */
  ImmutableTreeNode replaceChild(final ImmutableTreeNode updatedChild) {
    if (updatedChild instanceof EmptyTreeNode) {
      return EmptyTreeNode.INSTANCE;
    }
    final Bytes childPath =
        switch (updatedChild) {
          case LeafTreeNode leaf -> leaf.path();
          case ExtensionTreeNode ext -> ext.path();
          case BranchTreeNode ignored -> Bytes.EMPTY;
          default -> Bytes.EMPTY;
        };
    final Bytes combined = Bytes.concatenate(path, childPath);
    return switch (updatedChild) {
      case LeafTreeNode leaf -> leaf.replacePath(combined);
      case ExtensionTreeNode ext -> {
        if (combined.isEmpty()) {
          yield ext.child();
        }
        yield ext.replacePath(combined);
      }
      case BranchTreeNode branch -> {
        if (combined.isEmpty()) {
          yield branch;
        }
        yield branch.replacePath(combined);
      }
      default -> updatedChild;
    };
  }
}
