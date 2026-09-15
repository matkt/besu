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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Immutable branch node with 16 child slots and an optional value. */
public final class BranchTreeNode implements ImmutableTreeNode {
  private final ImmutableTreeNode[] children;
  private final Bytes value;
  private final Bytes rlp;
  private final Bytes32 hash;

  public BranchTreeNode(final ImmutableTreeNode[] children, final Bytes value) {
    if (children.length != 16) {
      throw new IllegalArgumentException("Branch must have 16 children");
    }
    this.children = Arrays.copyOf(children, 16);
    this.value = value;
    final Bytes[] refs = new Bytes[16];
    for (int i = 0; i < 16; i++) {
      final ImmutableTreeNode child = this.children[i];
      refs[i] = child == null || child instanceof EmptyTreeNode ? null : TreeCodec.reference(child);
    }
    this.rlp = TreeCodec.encodeBranch(refs, value);
    this.hash = TreeCodec.hashOf(this.rlp);
  }

  public ImmutableTreeNode child(final int nibble) {
    return children[nibble];
  }

  public ImmutableTreeNode[] childrenArray() {
    return Arrays.copyOf(children, 16);
  }

  @Override
  public Optional<Bytes> value() {
    return value == null || value.isEmpty() ? Optional.empty() : Optional.of(value);
  }

  @Override
  public List<ImmutableTreeNode> children() {
    final List<ImmutableTreeNode> list = new ArrayList<>(16);
    for (final ImmutableTreeNode child : children) {
      if (child != null && !(child instanceof EmptyTreeNode)) {
        list.add(child);
      }
    }
    return list;
  }

  @Override
  public Bytes32 hash() {
    return hash;
  }

  @Override
  public Bytes rlp() {
    return rlp;
  }
}
