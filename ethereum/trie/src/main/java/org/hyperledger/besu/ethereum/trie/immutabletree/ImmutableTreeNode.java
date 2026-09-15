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
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Immutable hexary Patricia node. Instances are never mutated: any structural change returns a new
 * node. Unchanged siblings are structurally shared across roots.
 */
public sealed interface ImmutableTreeNode
    permits EmptyTreeNode,
        LeafTreeNode,
        ExtensionTreeNode,
        BranchTreeNode,
        StoredTreeNode {

  Bytes32 hash();

  Bytes rlp();

  /** True when this is only a disk reference and children/value are not yet materialized. */
  default boolean isStored() {
    return false;
  }

  default Optional<Bytes> value() {
    return Optional.empty();
  }

  default List<ImmutableTreeNode> children() {
    return List.of();
  }
}
