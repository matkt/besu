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

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Placeholder for a node that exists on disk but is not loaded in memory. Resolving it never
 * mutates this instance — the cache materializes a new concrete node and stores that instead.
 */
public final class StoredTreeNode implements ImmutableTreeNode {
  private final Bytes location;
  private final Bytes32 hash;

  public StoredTreeNode(final Bytes location, final Bytes32 hash) {
    this.location = location == null ? Bytes.EMPTY : location;
    this.hash = hash;
  }

  public Bytes location() {
    return location;
  }

  @Override
  public boolean isStored() {
    return true;
  }

  @Override
  public Bytes32 hash() {
    return hash;
  }

  @Override
  public Bytes rlp() {
    throw new IllegalStateException(
        "Stored node " + hash + " at " + location + " must be loaded before RLP access");
  }
}
