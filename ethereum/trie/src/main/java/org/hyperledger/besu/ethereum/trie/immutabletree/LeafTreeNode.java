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

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Immutable leaf node. Path includes the leaf terminator (Besu CompactEncoding convention). */
public final class LeafTreeNode implements ImmutableTreeNode {
  private final Bytes path;
  private final Bytes value;
  private final Bytes rlp;
  private final Bytes32 hash;

  public LeafTreeNode(final Bytes path, final Bytes value) {
    this.path = path;
    this.value = value;
    this.rlp = TreeCodec.encodeLeaf(path, value);
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

  @Override
  public Optional<Bytes> value() {
    return Optional.of(value);
  }

  @Override
  public Bytes32 hash() {
    return hash;
  }

  @Override
  public Bytes rlp() {
    return rlp;
  }

  LeafTreeNode replacePath(final Bytes newPath) {
    return new LeafTreeNode(newPath, value);
  }
}
