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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes32;

/** A registered root pointing at an immutable tree snapshot. */
public final class TreeRootHandle {
  private final Bytes32 rootHash;
  private final RootKind kind;
  private final TreeRole role;
  private volatile ImmutableTreeNode rootNode;
  private final AtomicInteger activeUsers = new AtomicInteger();
  private final AtomicLong lastAccessBlock = new AtomicLong();

  TreeRootHandle(
      final Bytes32 rootHash,
      final RootKind kind,
      final TreeRole role,
      final ImmutableTreeNode rootNode) {
    this.rootHash = Objects.requireNonNull(rootHash);
    this.kind = Objects.requireNonNull(kind);
    this.role = Objects.requireNonNull(role);
    this.rootNode = Objects.requireNonNull(rootNode);
  }

  public Bytes32 rootHash() {
    return rootHash;
  }

  public RootKind kind() {
    return kind;
  }

  public TreeRole role() {
    return role;
  }

  public ImmutableTreeNode rootNode() {
    return rootNode;
  }

  void replaceRootNode(final ImmutableTreeNode newRoot) {
    if (!newRoot.hash().equals(rootHash)) {
      throw new IllegalArgumentException(
          "Root node hash " + newRoot.hash() + " does not match handle " + rootHash);
    }
    this.rootNode = newRoot;
  }

  void touch(final long block) {
    lastAccessBlock.updateAndGet(prev -> Math.max(prev, block));
  }

  long lastAccessBlock() {
    return lastAccessBlock.get();
  }

  void markInUse() {
    activeUsers.incrementAndGet();
  }

  void markUnused() {
    final int remaining = activeUsers.decrementAndGet();
    if (remaining < 0) {
      activeUsers.set(0);
      throw new IllegalStateException("Root use count underflow for " + rootHash);
    }
  }

  boolean isInUse() {
    return activeUsers.get() > 0;
  }
}
