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

/** In-memory bookkeeping for a cached immutable node. */
final class CachedNodeRecord {
  private volatile ImmutableTreeNode node;
  private final AtomicInteger lockCount = new AtomicInteger();
  private final AtomicLong lastAccessBlock = new AtomicLong();

  CachedNodeRecord(final ImmutableTreeNode node, final long block) {
    this.node = Objects.requireNonNull(node);
    this.lastAccessBlock.set(block);
  }

  ImmutableTreeNode node() {
    return node;
  }

  void replaceMaterialized(final ImmutableTreeNode materialized, final long block) {
    if (materialized.isStored()) {
      throw new IllegalArgumentException("Cannot cache a stored placeholder as materialized");
    }
    this.node = materialized;
    touch(block);
  }

  void touch(final long block) {
    lastAccessBlock.updateAndGet(prev -> Math.max(prev, block));
  }

  long lastAccessBlock() {
    return lastAccessBlock.get();
  }

  void lock() {
    lockCount.incrementAndGet();
  }

  void unlock() {
    final int remaining = lockCount.decrementAndGet();
    if (remaining < 0) {
      lockCount.set(0);
      throw new IllegalStateException("Node unlock underflow for " + node.hash());
    }
  }

  boolean isLocked() {
    return lockCount.get() > 0;
  }

  Bytes32 hash() {
    return node.hash();
  }
}
