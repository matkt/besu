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
import java.util.List;

/**
 * RAII-style lock over a tree traversal. Locked nodes (and the root) cannot be pruned until {@link
 * #close()}.
 */
public final class TreeTraversalLock implements AutoCloseable {
  private final TreeRootHandle rootHandle;
  private final List<CachedNodeRecord> lockedNodes = new ArrayList<>();
  private boolean closed;

  TreeTraversalLock(final TreeRootHandle rootHandle) {
    this.rootHandle = rootHandle;
    this.rootHandle.markInUse();
  }

  void lockNode(final CachedNodeRecord record) {
    if (closed) {
      throw new IllegalStateException("Traversal lock already closed");
    }
    record.lock();
    lockedNodes.add(record);
  }

  public TreeRootHandle root() {
    return rootHandle;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    for (int i = lockedNodes.size() - 1; i >= 0; i--) {
      lockedNodes.get(i).unlock();
    }
    lockedNodes.clear();
    rootHandle.markUnused();
  }
}
