/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache;

import org.hyperledger.besu.ethereum.trie.MerkleTrie;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MerkleTrieProtector<K, V> {
  private final MerkleTrie<K, V> trie;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final AtomicBoolean frozen = new AtomicBoolean(false);

  public MerkleTrieProtector(final MerkleTrie<K, V> trie) {
    this.trie = trie;
  }

  public Optional<V> get(K key) {
    if (frozen.get()) {
      return Optional.empty();
    }
    lock.readLock().lock();
    try {
      if (frozen.get()) {
        return Optional.empty();
      }
      return trie.get(key);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void freeze() {
    lock.writeLock().lock();
    try {
      frozen.set(true);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public boolean isFrozen() {
    return frozen.get();
  }

  public boolean hasActiveReaders() {
    return lock.getReadLockCount() > 0;
  }

  public MerkleTrie<K, V> getTrie() {
    freeze();
    return trie;
  }
}
