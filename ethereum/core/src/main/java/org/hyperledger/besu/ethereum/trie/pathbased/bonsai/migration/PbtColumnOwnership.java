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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration;

import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;

import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-writer handoff of the binary-trie column. Before the PBT fork the background {@link
 * PbtMigrator} writes it; from the first time the chain itself rolls or reads it in {@code BINARY}
 * mode (the fork transition), the chain owns it for good. The lock makes the claim wait for a write
 * of the migrator in flight, so the two never write the column at the same time.
 */
public final class PbtColumnOwnership {

  private static final Logger LOG = LoggerFactory.getLogger(PbtColumnOwnership.class);

  private final BonsaiWorldStateKeyValueStorage storage;
  private final ReentrantLock lock = new ReentrantLock();
  private volatile boolean ownedByChain;

  public PbtColumnOwnership(final BonsaiWorldStateKeyValueStorage storage) {
    this.storage = storage;
  }

  /** {@code true} once the chain has claimed the column, for good. */
  public boolean isClaimedByChain() {
    return ownedByChain;
  }

  /**
   * Runs one write of the migrator while it still owns the column; the migrator computes it without
   * the lock, so the chain waits for a write at most.
   *
   * @return {@code false}, without running it, once the chain has claimed the column
   */
  public boolean writeAsMigrator(final Runnable write) {
    lock.lock();
    try {
      if (ownedByChain) {
        return false;
      }
      write.run();
      return true;
    } finally {
      lock.unlock();
    }
  }

  /** Hands the column to the chain, for good; waits for a write of the migrator in flight. */
  public void claimForChain() {
    if (ownedByChain) {
      return;
    }
    if (!lock.tryLock()) {
      LOG.info("Waiting for the PBT migrator to finish its current write before using the PBT");
      lock.lock();
    }
    try {
      if (!ownedByChain) {
        ownedByChain = true;
        if (!storage.isPbtMigratorRetired()) {
          storage.markPbtMigratorRetired();
          LOG.info("Binary-trie column claimed by the chain; PBT migrator retired");
        }
      }
    } finally {
      lock.unlock();
    }
  }
}
