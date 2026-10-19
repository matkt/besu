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

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BinaryTrieForkSupport;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes the PMT once a block past the PBT fork is finalized: no reorg can then bring the chain
 * back before the fork, so the PMT is never read again.
 *
 * <p>Runs once. The marker is written after the deletion, so a crash in between deletes again on
 * restart (the deletion is idempotent) and a restart after it skips the deletion.
 */
public class PmtPruner {
  private static final Logger LOG = LoggerFactory.getLogger(PmtPruner.class);

  private final BonsaiWorldStateKeyValueStorage storage;
  private final Blockchain blockchain;
  private final Optional<Long> binaryTrieMilestone;

  /** Set as soon as the deletion starts, so PMT reads are refused before the nodes go. */
  private volatile boolean pmtPruned;

  private volatile boolean done;
  private final AtomicBoolean checkQueued = new AtomicBoolean(false);
  private volatile ExecutorService executor;
  private final Object observerLock = new Object();
  private OptionalLong observerId = OptionalLong.empty();

  public PmtPruner(
      final BonsaiWorldStateKeyValueStorage storage,
      final Blockchain blockchain,
      final Optional<Long> binaryTrieMilestone) {
    this.storage = storage;
    this.blockchain = blockchain;
    this.binaryTrieMilestone = binaryTrieMilestone;
    this.pmtPruned = binaryTrieMilestone.isPresent() && storage.isPmtPruned();
    this.done = pmtPruned;
  }

  /** {@code true} once the PMT is deleted, or being deleted: no world state may be read from it. */
  public boolean isPmtPruned() {
    return pmtPruned;
  }

  /** Checks after each new block, on {@code executor}, until the PMT is deleted. */
  public void start(final ExecutorService executor) {
    if (binaryTrieMilestone.isEmpty() || done) {
      executor.shutdown();
      return;
    }
    this.executor = executor;
    synchronized (observerLock) {
      observerId = OptionalLong.of(blockchain.observeBlockAdded(event -> queueCheck()));
    }
    // On a restart, the fork may already be finalized.
    queueCheck();
  }

  /** Stops the checks; waits for a deletion in progress, so the storage is not closed under it. */
  public void stop() {
    done = true;
    removeObserver();
    if (executor == null) {
      return;
    }
    executor.shutdown();
    try {
      if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
        LOG.warn("PMT deletion still running at shutdown");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void queueCheck() {
    // Runs on the thread that added the block: only hands the check over.
    if (done || !checkQueued.compareAndSet(false, true)) {
      return;
    }
    try {
      executor.execute(
          () -> {
            checkQueued.set(false);
            if (done) {
              return;
            }
            try {
              if (pruneIfForkFinalized()) {
                done = true;
                removeObserver();
              }
            } catch (final RuntimeException e) {
              LOG.error("PMT deletion failed (will retry on the next block)", e);
            }
          });
    } catch (final RejectedExecutionException e) {
      checkQueued.set(false);
    }
  }

  private void removeObserver() {
    synchronized (observerLock) {
      observerId.ifPresent(blockchain::removeObserver);
      observerId = OptionalLong.empty();
    }
  }

  /** Deletes the PMT if a block past the fork is finalized; {@code true} once it is deleted. */
  synchronized boolean pruneIfForkFinalized() {
    if (storage.isPmtPruned()) {
      pmtPruned = true;
      return true;
    }
    // The chain has claimed the binary column, and no reorg can go back before the fork.
    if (!isForkFinalized() || !storage.isPbtMigratorRetired()) {
      return false;
    }
    pmtPruned = true;
    LOG.info("PBT fork finalized; deleting the PMT");
    storage.deletePatriciaTrie();
    storage.markPmtPruned();
    LOG.info("PMT deleted");
    return true;
  }

  private boolean isForkFinalized() {
    return blockchain
        .getFinalized()
        .flatMap(blockchain::getBlockHeader)
        .filter(
            header ->
                BinaryTrieForkSupport.isBinaryTrieActive(
                    header.getTimestamp(), binaryTrieMilestone))
        .isPresent();
  }
}
