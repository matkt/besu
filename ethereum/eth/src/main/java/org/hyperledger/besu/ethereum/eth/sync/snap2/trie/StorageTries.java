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
package org.hyperledger.besu.ethereum.eth.sync.snap2.trie;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the storage tries of the downloaded storages, largest first, while the rest of the state
 * downloads. A move changing a storage marks its root unknown, so the final build redoes it.
 */
public final class StorageTries {

  private static final Logger LOG = LoggerFactory.getLogger(StorageTries.class);
  // past it the smallest are left to the final build, which reads them in one pass
  private static final int MAX_QUEUED = 100_000;
  // a build checks this often whether the sync stopped or a move changed the storage
  private static final int SLOTS_PER_CHECK = 65_536;

  private final SyncStateStorage storage;
  private final ExecutorService workers;
  // guarded by this
  private final TreeSet<Pending> queue =
      new TreeSet<>(
          Comparator.comparingLong(Pending::slots).reversed().thenComparing(Pending::accountHash));
  private final AtomicLong built = new AtomicLong();
  private final AtomicLong left = new AtomicLong();
  private final AtomicLong slots = new AtomicLong();
  private volatile boolean stopped;

  private record Pending(Bytes32 accountHash, Bytes32 storageRoot, long slots) {}

  public StorageTries(final SyncStateStorage storage, final int threads) {
    this.storage = storage;
    this.workers =
        Executors.newFixedThreadPool(
            threads, Thread.ofPlatform().name("snap2-storage-trie-", 0).daemon().factory());
    for (int i = 0; i < threads; i++) {
      workers.execute(this::run);
    }
  }

  /** Queues a downloaded storage, with the root its slots were verified against. */
  public synchronized void completed(
      final Bytes32 accountHash, final Bytes32 storageRoot, final long slots) {
    if (stopped) {
      return;
    }
    queue.add(new Pending(accountHash.copy(), storageRoot.copy(), slots));
    if (queue.size() > MAX_QUEUED) {
      queue.pollLast();
      left.incrementAndGet();
    }
    notify();
  }

  /** Returns the number of storage tries built. */
  public long built() {
    return built.get();
  }

  /** Returns the number of slots added to storage tries, those of abandoned builds included. */
  public long slots() {
    return slots.get();
  }

  /** Returns the number of storages waiting for their trie. */
  public synchronized int queued() {
    return queue.size();
  }

  /** Returns the number of storages left to the final build: too many queued, or changed. */
  public long left() {
    return left.get();
  }

  /** Stops building, once the builds in progress wrote what they built. */
  public void stop() {
    synchronized (this) {
      stopped = true;
      queue.clear();
      notifyAll();
    }
    workers.shutdownNow();
    try {
      if (!workers.awaitTermination(1, TimeUnit.MINUTES)) {
        LOG.warn("Storage trie builds still running a minute after they were stopped");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void run() {
    // its writes give way to the download's when the storage falls behind
    final SyncStateStorage.TrieWriter writer = storage.backgroundTrieWriter();
    try {
      for (Pending next = next(writer); next != null; next = next(writer)) {
        try {
          build(next, writer);
        } catch (final CancellationException e) {
          if (stopped) {
            return;
          }
          // a move changed the storage while it was built, the final build redoes it
          left.incrementAndGet();
        } catch (final RuntimeException e) {
          left.incrementAndGet();
          LOG.debug(
              "Storage trie of {} left to the final build: {}", next.accountHash(), e.getMessage());
        }
      }
    } catch (final InterruptedException e) {
      // stopped while waiting
    } finally {
      commit(writer);
    }
  }

  // the next storage to build, null once stopped; the batched writes are committed when idle
  private Pending next(final SyncStateStorage.TrieWriter writer) throws InterruptedException {
    synchronized (this) {
      if (!queue.isEmpty()) {
        return queue.pollFirst();
      }
    }
    commit(writer);
    synchronized (this) {
      while (!stopped && queue.isEmpty()) {
        wait();
      }
      return stopped ? null : queue.pollFirst();
    }
  }

  // a failed write leaves the storages batched in it to the final build
  private static void commit(final SyncStateStorage.TrieWriter writer) {
    try {
      writer.commit();
    } catch (final RuntimeException e) {
      LOG.debug("Storage tries left to the final build: {}", e.getMessage());
    }
  }

  private void build(final Pending pending, final SyncStateStorage.TrieWriter writer) {
    final Bytes32 accountHash = pending.accountHash();
    if (!Accounts.hasBuildableStorage(Hash.wrap(pending.storageRoot()))) {
      return;
    }
    final Bytes[] rootNode = new Bytes[1];
    final StackTrie trie =
        new StackTrie(
            (location, hash, rlp) -> {
              if (location.isEmpty()) {
                rootNode[0] = rlp;
              } else {
                writer.storageNode(accountHash, location, hash, rlp);
              }
            });
    final long[] count = {0};
    storage.forEachSlotValue(
        accountHash,
        (slot, value) -> {
          if (++count[0] % SLOTS_PER_CHECK == 0) {
            slots.addAndGet(SLOTS_PER_CHECK);
            if (stopped || changed(pending)) {
              throw new CancellationException("storage tries stopped or storage changed");
            }
          }
          trie.addLeaf(slot, RLP.encodeOne(value.trimLeadingZeros()));
        });
    slots.addAndGet(count[0] % SLOTS_PER_CHECK);
    final Bytes32 root = trie.rootHash();
    if (!root.equals(pending.storageRoot())) {
      left.incrementAndGet();
      LOG.debug(
          "Storage of {} gives root {} instead of {}, left to the final build",
          accountHash,
          root,
          pending.storageRoot());
      return;
    }
    // last, so that a root node is only found with the nodes below it
    writer.storageNode(accountHash, Bytes.EMPTY, root, rootNode[0]);
    built.incrementAndGet();
  }

  // a move changing the storage marks the root of its account unknown
  private boolean changed(final Pending pending) {
    return storage
        .getAccount(pending.accountHash())
        .map(
            account ->
                !Accounts.decode(account).getStorageRoot().getBytes().equals(pending.storageRoot()))
        .orElse(true);
  }
}
