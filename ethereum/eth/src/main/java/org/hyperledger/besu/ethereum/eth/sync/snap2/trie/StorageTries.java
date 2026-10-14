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
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the storage tries of the downloaded storages, largest first, while the rest of the state
 * downloads, and applies to the built ones the slots the pivot moves change, before any new build.
 * A changed storage keeps an unknown root in its account until its trie gives the new one.
 */
public final class StorageTries {

  private static final Logger LOG = LoggerFactory.getLogger(StorageTries.class);
  // past it the smallest builds are left to the final build, which reads them in one pass
  private static final int MAX_QUEUED = 100_000;
  // a build checks this often whether it must stop
  private static final int SLOTS_PER_CHECK = 65_536;

  private final SyncStateStorage storage;
  private final Predicate<Runnable> withoutMoves;
  private final ExecutorService workers;
  // guarded by this: the jobs not started, updates in arrival order, builds largest first, and
  // the accounts a worker is on, which no other worker takes
  private final Map<Bytes32, Job> jobs = new HashMap<>();
  private final ArrayDeque<Bytes32> updates = new ArrayDeque<>();
  private final TreeSet<Job> builds =
      new TreeSet<>(
          Comparator.comparingLong((Job job) -> job.slots).reversed().thenComparing(Job::account));
  private final Set<Bytes32> inProgress = new HashSet<>();
  private final AtomicLong built = new AtomicLong();
  private final AtomicLong updated = new AtomicLong();
  private final AtomicLong left = new AtomicLong();
  private final AtomicLong slots = new AtomicLong();
  private volatile boolean finishing;
  private volatile boolean stopped;

  /** A storage to build from its slots, or a built trie to change, with the slots moves changed. */
  private static final class Job {
    private final Bytes32 account;
    private final long slots;
    // a build: the root its downloaded slots were verified against
    private final Bytes32 verifiedRoot;
    // an update: the root of the trie to change, null until the job before it gives it
    private Bytes32 base;
    private final Map<Bytes32, UInt256> changes = new HashMap<>();

    private Job(final Bytes32 account, final long slots, final Bytes32 verifiedRoot) {
      this.account = account;
      this.slots = slots;
      this.verifiedRoot = verifiedRoot;
    }

    private Bytes32 account() {
      return account;
    }

    private boolean isBuild() {
      return verifiedRoot != null;
    }
  }

  /**
   * Creates the builder.
   *
   * @param withoutMoves runs an action exclusive with pivot moves, false once the sync stopped
   */
  public StorageTries(
      final SyncStateStorage storage, final Predicate<Runnable> withoutMoves, final int threads) {
    this.storage = storage;
    this.withoutMoves = withoutMoves;
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
    if (stopped || finishing) {
      return;
    }
    final Job job = new Job(accountHash.copy(), slots, storageRoot.copy());
    jobs.put(job.account, job);
    builds.add(job);
    if (builds.size() > MAX_QUEUED) {
      jobs.remove(builds.pollLast().account);
      left.incrementAndGet();
    }
    notifyAll();
  }

  /**
   * Records slots a pivot move changed in a storage downloaded whole, whose account had the given
   * root before the move; moves call it exclusive with the other writes of the download.
   */
  public synchronized void changed(
      final Bytes32 accountHash, final Hash previousRoot, final Map<Bytes32, UInt256> changed) {
    if (stopped) {
      return;
    }
    Job job = jobs.get(accountHash);
    if (job == null) {
      final Bytes32 base;
      if (inProgress.contains(accountHash)) {
        // the job in progress gives it
        base = null;
      } else if (Accounts.UNKNOWN_STORAGE_ROOT.equals(previousRoot)) {
        // no trie to change: the final build makes it
        return;
      } else {
        base = Bytes32.wrap(previousRoot.getBytes());
      }
      job = new Job(accountHash.copy(), 0, null);
      job.base = base;
      jobs.put(job.account, job);
      updates.add(job.account);
      notifyAll();
    }
    for (final Map.Entry<Bytes32, UInt256> slot : changed.entrySet()) {
      job.changes.put(slot.getKey().copy(), slot.getValue());
    }
  }

  /** Returns the number of storage tries built. */
  public long built() {
    return built.get();
  }

  /** Returns the number of built storage tries changed by pivot moves. */
  public long updated() {
    return updated.get();
  }

  /** Returns the number of slots added to storage tries, those of abandoned builds included. */
  public long slots() {
    return slots.get();
  }

  /** Returns the number of storages waiting for their trie or for a change of it. */
  public synchronized int queued() {
    return jobs.size();
  }

  /** Returns the number of storages left to the final build. */
  public long left() {
    return left.get();
  }

  /** Applies the changes still waiting, gives up the builds, then stops. */
  public void finish() {
    synchronized (this) {
      finishing = true;
      left.addAndGet(builds.size());
      builds.forEach(job -> jobs.remove(job.account));
      builds.clear();
      // builds in progress give up at their next check
      while (!stopped && (!updates.isEmpty() || !inProgress.isEmpty())) {
        try {
          wait();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    stop();
  }

  /** Stops, once the jobs in progress wrote what they built. */
  public void stop() {
    synchronized (this) {
      stopped = true;
      jobs.clear();
      updates.clear();
      builds.clear();
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
    final SyncStateStorage.TrieWriter writer = storage.trieWriter();
    try {
      for (Job job = next(writer); job != null; job = next(writer)) {
        try {
          process(job, writer);
        } catch (final CancellationException e) {
          if (stopped) {
            return;
          }
          // a build the end of the download cut short
          left.incrementAndGet();
        } catch (final RuntimeException e) {
          left.incrementAndGet();
          LOG.debug("Storage trie of {} left to the final build: {}", job.account, e.getMessage());
        } finally {
          release(job.account);
        }
      }
    } catch (final InterruptedException e) {
      // stopped while waiting
    } finally {
      commit(writer);
    }
  }

  // the next job, null once stopped; the batched writes are committed when idle
  private Job next(final SyncStateStorage.TrieWriter writer) throws InterruptedException {
    synchronized (this) {
      final Job job = stopped ? null : take();
      if (job != null || stopped) {
        return job;
      }
    }
    commit(writer);
    synchronized (this) {
      while (!stopped) {
        final Job job = take();
        if (job != null) {
          return job;
        }
        wait();
      }
      return null;
    }
  }

  // updates first, never of an account a worker is on; then builds, unless finishing
  private Job take() {
    for (final Iterator<Bytes32> accounts = updates.iterator(); accounts.hasNext(); ) {
      final Bytes32 account = accounts.next();
      if (!inProgress.contains(account)) {
        accounts.remove();
        inProgress.add(account);
        return jobs.remove(account);
      }
    }
    final Job build = finishing ? null : builds.pollFirst();
    if (build != null) {
      jobs.remove(build.account);
      inProgress.add(build.account);
    }
    return build;
  }

  // a move records its changes before writing them, so slots read changed have a next job
  private synchronized boolean changedSince(final Bytes32 account) {
    return jobs.containsKey(account);
  }

  private synchronized void release(final Bytes32 account) {
    inProgress.remove(account);
    notifyAll();
  }

  // a failed write leaves the storages batched in it to the final build
  private static void commit(final SyncStateStorage.TrieWriter writer) {
    try {
      writer.commit();
    } catch (final RuntimeException e) {
      LOG.debug("Storage tries left to the final build: {}", e.getMessage());
    }
  }

  private void process(final Job job, final SyncStateStorage.TrieWriter writer) {
    final Bytes32 root;
    if (job.isBuild()) {
      final Bytes32 builtRoot = build(job, writer);
      if (builtRoot == null) {
        return;
      }
      // applied again if already read, as a move may have changed the slots during the build
      root = job.changes.isEmpty() ? builtRoot : change(job.account, builtRoot, job, writer);
      built.incrementAndGet();
    } else {
      if (job.base == null || !hasTrie(job.account, job.base)) {
        // never built, or its build gave up
        return;
      }
      root = change(job.account, job.base, job, writer);
      updated.incrementAndGet();
    }
    publish(job, root, writer);
  }

  // the root built from the slots, null when they do not give the root they were verified against
  private Bytes32 build(final Job job, final SyncStateStorage.TrieWriter writer) {
    final Bytes32 accountHash = job.account;
    if (!Accounts.hasBuildableStorage(Hash.wrap(job.verifiedRoot))) {
      return null;
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
            if (stopped || finishing) {
              throw new CancellationException("storage tries stopped");
            }
          }
          trie.addLeaf(slot, RLP.encodeOne(value.trimLeadingZeros()));
        });
    slots.addAndGet(count[0] % SLOTS_PER_CHECK);
    final Bytes32 root = trie.rootHash();
    // slots a move changed during the build give another root, until they are applied again
    if (job.changes.isEmpty() && !root.equals(job.verifiedRoot) && !changedSince(accountHash)) {
      left.incrementAndGet();
      LOG.debug(
          "Storage of {} gives root {} instead of {}, left to the final build",
          accountHash,
          root,
          job.verifiedRoot);
      return null;
    }
    // last, so that a root node is only found with the nodes below it
    if (rootNode[0] != null) {
      writer.storageNode(accountHash, Bytes.EMPTY, root, rootNode[0]);
    }
    return root;
  }

  // applies the changed slots to a stored trie, as a block import does, and returns its new root
  private Bytes32 change(
      final Bytes32 accountHash,
      final Bytes32 base,
      final Job job,
      final SyncStateStorage.TrieWriter writer) {
    // the nodes of the base are read back
    writer.commit();
    final StoredMerklePatriciaTrie<Bytes32, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) -> storage.storageTrieNode(accountHash, location, hash),
            base,
            Function.identity(),
            Function.identity());
    job.changes.forEach(
        (slot, value) -> {
          if (value.isZero()) {
            trie.remove(slot);
          } else {
            trie.put(slot, RLP.encodeOne(value.toMinimalBytes()));
          }
        });
    trie.commit((location, hash, node) -> writer.storageNode(accountHash, location, hash, node));
    return trie.getRootHash();
  }

  private boolean hasTrie(final Bytes32 accountHash, final Bytes32 root) {
    return root.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)
        || storage.hasStorageTrie(accountHash, root);
  }

  // commits the trie, then gives its root to the next job of the account, or to the account
  private void publish(
      final Job job, final Bytes32 root, final SyncStateStorage.TrieWriter writer) {
    writer.commit();
    final boolean running =
        withoutMoves.test(
            () -> {
              final Job next;
              synchronized (this) {
                next = jobs.get(job.account);
                if (next != null) {
                  next.base = root;
                }
              }
              // a move left the account with an unknown root
              if (next == null && (!job.isBuild() || !job.changes.isEmpty())) {
                writeRoot(job.account, root);
              }
            });
    if (!running) {
      throw new CancellationException("storage tries stopped");
    }
  }

  // an account a move removed gets no root
  private void writeRoot(final Bytes32 accountHash, final Bytes32 root) {
    storage
        .getAccount(accountHash)
        .map(Accounts::decode)
        .ifPresent(
            account -> {
              final SyncStateStorage.Batch batch = storage.batch();
              batch.putAccount(
                  accountHash, Accounts.encode(Accounts.withStorageRoot(account, Hash.wrap(root))));
              batch.commit();
            });
  }
}
