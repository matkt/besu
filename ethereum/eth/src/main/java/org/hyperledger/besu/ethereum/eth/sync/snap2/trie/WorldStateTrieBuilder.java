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
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the account trie and the storage tries from the flat state, in parallel over the 256
 * first-byte partitions of the account key space, and fixes the storage roots of the flat accounts.
 * A storage trie already written for the storage root of its account, while downloading, is kept
 * and its slots are not read.
 */
public final class WorldStateTrieBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(WorldStateTrieBuilder.class);
  private static final int PARTITIONS = 256;
  private static final int ACCOUNTS_PER_COMMIT = 10_000;
  private static final int SLOTS_PER_CANCEL_CHECK = 65_536;

  private final SyncStateStorage storage;
  private final int threads;
  private final AtomicLong accounts = new AtomicLong();
  private final AtomicLong slots = new AtomicLong();
  private final AtomicLong keptStorageTries = new AtomicLong();
  private final AtomicBoolean cancelled = new AtomicBoolean();
  private final AtomicReference<RuntimeException> failure = new AtomicReference<>();

  public WorldStateTrieBuilder(final SyncStateStorage storage, final int threads) {
    this.storage = storage;
    this.threads = Math.max(1, threads);
  }

  /**
   * Result of a build.
   *
   * @param root the state root
   * @param rootNode the encoding of the root node
   * @param accounts the number of accounts
   * @param slots the number of storage slots read to build tries
   * @param keptStorageTries the number of storage tries built while downloading and kept
   */
  public record Result(
      Bytes32 root, Bytes rootNode, long accounts, long slots, long keptStorageTries) {}

  /** Returns the number of accounts processed so far. */
  public long accountsProcessed() {
    return accounts.get();
  }

  /** Returns the number of slots processed so far. */
  public long slotsProcessed() {
    return slots.get();
  }

  /** Stops a running build, which then throws a {@link CancellationException}. */
  public void cancel() {
    cancelled.set(true);
  }

  @VisibleForTesting
  boolean isCancelled() {
    return cancelled.get();
  }

  /** Builds the tries and writes their nodes. */
  public Result build() {
    final ExecutorService executor =
        Executors.newFixedThreadPool(
            threads, Thread.ofPlatform().name("snap2-trie-builder-", 0).daemon().factory());
    try {
      final List<Future<StackTrie.Item>> parts = new ArrayList<>(PARTITIONS);
      for (int prefix = 0; prefix < PARTITIONS; prefix++) {
        final int p = prefix;
        parts.add(executor.submit(() -> buildPartition(p)));
      }
      final SyncStateStorage.TrieWriter writer = storage.trieWriter();
      final Bytes[] rootNode = {MerkleTrie.EMPTY_TRIE_NODE};
      final StackTrie top =
          new StackTrie(
              (location, hash, rlp) -> {
                writer.accountNode(location, hash, rlp);
                if (location.isEmpty()) {
                  rootNode[0] = rlp;
                }
              });
      for (final Future<StackTrie.Item> part : parts) {
        final StackTrie.Item item = part.get();
        if (item != null) {
          top.addItem(item);
        }
      }
      final Bytes32 root = top.rootHash();
      writer.commit();
      return new Result(root, rootNode[0], accounts.get(), slots.get(), keptStorageTries.get());
    } catch (final InterruptedException e) {
      cancel();
      Thread.currentThread().interrupt();
      throw new CancellationException("trie build interrupted");
    } catch (final ExecutionException e) {
      cancel();
      final RuntimeException first = failure.get();
      throw first != null ? first : new IllegalStateException("trie build failed", e.getCause());
    } finally {
      executor.shutdownNow();
      awaitPartitions(executor);
    }
  }

  // no partition writes once the build returned
  private static void awaitPartitions(final ExecutorService executor) {
    try {
      if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
        LOG.warn("Trie build partitions still running a minute after the build stopped");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private StackTrie.Item buildPartition(final int prefix) {
    final byte[] from = new byte[32];
    final byte[] to = RangeManager.MAX_RANGE.toArray();
    from[0] = (byte) prefix;
    to[0] = (byte) prefix;
    final SyncStateStorage.TrieWriter writer = storage.partitionTrieWriter();
    try {
      final PartitionVisitor visitor = new PartitionVisitor(writer);
      storage.scan(Bytes32.wrap(from), Bytes32.wrap(to), visitor);
      visitor.finishAccount();
      final StackTrie.Item item = visitor.accountTrie.finishAsItem();
      visitor.commitFixes();
      writer.commit();
      return item;
    } catch (final RuntimeException e) {
      writer.abort();
      // the other partitions stop too, and the build reports the first failure
      failure.compareAndSet(null, e);
      cancel();
      throw e;
    }
  }

  private final class PartitionVisitor implements SyncStateStorage.ScanVisitor {
    private final SyncStateStorage.TrieWriter writer;
    private final StackTrie accountTrie;
    private SyncStateStorage.Batch fixes;
    private int pendingFixes;
    private Bytes32 accountHash;
    private Bytes account;
    private StackTrie storageTrie;
    private long partitionSlots;

    private PartitionVisitor(final SyncStateStorage.TrieWriter writer) {
      this.writer = writer;
      this.accountTrie = new StackTrie(writer::accountNode);
    }

    @Override
    public boolean onAccount(final Bytes32 hash, final Bytes rlp) {
      finishAccount();
      if (cancelled.get()) {
        throw new CancellationException("trie build cancelled");
      }
      final Hash storageRoot = Accounts.decode(rlp).getStorageRoot();
      if (Accounts.hasBuildableStorage(storageRoot)
          && storage.hasStorageTrie(hash, Bytes32.wrap(storageRoot.getBytes()))) {
        accountTrie.addLeaf(hash, rlp);
        accounts.incrementAndGet();
        keptStorageTries.incrementAndGet();
        return false;
      }
      accountHash = hash;
      account = rlp;
      storageTrie =
          new StackTrie(
              (location, nodeHash, node) -> writer.storageNode(hash, location, nodeHash, node));
      return true;
    }

    @Override
    public void onSlot(final Bytes32 slotAccount, final Bytes32 slotHash, final Bytes value) {
      if (!slotAccount.equals(accountHash)) {
        LOG.debug("Dropping slot {} of missing account {}", slotHash, slotAccount);
        fixes().removeStorage(slotAccount, slotHash);
        return;
      }
      if (++partitionSlots % SLOTS_PER_CANCEL_CHECK == 0 && cancelled.get()) {
        throw new CancellationException("trie build cancelled");
      }
      storageTrie.addLeaf(slotHash, RLP.encodeOne(value.trimLeadingZeros()));
      slots.incrementAndGet();
    }

    private void finishAccount() {
      if (accountHash == null) {
        return;
      }
      final Bytes32 storageRoot = storageTrie.rootHash();
      final PmtStateTrieAccountValue value = Accounts.decode(account);
      Bytes leaf = account;
      if (!value.getStorageRoot().getBytes().equals(storageRoot)) {
        leaf = Accounts.encode(Accounts.withStorageRoot(value, Hash.wrap(storageRoot)));
        fixes().putAccount(accountHash, leaf);
      }
      accountTrie.addLeaf(accountHash, leaf);
      accounts.incrementAndGet();
      accountHash = null;
      storageTrie = null;
    }

    private SyncStateStorage.Batch fixes() {
      if (fixes == null) {
        fixes = storage.batch();
      }
      if (++pendingFixes >= ACCOUNTS_PER_COMMIT) {
        commitFixes();
        fixes = storage.batch();
      }
      return fixes;
    }

    private void commitFixes() {
      if (fixes != null) {
        fixes.commit();
        fixes = null;
        pendingFixes = 0;
      }
    }
  }
}
