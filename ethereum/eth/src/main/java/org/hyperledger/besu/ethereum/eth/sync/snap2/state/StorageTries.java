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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateDiffApplier.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.StackTrie;
import org.hyperledger.besu.ethereum.rlp.RLP;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the storage trie of each account as soon as its storage is downloaded, while the rest of
 * the state downloads, so that the final build only redoes the storages changed since. The root
 * node is written last, and only if the account still has the storage root that was built, which is
 * checked exclusive with pivot moves: a move changing the slots marks the root unknown.
 */
final class StorageTries {

  private static final Logger LOG = LoggerFactory.getLogger(StorageTries.class);
  private static final int MAX_QUEUED = 1_000_000;
  private static final int SLOTS_PER_STOP_CHECK = 65_536;

  private final SyncStateStorage storage;
  private final Predicate<Runnable> withoutMoves;
  private final ExecutorService workers;
  private final LinkedBlockingQueue<Bytes32> queue = new LinkedBlockingQueue<>(MAX_QUEUED);
  private final AtomicLong built = new AtomicLong();
  private volatile boolean stopped;

  /**
   * Creates the builder.
   *
   * @param withoutMoves runs an action exclusive with pivot moves, false once the sync stopped
   */
  StorageTries(
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

  /** Queues an account whose storage is downloaded; when too many wait, the final build does it. */
  void completed(final Bytes32 accountHash) {
    if (!stopped) {
      queue.offer(accountHash.copy());
    }
  }

  /** Returns the number of storage tries built. */
  long built() {
    return built.get();
  }

  /** Returns the number of accounts waiting for their storage trie. */
  int queued() {
    return queue.size();
  }

  /** Stops building and waits for the builds in progress to give up. */
  void stop() {
    stopped = true;
    queue.clear();
    workers.shutdownNow();
    try {
      workers.awaitTermination(1, TimeUnit.MINUTES);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void run() {
    final SyncStateStorage.TrieWriter writer = storage.trieWriter();
    while (!stopped) {
      final Bytes32 account;
      try {
        account = queue.take();
      } catch (final InterruptedException e) {
        return;
      }
      try {
        build(account, writer);
      } catch (final CancellationException e) {
        return;
      } catch (final RuntimeException e) {
        LOG.debug("Storage trie of {} left to the final build: {}", account, e.getMessage());
      }
    }
  }

  private void build(final Bytes32 accountHash, final SyncStateStorage.TrieWriter writer) {
    final Optional<Hash> expected = storageRoot(accountHash);
    if (expected.isEmpty()
        || expected.get().equals(Hash.EMPTY_TRIE_HASH)
        || expected.get().equals(Accounts.UNKNOWN_STORAGE_ROOT)) {
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
    final long[] slots = {0};
    storage.forEachSlotValue(
        accountHash,
        (slot, value) -> {
          if (++slots[0] % SLOTS_PER_STOP_CHECK == 0 && stopped) {
            throw new CancellationException("storage tries stopped");
          }
          trie.addLeaf(slot, RLP.encodeOne(value.trimLeadingZeros()));
        });
    final Bytes32 root = trie.rootHash();
    writer.commit();
    if (!root.equals(expected.get().getBytes())) {
      LOG.debug(
          "Storage of {} gives root {} instead of {}, left to the final build",
          accountHash,
          root,
          expected.get());
      return;
    }
    final boolean running =
        withoutMoves.test(
            () -> {
              // a pivot move during the build may have changed the slots
              if (storageRoot(accountHash)
                  .map(current -> current.getBytes().equals(root))
                  .orElse(false)) {
                writer.storageNode(accountHash, Bytes.EMPTY, root, rootNode[0]);
                writer.commit();
                built.incrementAndGet();
              }
            });
    if (!running) {
      throw new CancellationException("storage tries stopped");
    }
  }

  private Optional<Hash> storageRoot(final Bytes32 accountHash) {
    return storage.getAccount(accountHash).map(Accounts::decode).map(a -> a.getStorageRoot());
  }
}
