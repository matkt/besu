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
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateDiff.AccountChange;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.SyncStateStorage;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recovers the flat state after the chain reorganised below the pivot. With W the common ancestor,
 * the orphaned blocks are W+1..P and the canonical ones W+1..P'. Every entry falls in one of four
 * cases:
 *
 * <ul>
 *   <li>written by both forks, or by the canonical fork only: the canonical block access lists set
 *       its value at P';
 *   <li>written by the orphaned fork only: its value at P' is its value at W, fetched again at P',
 *       or removed if it does not exist there;
 *   <li>written by neither fork: already right.
 * </ul>
 *
 * <p>Accounts are compared field by field, so an account whose nonce only the orphaned fork changed
 * is fetched again even if the canonical fork changed its balance.
 */
public final class ReorgRecovery {

  private static final Logger LOG = LoggerFactory.getLogger(ReorgRecovery.class);
  private static final int ATTEMPTS = 10;
  private static final int REFETCH_BYTES = 4096;

  private final StateSync stateSync;
  private final SyncStateStorage storage;
  private final RequestSender sender;

  public ReorgRecovery(
      final StateSync stateSync, final SyncStateStorage storage, final RequestSender sender) {
    this.stateSync = stateSync;
    this.storage = storage;
    this.sender = sender;
  }

  /**
   * What must be fetched again at the new pivot.
   *
   * @param accounts accounts whose fields the canonical fork did not set, or owning such slots
   * @param slots slots written by the orphaned fork only, per account
   */
  record Plan(NavigableSet<Bytes32> accounts, Map<Bytes32, NavigableSet<Bytes32>> slots) {}

  /** Recovers to the canonical pivot {@code newPivot}. */
  public CompletableFuture<Void> recover(
      final BlockHeader newPivot, final StateDiff orphaned, final StateDiff canonical) {
    final Plan plan =
        stateSync.exclusively(
            () -> {
              final Plan computed = plan(orphaned, canonical, stateSync.coverage());
              StateDiffApplier.apply(canonical, storage, stateSync.coverage());
              stateSync.switchPivot(newPivot);
              return computed;
            });
    final StateSync.Pivot pivot = stateSync.pivot();
    LOG.info(
        "Recovering from a reorg below the pivot: refetching {} accounts and {} slots at block {}",
        plan.accounts().size(),
        plan.slots().values().stream().mapToInt(NavigableSet::size).sum(),
        newPivot.getNumber());
    final Map<Bytes32, Optional<Bytes32>> storageRoots = new ConcurrentHashMap<>();
    final List<CompletableFuture<Void>> accounts = new ArrayList<>();
    for (final Bytes32 account : plan.accounts()) {
      accounts.add(refetchAccount(pivot, account, storageRoots));
    }
    return CompletableFuture.allOf(accounts.toArray(CompletableFuture[]::new))
        .thenCompose(
            unused -> {
              final List<CompletableFuture<Void>> slots = new ArrayList<>();
              plan.slots()
                  .forEach(
                      (account, slotHashes) ->
                          storageRoots
                              .getOrDefault(account, Optional.empty())
                              .ifPresent(
                                  storageRoot ->
                                      slotHashes.forEach(
                                          slot ->
                                              slots.add(
                                                  refetchSlot(
                                                      pivot, account, storageRoot, slot)))));
              return CompletableFuture.allOf(slots.toArray(CompletableFuture[]::new));
            });
  }

  static Plan plan(
      final StateDiff orphaned, final StateDiff canonical, final StateCoverage coverage) {
    final NavigableSet<Bytes32> accounts = new TreeSet<>();
    final Map<Bytes32, NavigableSet<Bytes32>> slots = new TreeMap<>();
    for (final AccountChange old : orphaned.accounts().values()) {
      final Bytes32 hash = old.accountHash();
      if (!coverage.accounts().contains(hash)) {
        continue;
      }
      final Optional<AccountChange> canon = canonical.account(hash);
      final boolean fields =
          (old.nonce().isPresent() && canon.flatMap(AccountChange::nonce).isEmpty())
              || (old.balance().isPresent() && canon.flatMap(AccountChange::balance).isEmpty())
              || (old.code().isPresent() && canon.flatMap(AccountChange::code).isEmpty());
      final StorageTask task = coverage.storageTask(hash);
      final NavigableSet<Bytes32> orphanedSlots = new TreeSet<>();
      for (final Bytes32 slot : old.storage().keySet()) {
        final boolean rewritten = canon.map(c -> c.storage().containsKey(slot)).orElse(false);
        if (!rewritten && (task == null || task.covers(slot))) {
          orphanedSlots.add(slot);
        }
      }
      if (task != null && !old.storage().isEmpty()) {
        // its root was learned on the orphaned fork
        task.markRootStale();
      }
      if (fields || !orphanedSlots.isEmpty()) {
        accounts.add(hash);
      }
      if (!orphanedSlots.isEmpty()) {
        slots.put(hash, orphanedSlots);
      }
    }
    return new Plan(accounts, slots);
  }

  private CompletableFuture<Void> refetchAccount(
      final StateSync.Pivot pivot,
      final Bytes32 hash,
      final Map<Bytes32, Optional<Bytes32>> storageRoots) {
    final AccountRangeRequest request =
        new AccountRangeRequest(pivot, hash, hash, REFETCH_BYTES, "account_refetch");
    return sender
        .submitWithRetries(request, ATTEMPTS)
        .thenAccept(
            response -> {
              final AccountRangeRequest.Result result = response.value();
              final boolean exists =
                  !result.hashes().isEmpty() && result.hashes().getFirst().equals(hash);
              write(
                  pivot,
                  batch -> {
                    final StorageTask task = stateSync.coverage().storageTask(hash);
                    if (!exists) {
                      batch.removeAccount(hash);
                      storage.deleteStorage(hash);
                      stateSync.coverage().removeStorageTask(hash);
                      storageRoots.put(hash, Optional.empty());
                      return;
                    }
                    final Bytes account = result.accounts().getFirst();
                    final PmtStateTrieAccountValue value = Accounts.decode(account);
                    batch.putAccount(hash, account);
                    final Bytes32 storageRoot = Bytes32.wrap(value.getStorageRoot().getBytes());
                    if (value.getStorageRoot().equals(Hash.EMPTY_TRIE_HASH)) {
                      storage.deleteStorage(hash);
                      stateSync.coverage().removeStorageTask(hash);
                      storageRoots.put(hash, Optional.empty());
                    } else {
                      if (task != null) {
                        task.refreshRoot(storageRoot);
                      }
                      storageRoots.put(hash, Optional.of(storageRoot.copy()));
                    }
                  });
            });
  }

  private CompletableFuture<Void> refetchSlot(
      final StateSync.Pivot pivot,
      final Bytes32 account,
      final Bytes32 storageRoot,
      final Bytes32 slot) {
    final StorageRangesRequest request =
        new StorageRangesRequest(
            pivot, List.of(new StorageTask(account, storageRoot)), slot, slot, null, REFETCH_BYTES);
    return sender
        .submitWithRetries(request, ATTEMPTS)
        .thenAccept(
            response -> {
              final StorageRangesRequest.Slots slots = response.value().accounts().getFirst();
              final boolean exists =
                  !slots.hashes().isEmpty() && slots.hashes().getFirst().equals(slot);
              write(
                  pivot,
                  batch -> {
                    if (exists) {
                      batch.putStorage(
                          account, slot, Bytes32.leftPad(RLP.decodeOne(slots.values().getFirst())));
                    } else {
                      batch.removeStorage(account, slot);
                    }
                    if (stateSync.coverage().storageTask(account) == null) {
                      // a storage trie built for the downloaded slots no longer matches
                      storage
                          .getAccount(account)
                          .map(Accounts::decode)
                          .ifPresent(
                              value ->
                                  batch.putAccount(
                                      account,
                                      Accounts.encode(
                                          Accounts.withStorageRoot(
                                              value, Accounts.UNKNOWN_STORAGE_ROOT))));
                    }
                  });
            });
  }

  private void write(final StateSync.Pivot pivot, final Consumer<SyncStateStorage.Batch> writes) {
    final boolean written =
        stateSync.withPivot(
            pivot.epoch(),
            () -> {
              final SyncStateStorage.Batch batch = storage.batch();
              writes.accept(batch);
              batch.commit();
            });
    if (!written) {
      throw new IllegalStateException("the pivot moved during the reorg recovery");
    }
  }
}
