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
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.sync.snap2.Snap2Configuration;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateDiffApplier.Accounts;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads the flat state at the current pivot: account ranges in parallel chunks, storage in
 * multi-account batches or, for large storages, in parallel slot chunks, and bytecodes.
 *
 * <p>Every response is verified against the root it was requested at and persisted only if the
 * pivot did not move meanwhile; a pivot move applies the block access lists of the skipped blocks
 * to what is persisted, under an exclusive lock. Bookkeeping runs on a single thread.
 */
public final class StateSync {

  private static final Logger LOG = LoggerFactory.getLogger(StateSync.class);
  private static final int REFRESH_BYTES = 4096;
  private static final int EMPTY_RESPONSES_FOR_STALE_PIVOT = 12;
  private static final int NO_EPOCH = -1;
  private static final int SEEN_CODES_LIMIT = 200_000;

  /** Current pivot; responses requested at another epoch are discarded. */
  public record Pivot(int epoch, BlockHeader header) {
    Bytes32 root() {
      return Bytes32.wrap(header.getStateRoot().getBytes());
    }
  }

  private final SyncStateStorage storage;
  private final RequestSender scheduler;
  private final Snap2Configuration config;
  private final StateCoverage coverage = new StateCoverage();
  private final ReentrantReadWriteLock pivotLock = new ReentrantReadWriteLock();
  private final ScheduledExecutorService loop;
  private final CompletableFuture<Void> completion = new CompletableFuture<>();
  private volatile Pivot pivot;
  private volatile boolean pivotStale;
  private volatile boolean stopped;
  private volatile boolean paused;

  // bookkeeping, loop thread only
  private final List<RangeTask> accountTasks = new ArrayList<>();
  private final ArrayDeque<StorageTask> storageQueue = new ArrayDeque<>();
  private final ArrayDeque<StorageTask> refreshQueue = new ArrayDeque<>();
  private final Set<StorageTask> chunked = new LinkedHashSet<>();
  private final LinkedHashSet<Bytes32> codeQueue = new LinkedHashSet<>();
  private final Set<Bytes32> codesInFlight = new HashSet<>();
  private final Set<Bytes32> seenCodes = new HashSet<>();
  private final Set<EthPeer> emptyPeers = new HashSet<>();
  private final StateServers servers = new StateServers();
  private final StorageTries tries;
  // peers left out of state requests, refreshed on each dispatch
  private Set<EthPeer> excluded = Set.of();
  private volatile int excludedCount;
  private int accountsInFlight;
  private int storageInFlight;
  private int codesRequestsInFlight;
  private int refreshesInFlight;
  private int consecutiveEmpty;

  private final AtomicLong accounts = new AtomicLong();
  private final AtomicLong slots = new AtomicLong();
  private final AtomicLong codes = new AtomicLong();

  public StateSync(
      final SyncStateStorage storage,
      final RequestSender scheduler,
      final Snap2Configuration config) {
    this.storage = storage;
    this.scheduler = scheduler;
    this.config = config;
    this.loop =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("snap2-state").daemon().factory());
    this.tries =
        new StorageTries(storage, this::withoutMoves, Math.max(1, config.trieBuildThreads() / 4));
  }

  /** Starts downloading the state of the given pivot. */
  public CompletableFuture<Void> start(final BlockHeader pivotHeader) {
    pivot = new Pivot(0, pivotHeader);
    execute(
        () -> {
          for (final Bytes32[] range : Keys.split(Keys.MIN, Keys.MAX, config.accountChunks())) {
            accountTasks.add(new RangeTask(range[0], range[1]));
          }
          LOG.info(
              "Starting SNAP2 world state download at block {} with {} account ranges",
              pivotHeader.getNumber(),
              accountTasks.size());
          dispatch();
        });
    loop.scheduleWithFixedDelay(this::dispatchSafely, 500, 500, TimeUnit.MILLISECONDS);
    return completion;
  }

  /** Returns the current pivot. */
  public Pivot pivot() {
    return pivot;
  }

  /** Returns what the flat state holds. */
  public StateCoverage coverage() {
    return coverage;
  }

  /** Returns whether peers stopped serving the state of the pivot. */
  public boolean isPivotStale() {
    return pivotStale;
  }

  /** Returns whether every account, slot and bytecode of the pivot is persisted. */
  public boolean isComplete() {
    return completion.isDone() && !completion.isCompletedExceptionally();
  }

  /**
   * Moves the download to a newer pivot, applying the state changes of the blocks in between to
   * what is already persisted.
   */
  public CompletableFuture<StateDiffApplier.Outcome> moveTo(
      final BlockHeader newPivot, final StateDiff diff) {
    return CompletableFuture.supplyAsync(
        () -> {
          pivotLock.writeLock().lock();
          try {
            if (stopped) {
              throw new IllegalStateException("state download stopped");
            }
            final StateDiffApplier.Outcome outcome =
                StateDiffApplier.apply(diff, storage, coverage);
            pivot = new Pivot(pivot.epoch() + 1, newPivot);
            pivotStale = false;
            return outcome;
          } finally {
            pivotLock.writeLock().unlock();
            execute(
                () -> {
                  consecutiveEmpty = 0;
                  emptyPeers.clear();
                  dispatch();
                });
          }
        },
        scheduler.workers());
  }

  /**
   * Runs a write on the flat state at a known pivot, exclusive with pivot moves.
   *
   * @return false if the pivot moved and nothing was run
   */
  public boolean withPivot(final int epoch, final Runnable write) {
    pivotLock.readLock().lock();
    try {
      // a stopped download must not touch a storage that may already be wiped for a new sync
      if (stopped || pivot.epoch() != epoch) {
        return false;
      }
      write.run();
      return true;
    } finally {
      pivotLock.readLock().unlock();
    }
  }

  // runs an action exclusive with pivot moves, unless the download stopped
  private boolean withoutMoves(final Runnable action) {
    pivotLock.readLock().lock();
    try {
      if (stopped) {
        return false;
      }
      action.run();
      return true;
    } finally {
      pivotLock.readLock().unlock();
    }
  }

  /** Returns the number of storage tries built while downloading. */
  public long storageTriesBuilt() {
    return tries.built();
  }

  /** Returns the number of downloaded storages waiting for their trie. */
  public int storageTriesQueued() {
    return tries.queued();
  }

  /** Runs a write on the flat state exclusive with every download write. */
  public <T> T exclusively(final java.util.function.Supplier<T> write) {
    pivotLock.writeLock().lock();
    try {
      if (stopped) {
        throw new IllegalStateException("state download stopped");
      }
      return write.get();
    } finally {
      pivotLock.writeLock().unlock();
    }
  }

  /** Stops sending new requests, while several pivot moves are applied in a row. */
  public void pause() {
    paused = true;
  }

  /** Resumes sending requests. */
  public void resume() {
    paused = false;
    execute(this::dispatch);
  }

  /** Switches to a new pivot; callers hold the exclusive lock. */
  void switchPivot(final BlockHeader newPivot) {
    pivot = new Pivot(pivot.epoch() + 1, newPivot);
    pivotStale = false;
    execute(
        () -> {
          consecutiveEmpty = 0;
          emptyPeers.clear();
        });
  }

  /** Stops downloading; once it returns, nothing more is written. */
  public void stop() {
    // waits for writes in progress
    pivotLock.writeLock().lock();
    try {
      stopped = true;
    } finally {
      pivotLock.writeLock().unlock();
    }
    loop.shutdownNow();
    tries.stop();
    completion.completeExceptionally(new CancellationException("state download stopped"));
  }

  /** Returns a one-line progress summary. */
  public String progress() {
    return String.format(
        "accounts %d (%.2f%% of key space), slots %d, bytecodes %d, pending storages %d, %d peers left out",
        accounts.get(),
        coverage.accounts().coverage() * 100,
        slots.get(),
        codes.get(),
        coverage.pendingStorageCount(),
        excludedCount);
  }

  /** Returns the download bookkeeping, for diagnostics. */
  public String diagnostics() {
    final CompletableFuture<String> out = new CompletableFuture<>();
    execute(
        () ->
            out.complete(
                String.format(
                    "epoch=%d stale=%s accountTasksLeft=%d inFlight[acc=%d sto=%d code=%d ref=%d] storageQueue=%d refreshQueue=%d chunked=%d codeQueue=%d codesInFlight=%d pendingStorage=%d coverage=%s",
                    pivot.epoch(),
                    pivotStale,
                    accountTasks.stream().filter(t -> !t.done).count(),
                    accountsInFlight,
                    storageInFlight,
                    codesRequestsInFlight,
                    refreshesInFlight,
                    storageQueue.size(),
                    refreshQueue.size(),
                    chunked.size(),
                    codeQueue.size(),
                    codesInFlight.size(),
                    coverage.pendingStorageCount(),
                    coverage.accounts().snapshot().size())));
    try {
      return out.get(5, TimeUnit.SECONDS);
    } catch (final Exception e) {
      return "unavailable: " + e;
    }
  }

  public long downloadedAccounts() {
    return accounts.get();
  }

  public long downloadedSlots() {
    return slots.get();
  }

  public long downloadedCodes() {
    return codes.get();
  }

  private void execute(final Runnable task) {
    if (!loop.isShutdown()) {
      try {
        loop.execute(task);
      } catch (final RuntimeException e) {
        LOG.trace("State loop rejected a task", e);
      }
    }
  }

  private void dispatchSafely() {
    try {
      dispatch();
    } catch (final RuntimeException e) {
      LOG.warn("SNAP2 state dispatch failed", e);
    }
  }

  private int inFlight() {
    return accountsInFlight + storageInFlight + codesRequestsInFlight + refreshesInFlight;
  }

  private void dispatch() {
    if (stopped || completion.isDone()) {
      return;
    }
    if (checkCompletion()) {
      return;
    }
    if (pivotStale || paused) {
      return;
    }
    excluded = servers.excluded(System.nanoTime());
    excludedCount = excluded.size();
    final Pivot current = pivot;
    final int capacity = Math.max(1, scheduler.capacity(Protocol.SNAP));
    final int target = capacity + capacity / 4;

    while (inFlight() < target && !refreshQueue.isEmpty()) {
      final StorageTask task = refreshQueue.poll();
      task.refreshQueued = false;
      if (isPending(task) && !task.refreshing) {
        sendRefresh(task, current);
      }
    }

    // accounts feed storages and bytecodes: pause them while those lag, to bound memory
    final boolean backlogged =
        coverage.pendingStorageCount() > config.storageBacklog()
            || codeQueue.size() > config.storageBacklog();
    final int accountShare = backlogged ? 0 : Math.max(1, target / 3);
    for (final RangeTask task : accountTasks) {
      if (inFlight() >= target || accountsInFlight >= accountShare) {
        break;
      }
      if (!task.done && !task.inFlight) {
        sendAccounts(task, current);
      }
    }

    final int codeShare = Math.max(1, target / 8);
    while (inFlight() < target && codesRequestsInFlight < codeShare && sendCodes()) {
      // keep bytecodes flowing alongside the state
    }

    final Iterator<StorageTask> chunkedIterator = chunked.iterator();
    while (chunkedIterator.hasNext() && inFlight() < target) {
      final StorageTask task = chunkedIterator.next();
      if (!isPending(task)) {
        chunkedIterator.remove();
        continue;
      }
      if (task.isRootStale()) {
        if (task.inFlight == 0) {
          requestRefresh(task);
        }
        continue;
      }
      for (final RangeTask chunk : task.chunks) {
        if (inFlight() >= target) {
          break;
        }
        if (!chunk.done && !chunk.inFlight && !task.refreshing) {
          sendStorageChunk(task, chunk, current);
        }
      }
    }

    while (inFlight() < target && !storageQueue.isEmpty()) {
      final List<StorageTask> batch = takeStorageBatch();
      if (batch.isEmpty()) {
        break;
      }
      sendStorageBatch(batch, current);
    }

    while (inFlight() < target && sendCodes()) {
      // use idle capacity for bytecodes
    }
  }

  private boolean isPending(final StorageTask task) {
    return coverage.storageTask(task.accountHash()) == task;
  }

  private void requestRefresh(final StorageTask task) {
    if (!task.refreshing && !task.refreshQueued) {
      task.refreshQueued = true;
      refreshQueue.add(task);
    }
  }

  private List<StorageTask> takeStorageBatch() {
    final List<StorageTask> batch = new ArrayList<>();
    while (batch.size() < config.storageBatchSize() && !storageQueue.isEmpty()) {
      final StorageTask task = storageQueue.poll();
      task.queued = false;
      if (!isPending(task)) {
        continue;
      }
      if (task.isRootStale()) {
        requestRefresh(task);
        continue;
      }
      batch.add(task);
    }
    return batch;
  }

  private void sendAccounts(final RangeTask task, final Pivot current) {
    task.inFlight = true;
    accountsInFlight++;
    final AccountRangeRequest request =
        new AccountRangeRequest(
            current.epoch(),
            current.header().getNumber(),
            current.root(),
            task.next,
            task.limit,
            config.responseBytes(),
            "account_range");
    scheduler
        .submit(request, excluded)
        .whenComplete(
            (response, error) -> {
              final AccountsPersisted persisted =
                  error == null
                      ? persistSafely(() -> persistAccounts(request, response.value()), request)
                      : null;
              execute(
                  () -> {
                    task.inFlight = false;
                    accountsInFlight--;
                    if (error != null) {
                      onFailure(error, request.epoch);
                    } else {
                      onServed(response.peer(), request.epoch);
                    }
                    if (persisted != null) {
                      if (persisted.done) {
                        task.done = true;
                      } else {
                        task.next = Keys.next(persisted.coveredEnd).orElseThrow();
                      }
                      persisted.storageTasks.forEach(this::queueStorage);
                      persisted.codeHashes.forEach(this::queueCode);
                    }
                    dispatch();
                  });
            });
  }

  private record AccountsPersisted(
      Bytes32 coveredEnd, boolean done, List<StorageTask> storageTasks, Set<Bytes32> codeHashes) {}

  private AccountsPersisted persistAccounts(
      final AccountRangeRequest request, final AccountRangeRequest.Result result) {
    final List<StorageTask> storageTasks = new ArrayList<>();
    final Set<Bytes32> codeHashes = new LinkedHashSet<>();
    final Bytes32[] end = new Bytes32[1];
    final boolean[] done = new boolean[1];
    final boolean persisted =
        withPivot(
            request.epoch,
            () -> {
              final SyncStateStorage.Batch batch = storage.batch();
              int count = 0;
              for (int i = 0; i < result.hashes().size(); i++) {
                final Bytes32 hash = result.hashes().get(i);
                if (hash.compareTo(request.limit) > 0) {
                  break;
                }
                final Bytes account = result.accounts().get(i);
                batch.putAccount(hash, account);
                final PmtStateTrieAccountValue value = Accounts.decode(account);
                if (!value.getStorageRoot().equals(Hash.EMPTY_TRIE_HASH)) {
                  final StorageTask task =
                      new StorageTask(hash, Bytes32.wrap(value.getStorageRoot().getBytes()));
                  coverage.addStorageTask(task);
                  storageTasks.add(task);
                }
                if (!value.getCodeHash().equals(Hash.EMPTY)) {
                  codeHashes.add(Bytes32.wrap(value.getCodeHash().getBytes()));
                }
                count++;
              }
              final List<Bytes32> hashes = result.hashes();
              final Bytes32 last = hashes.isEmpty() ? null : hashes.getLast();
              done[0] = last == null || !result.hasMore() || last.compareTo(request.limit) >= 0;
              end[0] = done[0] ? request.limit : last;
              batch.commit();
              coverage.accounts().add(request.origin, end[0]);
              accounts.addAndGet(count);
            });
    return persisted ? new AccountsPersisted(end[0], done[0], storageTasks, codeHashes) : null;
  }

  private void queueStorage(final StorageTask task) {
    if (!task.queued) {
      task.queued = true;
      storageQueue.add(task);
    }
  }

  // seenCodes only spares duplicate lookups of common bytecodes, it is bounded and may forget
  private void queueCode(final Bytes32 codeHash) {
    if (seenCodes.contains(codeHash) || codesInFlight.contains(codeHash)) {
      return;
    }
    if (seenCodes.size() >= SEEN_CODES_LIMIT) {
      seenCodes.clear();
    }
    final Bytes32 compact = codeHash.copy();
    seenCodes.add(compact);
    codeQueue.add(compact);
  }

  private void sendStorageBatch(final List<StorageTask> batch, final Pivot current) {
    storageInFlight++;
    batch.forEach(task -> task.inFlight++);
    final StorageRangesRequest request =
        new StorageRangesRequest(
            current.epoch(),
            current.header().getNumber(),
            current.root(),
            batch,
            Keys.MIN,
            Keys.MAX,
            null,
            config.responseBytes());
    submitStorage(request);
  }

  private void sendStorageChunk(
      final StorageTask task, final RangeTask chunk, final Pivot current) {
    storageInFlight++;
    task.inFlight++;
    chunk.inFlight = true;
    final StorageRangesRequest request =
        new StorageRangesRequest(
            current.epoch(),
            current.header().getNumber(),
            current.root(),
            List.of(task),
            chunk.next,
            chunk.limit,
            chunk,
            config.responseBytes());
    submitStorage(request);
  }

  private void submitStorage(final StorageRangesRequest request) {
    scheduler
        .submit(request, excluded)
        .whenComplete(
            (response, error) -> {
              final StoragePersisted persisted =
                  error == null
                      ? persistSafely(() -> persistStorage(request, response.value()), request)
                      : null;
              execute(
                  () -> {
                    storageInFlight--;
                    request.tasks.forEach(task -> task.inFlight--);
                    if (request.chunk != null) {
                      request.chunk.inFlight = false;
                    }
                    if (error != null) {
                      onFailure(error, request.epoch);
                    } else {
                      onServed(response.peer(), request.epoch);
                    }
                    afterStorage(request, persisted);
                    dispatch();
                  });
            });
  }

  private record StoragePersisted(int served, Bytes32 lastSlot, boolean lastComplete, int slots) {}

  private StoragePersisted persistStorage(
      final StorageRangesRequest request, final StorageRangesRequest.Result result) {
    final StoragePersisted[] out = new StoragePersisted[1];
    withPivot(
        request.epoch,
        () -> {
          final SyncStateStorage.Batch batch = storage.batch();
          Bytes32 lastSlot = null;
          boolean lastComplete = true;
          int count = 0;
          for (int i = 0; i < result.accounts().size(); i++) {
            final StorageTask task = request.tasks.get(i);
            final StorageRangesRequest.Slots accountSlots = result.accounts().get(i);
            for (int s = 0; s < accountSlots.hashes().size(); s++) {
              final Bytes32 slotHash = accountSlots.hashes().get(s);
              if (slotHash.compareTo(request.limit) > 0) {
                break;
              }
              final Bytes32 value = Bytes32.leftPad(RLP.decodeOne(accountSlots.values().get(s)));
              if (isPending(task) && !value.isZero()) {
                batch.putStorage(task.accountHash(), slotHash, value);
              }
              count++;
            }
            // the proof covers up to the last returned slot, which may lie past the limit
            final List<Bytes32> returned = accountSlots.hashes();
            final Bytes32 last = returned.isEmpty() ? null : returned.getLast();
            final boolean complete =
                accountSlots.complete() || last == null || last.compareTo(request.limit) >= 0;
            if (isPending(task)) {
              task.cover(request.origin, complete ? request.limit : last);
              if (task.isComplete()) {
                coverage.removeStorageTask(task.accountHash());
                tries.completed(task.accountHash());
              }
            }
            lastSlot = last;
            lastComplete = complete;
          }
          batch.commit();
          slots.addAndGet(count);
          out[0] = new StoragePersisted(result.accounts().size(), lastSlot, lastComplete, count);
        });
    return out[0];
  }

  private void afterStorage(final StorageRangesRequest request, final StoragePersisted persisted) {
    if (request.chunk != null) {
      final RangeTask chunk = request.chunk;
      if (persisted != null) {
        if (persisted.lastComplete) {
          chunk.done = true;
        } else {
          chunk.next = Keys.next(persisted.lastSlot).orElseThrow();
        }
      }
      return;
    }
    final int served = persisted == null ? 0 : persisted.served;
    for (int i = 0; i < request.tasks.size(); i++) {
      final StorageTask task = request.tasks.get(i);
      if (!isPending(task)) {
        continue;
      }
      if (i < served - 1 || (i == served - 1 && persisted.lastComplete)) {
        continue;
      }
      if (i == served - 1) {
        splitIntoChunks(task, persisted.lastSlot);
      } else {
        queueStorage(task);
      }
    }
  }

  // a storage too large for one response is downloaded in parallel slot ranges
  private void splitIntoChunks(final StorageTask task, final Bytes32 lastSlot) {
    final Bytes32 start = Keys.next(lastSlot).orElseThrow();
    // one response covered [0, lastSlot], the rest needs about as many responses as it is wide
    final BigInteger span = lastSlot.toUnsignedBigInteger().add(BigInteger.ONE);
    final BigInteger remaining =
        Keys.MAX.toUnsignedBigInteger().subtract(lastSlot.toUnsignedBigInteger());
    final long estimatedRequests =
        remaining.divide(span).min(BigInteger.valueOf(1L << 20)).longValue();
    final int parts =
        (int) Math.max(1, Math.min(config.maxStorageChunks(), (estimatedRequests + 3) / 4));
    for (final Bytes32[] range : Keys.split(start, Keys.MAX, parts)) {
      task.chunks.add(new RangeTask(range[0], range[1]));
    }
    chunked.add(task);
  }

  private void sendRefresh(final StorageTask task, final Pivot current) {
    task.refreshing = true;
    refreshesInFlight++;
    final AccountRangeRequest request =
        new AccountRangeRequest(
            current.epoch(),
            current.header().getNumber(),
            current.root(),
            task.accountHash(),
            task.accountHash(),
            REFRESH_BYTES,
            "account_refresh");
    scheduler
        .submit(request, excluded)
        .whenComplete(
            (response, error) -> {
              if (error == null) {
                persistSafely(() -> persistRefresh(request, task, response.value()), request);
              }
              execute(
                  () -> {
                    task.refreshing = false;
                    refreshesInFlight--;
                    if (error != null) {
                      onFailure(error, request.epoch);
                    } else {
                      onServed(response.peer(), request.epoch);
                    }
                    if (isPending(task) && task.chunks.isEmpty()) {
                      queueStorage(task);
                    }
                    dispatch();
                  });
            });
  }

  private boolean persistRefresh(
      final AccountRangeRequest request,
      final StorageTask task,
      final AccountRangeRequest.Result result) {
    return withPivot(
        request.epoch,
        () -> {
          if (!isPending(task)) {
            return;
          }
          final Bytes32 hash = task.accountHash();
          final SyncStateStorage.Batch batch = storage.batch();
          if (result.hashes().isEmpty() || !result.hashes().getFirst().equals(hash)) {
            LOG.debug("Account {} no longer exists at the pivot, dropping it", hash);
            batch.removeAccount(hash);
            storage.deleteStorage(hash);
            coverage.removeStorageTask(hash);
          } else {
            final Bytes account = result.accounts().getFirst();
            final PmtStateTrieAccountValue value = Accounts.decode(account);
            batch.putAccount(hash, account);
            if (value.getStorageRoot().equals(Hash.EMPTY_TRIE_HASH)) {
              storage.deleteStorage(hash);
              coverage.removeStorageTask(hash);
            } else {
              task.refreshRoot(Bytes32.wrap(value.getStorageRoot().getBytes()));
            }
          }
          batch.commit();
        });
  }

  private boolean sendCodes() {
    final List<Bytes32> batch = new ArrayList<>();
    final Iterator<Bytes32> iterator = codeQueue.iterator();
    while (iterator.hasNext() && batch.size() < config.codeBatchSize()) {
      final Bytes32 codeHash = iterator.next();
      iterator.remove();
      if (!storage.hasCode(codeHash)) {
        batch.add(codeHash);
      }
    }
    if (batch.isEmpty()) {
      return false;
    }
    codesRequestsInFlight++;
    codesInFlight.addAll(batch);
    final ByteCodesRequest request = new ByteCodesRequest(batch, config.responseBytes());
    scheduler
        .submit(request)
        .whenComplete(
            (response, error) -> {
              final Map<Bytes32, Bytes> received =
                  error == null
                          && persistSafely(() -> persistCodes(response.value()), request) != null
                      ? response.value()
                      : Map.of();
              execute(
                  () -> {
                    codesRequestsInFlight--;
                    codesInFlight.removeAll(batch);
                    // missing codes go back to the queue
                    batch.stream()
                        .filter(hash -> !received.containsKey(hash))
                        .forEach(codeQueue::add);
                    if (error != null) {
                      onFailure(error, NO_EPOCH);
                    }
                    dispatch();
                  });
            });
    return true;
  }

  private Boolean persistCodes(final Map<Bytes32, Bytes> received) {
    final SyncStateStorage.Batch batch = storage.batch();
    received.forEach((hash, code) -> batch.putCode(Bytes32.ZERO, hash, code));
    batch.commit();
    codes.addAndGet(received.size());
    return Boolean.TRUE;
  }

  // a failed write is retried like a discarded response, and never skips the bookkeeping
  private <R> R persistSafely(
      final java.util.function.Supplier<R> persist, final SyncRequest<?> request) {
    try {
      return persist.get();
    } catch (final RuntimeException e) {
      LOG.warn("Failed to persist {}, retrying it", request.name(), e);
      return null;
    }
  }

  private void onServed(final EthPeer peer, final int epoch) {
    servers.served(peer, epoch);
    if (epoch == pivot.epoch()) {
      consecutiveEmpty = 0;
      emptyPeers.clear();
    }
  }

  // empty answers of peers that serve the state tell that the pivot is too old
  private void onFailure(final Throwable error, final int epoch) {
    RequestFailedException.from(error)
        .ifPresentOrElse(
            failure -> {
              if (failure.reason() == RequestFailedException.Reason.EMPTY
                  && epoch == pivot.epoch()
                  && failure.peer().isPresent()) {
                onEmpty(failure.peer().get(), epoch);
              }
              LOG.trace("State request failed: {}", failure.getMessage());
            },
            () -> LOG.debug("State request failed", error));
  }

  private void onEmpty(final EthPeer peer, final int epoch) {
    if (servers.empty(peer, epoch, System.nanoTime())) {
      LOG.debug(
          "{} does not serve the state of block {}, leaving it out for a while",
          peer.getLoggableId(),
          pivot.header().getNumber());
      emptyPeers.remove(peer);
      return;
    }
    consecutiveEmpty++;
    emptyPeers.add(peer);
    final int candidates = Math.max(1, scheduler.peerCount(Protocol.SNAP) - excluded.size());
    if (consecutiveEmpty >= EMPTY_RESPONSES_FOR_STALE_PIVOT
        && emptyPeers.size() >= Math.min(3, candidates)
        && !pivotStale) {
      LOG.info(
          "Peers stopped serving the state of block {}, the pivot must move",
          pivot.header().getNumber());
      pivotStale = true;
    }
  }

  /** Returns the number of peers left out of state requests because they serve no state. */
  public int peersWithoutState() {
    return excludedCount;
  }

  private boolean checkCompletion() {
    if (accountTasks.isEmpty()
        || !coverage.accounts().isFull()
        || coverage.pendingStorageCount() > 0
        || !codeQueue.isEmpty()
        || !codesInFlight.isEmpty()
        || inFlight() > 0) {
      return false;
    }
    LOG.info(
        "SNAP2 world state download complete at block {}: {}",
        pivot.header().getNumber(),
        progress());
    completion.complete(null);
    return true;
  }
}
