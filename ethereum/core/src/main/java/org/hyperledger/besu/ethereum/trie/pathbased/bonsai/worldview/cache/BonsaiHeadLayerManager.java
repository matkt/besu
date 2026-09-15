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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_CHECKPOINT_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_CHECKPOINT_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.LayeredKeyValueStorage;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the canonical layered-head window: candidate payload layers keyed by block hash, the
 * current head layer chain, and periodic RocksDB checkpoints.
 *
 * <p>When enabled, validated payloads keep a durable in-memory layer (flat + trie). Forkchoice
 * promotion swaps the head to that layer without flushing RocksDB. A checkpoint flushes the
 * compacted window atomically and advances the durable checkpoint metadata.
 *
 * <p>Expensive {@code reparentOnto(durableRoot)} work is scheduled on a dedicated idle-prep thread
 * so {@code newPayload} only pays for a cheap layer clone + local diff snapshot. {@code
 * forkchoiceUpdated} joins that prep (usually already finished in the inter-block gap).
 */
public class BonsaiHeadLayerManager {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiHeadLayerManager.class);

  private final BonsaiWorldStateKeyValueStorage rootStorage;
  private final int checkpointInterval;
  private final long memoryBudgetBytes;
  private final ExecutorService prepExecutor;
  private Consumer<BlockHeader> onCheckpoint = header -> {};

  private final Map<Hash, CandidateLayer> candidates = new ConcurrentHashMap<>();
  private final Deque<CanonicalLayer> canonicalWindow = new ArrayDeque<>();

  private volatile Hash checkpointHash = Hash.ZERO;
  private volatile long checkpointNumber = 0L;
  private final AtomicLong estimatedWindowBytes = new AtomicLong(0L);
  private final AtomicLong promotions = new AtomicLong(0L);
  private final AtomicLong checkpoints = new AtomicLong(0L);
  private final AtomicLong trieLogFallbacks = new AtomicLong(0L);

  public BonsaiHeadLayerManager(
      final BonsaiWorldStateKeyValueStorage rootStorage,
      final int checkpointInterval,
      final long memoryBudgetBytes) {
    this.rootStorage = rootStorage;
    this.checkpointInterval = Math.max(1, checkpointInterval);
    this.memoryBudgetBytes = Math.max(1L, memoryBudgetBytes);
    this.checkpointHash = rootStorage.getWorldStateCheckpointHash().orElse(Hash.ZERO);
    this.checkpointNumber = rootStorage.getWorldStateCheckpointNumber().orElse(0L);
    this.prepExecutor =
        Executors.newSingleThreadExecutor(
            new ThreadFactory() {
              @Override
              public Thread newThread(final Runnable r) {
                final Thread t = new Thread(r, "bonsai-layered-head-prep");
                t.setDaemon(true);
                return t;
              }
            });
  }

  public void setOnCheckpoint(final Consumer<BlockHeader> onCheckpoint) {
    this.onCheckpoint = onCheckpoint == null ? header -> {} : onCheckpoint;
  }

  /**
   * Registers a candidate from a validated payload. Hot path: freeze the local diff only. {@code
   * reparentOnto(root)} runs asynchronously on the idle-prep thread (inter-block gap).
   *
   * <p>{@code layerStorage} may be shared with the world-state cache — this manager never closes
   * that source layer. Only the exclusive reparented copy created during prep is owned here.
   */
  public synchronized void registerCandidate(
      final BlockHeader blockHeader, final BonsaiWorldStateLayerStorage layerStorage) {
    final LayeredKeyValueStorage composed = layerStorage.getComposedWorldStateStorage();
    final LayeredKeyValueStorage diff = composed.snapshotDiff();
    final long bytes = diff.estimatedDiffBytes();

    final CompletableFuture<BonsaiWorldStateLayerStorage> prepFuture =
        CompletableFuture.supplyAsync(() -> layerStorage.reparentOnto(rootStorage), prepExecutor);

    final CandidateLayer candidate =
        new CandidateLayer(blockHeader, layerStorage, diff, bytes, prepFuture);
    prepFuture.whenComplete(
        (prepared, error) -> {
          if (error != null) {
            LOG.warn(
                "Layered-head idle reparent failed for {}: {}",
                blockHeader.toLogString(),
                error.toString());
            return;
          }
          synchronized (BonsaiHeadLayerManager.this) {
            final CandidateLayer current = candidates.get(blockHeader.getBlockHash());
            if (current != candidate || candidate.ownershipTransferred) {
              // Superseded, pruned, or already promoted while prep was running.
              closeQuietly(prepared);
              return;
            }
            if (!candidate.preparedStorage.compareAndSet(null, prepared)) {
              // ensurePrepared already installed the same (or equivalent) result.
              if (candidate.preparedStorage.get() != prepared) {
                closeQuietly(prepared);
              }
            }
          }
        });

    final CandidateLayer previous = candidates.put(blockHeader.getBlockHash(), candidate);
    if (previous != null) {
      estimatedWindowBytes.addAndGet(-previous.estimatedBytes());
      previous.cancelAndCloseOwned();
    }
    estimatedWindowBytes.addAndGet(bytes);
    LOG.debug(
        "Registered layered-head candidate {} ({} bytes, {} candidates; reparent deferred)",
        blockHeader.toLogString(),
        bytes,
        candidates.size());
  }

  public boolean hasCandidate(final Hash blockHash) {
    return candidates.containsKey(blockHash);
  }

  public Optional<BonsaiWorldStateLayerStorage> getCandidateStorage(final Hash blockHash) {
    final CandidateLayer candidate = candidates.get(blockHash);
    if (candidate == null) {
      return Optional.empty();
    }
    ensurePrepared(candidate);
    return Optional.ofNullable(candidate.preparedStorage.get());
  }

  /**
   * Promotes a previously registered candidate to canonical head. Returns the storage that should
   * become the live head world-state storage, or empty when the candidate is missing. Joins idle
   * prep if the inter-block gap was too short.
   */
  public Optional<BonsaiWorldStateLayerStorage> promote(final BlockHeader newHead) {
    final CandidateLayer candidate = candidates.get(newHead.getBlockHash());
    if (candidate == null) {
      trieLogFallbacks.incrementAndGet();
      return Optional.empty();
    }

    // Join idle prep OUTSIDE the manager monitor — prep.whenComplete also needs this lock.
    ensurePrepared(candidate);

    synchronized (this) {
      if (candidates.get(newHead.getBlockHash()) != candidate) {
        trieLogFallbacks.incrementAndGet();
        return Optional.empty();
      }
      final BonsaiWorldStateLayerStorage promotedStorage = candidate.takePreparedStorage();
      if (promotedStorage == null) {
        trieLogFallbacks.incrementAndGet();
        return Optional.empty();
      }

      // Drop candidates that are neither the new head nor siblings of its parent (keep siblings for
      // competing FCUs within the window).
      candidates
          .entrySet()
          .removeIf(
              entry -> {
                final BlockHeader header = entry.getValue().blockHeader();
                final boolean keep =
                    header.getBlockHash().equals(newHead.getBlockHash())
                        || header.getParentHash().equals(newHead.getParentHash());
                if (!keep) {
                  estimatedWindowBytes.addAndGet(-entry.getValue().estimatedBytes());
                  entry.getValue().cancelAndCloseOwned();
                }
                return !keep;
              });

      // Truncate canonical window above the parent (reorg within RAM window).
      while (!canonicalWindow.isEmpty()
          && !canonicalWindow.peekLast().blockHash().equals(newHead.getParentHash())
          && canonicalWindow.peekLast().blockNumber() >= newHead.getNumber()) {
        final CanonicalLayer removed = canonicalWindow.removeLast();
        estimatedWindowBytes.addAndGet(-removed.estimatedBytes());
      }

      final CanonicalLayer promoted =
          new CanonicalLayer(
              newHead.getBlockHash(),
              newHead.getNumber(),
              newHead.getStateRoot(),
              candidate.diff(),
              candidate.estimatedBytes());
      canonicalWindow.addLast(promoted);
      promotions.incrementAndGet();

      LOG.debug(
          "Promoted layered head to {} (window={}, bytes={})",
          newHead.toLogString(),
          canonicalWindow.size(),
          estimatedWindowBytes.get());

      maybeCheckpoint(newHead);
      return Optional.of(promotedStorage);
    }
  }

  /** Forces a RocksDB checkpoint of the compacted canonical window. */
  public synchronized boolean checkpointNow(final BlockHeader headHeader) {
    return doCheckpoint(headHeader);
  }

  private void maybeCheckpoint(final BlockHeader headHeader) {
    final long depth = headHeader.getNumber() - checkpointNumber;
    if (depth >= checkpointInterval || estimatedWindowBytes.get() >= memoryBudgetBytes) {
      doCheckpoint(headHeader);
    }
  }

  private boolean doCheckpoint(final BlockHeader headHeader) {
    if (canonicalWindow.isEmpty()) {
      return false;
    }
    try {
      final LayeredKeyValueStorage compacted =
          new LayeredKeyValueStorage(rootStorage.getComposedWorldStateStorage());
      // Process oldest → newest so later layers overwrite earlier ones (latest wins).
      for (final CanonicalLayer layer : canonicalWindow) {
        compacted.mergeLatestInto(layer.diff());
      }

      final PathBasedWorldStateKeyValueStorage.Updater updater = rootStorage.updater();
      final SegmentedKeyValueStorageTransaction tx = updater.getWorldStateTransaction();
      compacted.mergeTo(tx);
      tx.put(
          TRIE_BRANCH_STORAGE,
          WORLD_ROOT_HASH_KEY,
          headHeader.getStateRoot().getBytes().toArrayUnsafe());
      tx.put(
          TRIE_BRANCH_STORAGE,
          WORLD_BLOCK_HASH_KEY,
          headHeader.getBlockHash().getBytes().toArrayUnsafe());
      tx.put(
          TRIE_BRANCH_STORAGE,
          WORLD_BLOCK_NUMBER_KEY,
          Bytes.ofUnsignedLong(headHeader.getNumber()).toArrayUnsafe());
      tx.put(
          TRIE_BRANCH_STORAGE,
          WORLD_CHECKPOINT_HASH_KEY,
          headHeader.getBlockHash().getBytes().toArrayUnsafe());
      tx.put(
          TRIE_BRANCH_STORAGE,
          WORLD_CHECKPOINT_NUMBER_KEY,
          Bytes.ofUnsignedLong(headHeader.getNumber()).toArrayUnsafe());
      updater.commitComposedOnly();

      checkpointHash = headHeader.getBlockHash();
      checkpointNumber = headHeader.getNumber();
      estimatedWindowBytes.set(0L);
      canonicalWindow.clear();
      candidates
          .entrySet()
          .removeIf(
              entry -> {
                if (entry.getValue().blockHeader().getNumber() <= checkpointNumber) {
                  entry.getValue().cancelAndCloseOwned();
                  return true;
                }
                return false;
              });
      checkpoints.incrementAndGet();
      LOG.info(
          "Layered-head RocksDB checkpoint at {} (interval={}, budget={})",
          headHeader.toLogString(),
          checkpointInterval,
          memoryBudgetBytes);
      onCheckpoint.accept(headHeader);
      return true;
    } catch (final Exception e) {
      LOG.error(
          "Failed layered-head checkpoint at {}; keeping in-memory window",
          headHeader.toLogString(),
          e);
      return false;
    }
  }

  private void ensurePrepared(final CandidateLayer candidate) {
    if (candidate.ownershipTransferred) {
      return;
    }
    if (candidate.preparedStorage.get() != null) {
      return;
    }
    try {
      final BonsaiWorldStateLayerStorage prepared = candidate.prepFuture.join();
      if (!candidate.ownershipTransferred) {
        candidate.preparedStorage.compareAndSet(null, prepared);
      } else if (candidate.preparedStorage.get() != prepared) {
        closeQuietly(prepared);
      }
    } catch (final CompletionException e) {
      LOG.warn(
          "Layered-head prep join failed for {}, falling back to sync reparent: {}",
          candidate.blockHeader().toLogString(),
          e.toString());
      if (!candidate.ownershipTransferred) {
        final BonsaiWorldStateLayerStorage fallback =
            candidate.sourceLayer().reparentOnto(rootStorage);
        candidate.preparedStorage.compareAndSet(null, fallback);
      }
    }
  }

  public Hash getCheckpointHash() {
    return checkpointHash;
  }

  public long getCheckpointNumber() {
    return checkpointNumber;
  }

  public int getCanonicalWindowDepth() {
    return canonicalWindow.size();
  }

  public long getEstimatedWindowBytes() {
    return estimatedWindowBytes.get();
  }

  public long getPromotionCount() {
    return promotions.get();
  }

  public long getCheckpointCount() {
    return checkpoints.get();
  }

  public long getTrieLogFallbackCount() {
    return trieLogFallbacks.get();
  }

  public void recordTrieLogFallback() {
    trieLogFallbacks.incrementAndGet();
  }

  /** Shuts down the idle-prep executor. Safe to call multiple times. */
  public void close() {
    prepExecutor.shutdownNow();
  }

  private static void closeQuietly(final PathBasedWorldStateKeyValueStorage storage) {
    if (storage == null) {
      return;
    }
    try {
      if (storage.isClosed()) {
        return;
      }
      storage.close();
    } catch (final Exception e) {
      LOG.debug("Failed closing layered-head storage", e);
    }
  }

  private static final class CandidateLayer {
    private final BlockHeader blockHeader;
    private final BonsaiWorldStateLayerStorage sourceLayer;
    private final LayeredKeyValueStorage diff;
    private final long estimatedBytes;
    private final CompletableFuture<BonsaiWorldStateLayerStorage> prepFuture;
    private final AtomicReference<BonsaiWorldStateLayerStorage> preparedStorage =
        new AtomicReference<>();
    private volatile boolean ownershipTransferred;

    private CandidateLayer(
        final BlockHeader blockHeader,
        final BonsaiWorldStateLayerStorage sourceLayer,
        final LayeredKeyValueStorage diff,
        final long estimatedBytes,
        final CompletableFuture<BonsaiWorldStateLayerStorage> prepFuture) {
      this.blockHeader = blockHeader;
      this.sourceLayer = sourceLayer;
      this.diff = diff;
      this.estimatedBytes = estimatedBytes;
      this.prepFuture = prepFuture;
    }

    private BlockHeader blockHeader() {
      return blockHeader;
    }

    private BonsaiWorldStateLayerStorage sourceLayer() {
      return sourceLayer;
    }

    private LayeredKeyValueStorage diff() {
      return diff;
    }

    private long estimatedBytes() {
      return estimatedBytes;
    }

    /** Transfers ownership of the prepared storage to the caller (promote). */
    private BonsaiWorldStateLayerStorage takePreparedStorage() {
      ownershipTransferred = true;
      return preparedStorage.getAndSet(null);
    }

    private void cancelAndCloseOwned() {
      ownershipTransferred = true;
      // Do not close sourceLayer — it is shared with the world-state cache.
      final BonsaiWorldStateLayerStorage prepared = preparedStorage.getAndSet(null);
      if (prepared != null && prepared != sourceLayer) {
        closeQuietly(prepared);
      }
    }
  }

  private record CanonicalLayer(
      Hash blockHash,
      long blockNumber,
      Hash stateRoot,
      LayeredKeyValueStorage diff,
      long estimatedBytes) {}
}
