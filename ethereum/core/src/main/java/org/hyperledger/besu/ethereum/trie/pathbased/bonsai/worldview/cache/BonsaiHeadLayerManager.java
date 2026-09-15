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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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
 */
public class BonsaiHeadLayerManager {
  private static final Logger LOG = LoggerFactory.getLogger(BonsaiHeadLayerManager.class);

  private final BonsaiWorldStateKeyValueStorage rootStorage;
  private final int checkpointInterval;
  private final long memoryBudgetBytes;
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
  }

  public void setOnCheckpoint(final Consumer<BlockHeader> onCheckpoint) {
    this.onCheckpoint = onCheckpoint == null ? header -> {} : onCheckpoint;
  }

  /**
   * Registers an immutable candidate layer produced by a validated payload. The layer must already
   * contain flat state and trie-node writes for that block and should already be parented onto the
   * durable root.
   */
  public synchronized void registerCandidate(
      final BlockHeader blockHeader, final BonsaiWorldStateLayerStorage layerStorage) {
    final LayeredKeyValueStorage composed = layerStorage.getComposedWorldStateStorage();
    // Freeze local diff once for the canonical window; worldStateStorage is already isolated from
    // the live payload world-state (reparented/cloned into the cache before registration).
    final LayeredKeyValueStorage diff = composed.snapshotDiff();
    final long bytes = diff.estimatedDiffBytes();
    final CandidateLayer previous =
        candidates.put(
            blockHeader.getBlockHash(),
            new CandidateLayer(blockHeader, layerStorage, diff, bytes));
    if (previous != null) {
      estimatedWindowBytes.addAndGet(-previous.estimatedBytes());
      closeQuietly(previous.worldStateStorage());
    }
    estimatedWindowBytes.addAndGet(bytes);
    LOG.debug(
        "Registered layered-head candidate {} ({} bytes, {} candidates)",
        blockHeader.toLogString(),
        bytes,
        candidates.size());
  }

  public boolean hasCandidate(final Hash blockHash) {
    return candidates.containsKey(blockHash);
  }

  public Optional<BonsaiWorldStateLayerStorage> getCandidateStorage(final Hash blockHash) {
    return Optional.ofNullable(candidates.get(blockHash)).map(CandidateLayer::worldStateStorage);
  }

  /**
   * Promotes a previously registered candidate to canonical head. Returns the storage that should
   * become the live head world-state storage, or empty when the candidate is missing.
   */
  public synchronized Optional<BonsaiWorldStateLayerStorage> promote(final BlockHeader newHead) {
    final CandidateLayer candidate = candidates.get(newHead.getBlockHash());
    if (candidate == null) {
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
                closeQuietly(entry.getValue().worldStateStorage());
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
    return Optional.of(candidate.worldStateStorage());
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
                  closeQuietly(entry.getValue().worldStateStorage());
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

  private static void closeQuietly(final PathBasedWorldStateKeyValueStorage storage) {
    try {
      storage.close();
    } catch (final Exception e) {
      LOG.debug("Failed closing layered-head storage", e);
    }
  }

  private record CandidateLayer(
      BlockHeader blockHeader,
      BonsaiWorldStateLayerStorage worldStateStorage,
      LayeredKeyValueStorage diff,
      long estimatedBytes) {}

  private record CanonicalLayer(
      Hash blockHash,
      long blockNumber,
      Hash stateRoot,
      LayeredKeyValueStorage diff,
      long estimatedBytes) {}
}
