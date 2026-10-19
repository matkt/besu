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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.BINARY_TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.MigrationScopedWorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SortedFileWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link NodeUpdater} that writes a snapshot's PBT into sorted, non-overlapping files, added to the
 * binary-trie column by {@link #finish} only, once the snapshot is accepted.
 */
final class SnapshotColumnWriter implements NodeUpdater, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotColumnWriter.class);

  /** Heap of the nodes kept in location order before the lower half of them is written. */
  static final long DEFAULT_WINDOW_BYTES = 32L << 20;

  /** Heap past which nodes written last mean the arrival order broke (a few MB on mainnet). */
  private static final long MAX_LATE_BYTES = 64L << 20;

  /** Approximate heap cost of a node in the window beyond its key and value. */
  private static final int NODE_OVERHEAD_BYTES = 80;

  /** Bytes of nodes per file, RocksDB's default target file size. */
  static final long DEFAULT_FILE_BYTES = 64L << 20;

  /** Files added per ingestion, which stops every write to the database while it runs. */
  static final int DEFAULT_FILES_PER_INGESTION = 16;

  /** Nodes handed to the sorting thread at a time, at most. */
  private static final int NODES_PER_BATCH = 100_000;

  /** Heap of the nodes handed to the sorting thread at a time, at most (a full stem is ~8.7 KB). */
  private static final long BATCH_BYTES = 8L << 20;

  /** Batches, then halves of the window, queued for the next thread: at most this many. */
  private static final int QUEUED = 2;

  private record Node(byte[] key, byte[] value) {
    int heapSize() {
      return key.length + value.length + NODE_OVERHEAD_BYTES;
    }
  }

  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final Path dir;
  private final long windowBytes;
  private final long fileBytes;
  private final int filesPerIngestion;
  private final ExecutorService sorter = singleThread("pbt-column-sort");
  private final ExecutorService fileWriter = singleThread("pbt-column-files");
  private final Semaphore batchesQueued = new Semaphore(QUEUED);
  private final Semaphore halvesQueued = new Semaphore(QUEUED);
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private List<Node> batch = new ArrayList<>(NODES_PER_BATCH);
  private long batchBytes;
  private long stored;

  // Sorting thread only.
  private final TreeMap<byte[], byte[]> window = new TreeMap<>(Arrays::compareUnsigned);
  private long windowSize;
  private byte[] lastWritten;
  private final List<Node> late = new ArrayList<>();
  private long lateSize;

  // File thread only.
  private final List<Path> files = new ArrayList<>();
  private SortedFileWriter file;
  private long fileSize;
  private long written;

  /**
   * @param worldStateStorage storage of the binary-trie column
   * @param workDir where the files are written, under their own directory
   */
  SnapshotColumnWriter(final BonsaiWorldStateKeyValueStorage worldStateStorage, final Path workDir)
      throws IOException {
    this(
        worldStateStorage,
        workDir,
        DEFAULT_WINDOW_BYTES,
        DEFAULT_FILE_BYTES,
        DEFAULT_FILES_PER_INGESTION);
  }

  /** Same, with explicit sizes (tests use tiny ones for many files and many nodes written last). */
  SnapshotColumnWriter(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final Path workDir,
      final long windowBytes,
      final long fileBytes,
      final int filesPerIngestion)
      throws IOException {
    this.worldStateStorage = worldStateStorage;
    this.dir = Files.createDirectories(workDir.resolve("pbt-column"));
    this.windowBytes = windowBytes;
    this.fileBytes = fileBytes;
    this.filesPerIngestion = filesPerIngestion;
  }

  @Override
  public void store(final Bytes location, final Bytes32 hash, final Bytes value) {
    if (value == null) {
      throw new IllegalStateException("a snapshot load removes no node");
    }
    final Node node = new Node(location.toArrayUnsafe(), value.toArrayUnsafe());
    batch.add(node);
    batchBytes += node.heapSize();
    stored++;
    if (batch.size() >= NODES_PER_BATCH || batchBytes >= BATCH_BYTES) {
      sort(batch);
      batch = new ArrayList<>(NODES_PER_BATCH);
      batchBytes = 0;
    }
  }

  /**
   * Writes the remaining nodes, adds every file to the column, then writes the nodes kept apart and
   * the cursor {@code (root, blockHash)}, last.
   */
  void finish(final Bytes32 root, final Hash blockHash) {
    sort(batch);
    batch = new ArrayList<>();
    submit(
        sorter,
        batchesQueued,
        () -> {
          writeOldest(Long.MAX_VALUE);
          submit(fileWriter, halvesQueued, this::finishFile);
        });
    awaitQueued(batchesQueued);
    awaitQueued(halvesQueued);
    throwIfFailed();
    if (written + late.size() != stored) {
      throw new IllegalStateException(
          String.format(
              "PBT column writer lost nodes: %d stored, %d written",
              stored, written + late.size()));
    }
    // Disjoint and ascending, so each batch also lands at the column's last level.
    for (final List<Path> ingestion : Lists.partition(files, filesPerIngestion)) {
      // The node is closing: the caller empties the column, no cursor was written.
      if (Thread.currentThread().isInterrupted()) {
        throw new CancellationException("PBT column load interrupted");
      }
      worldStateStorage
          .getComposedWorldStateStorage()
          .ingestSortedFiles(BINARY_TRIE_BRANCH_STORAGE, ingestion);
    }
    final BonsaiWorldStateKeyValueStorage.Updater updater =
        new MigrationScopedWorldStateKeyValueStorage(worldStateStorage).updater();
    late.forEach(
        node ->
            updater
                .getWorldStateTransaction()
                .put(BINARY_TRIE_BRANCH_STORAGE, node.key(), node.value()));
    PbtMigrator.putCursor(updater, root, blockHash);
    updater.commit();
    LOG.info(
        "PBT column loaded from {} file(s), {} node(s) written last", files.size(), late.size());
  }

  /**
   * Stops both threads once their queued work is done, even if interrupted (an open file must not
   * be closed under them), then deletes the files left.
   */
  @Override
  public void close() {
    for (final ExecutorService executor : List.of(sorter, fileWriter)) {
      executor.shutdown();
      Uninterruptibles.awaitTerminationUninterruptibly(executor);
    }
    if (file != null) {
      file.close();
    }
    Eip8347ExternalSorter.deleteRecursively(dir);
  }

  /** Sorts {@code nodes} into the window, writing its lower half whenever it is full. */
  private void sort(final List<Node> nodes) {
    submit(
        sorter,
        batchesQueued,
        () -> {
          for (final Node node : nodes) {
            // Below a written location (a top branch, the root): written last, with the cursor.
            if (lastWritten != null && Arrays.compareUnsigned(node.key(), lastWritten) <= 0) {
              late.add(node);
              lateSize += node.heapSize();
              if (lateSize > MAX_LATE_BYTES) {
                throw new IllegalStateException("PBT nodes no longer arrive in location order");
              }
              continue;
            }
            window.put(node.key(), node.value());
            windowSize += node.heapSize();
            if (windowSize > windowBytes) {
              writeOldest(windowSize / 2);
            }
          }
        });
  }

  /** Hands the window's lowest nodes, at least {@code size} bytes, to the file thread. */
  private void writeOldest(final long size) {
    final List<Node> oldest = new ArrayList<>();
    long taken = 0;
    while (taken < size && !window.isEmpty()) {
      final Map.Entry<byte[], byte[]> entry = window.pollFirstEntry();
      final Node node = new Node(entry.getKey(), entry.getValue());
      oldest.add(node);
      taken += node.heapSize();
      lastWritten = node.key();
    }
    windowSize -= taken;
    submit(fileWriter, halvesQueued, () -> append(oldest));
  }

  /** Appends sorted nodes to the current file, starting a new one past {@code fileBytes}. */
  private void append(final List<Node> nodes) {
    for (final Node node : nodes) {
      if (file == null) {
        final Path path = dir.resolve(String.format("binary-trie-%06d.sst", files.size()));
        file =
            worldStateStorage
                .getComposedWorldStateStorage()
                .createSortedFileWriter(BINARY_TRIE_BRANCH_STORAGE, path);
        files.add(path);
        fileSize = 0;
      }
      file.put(node.key(), node.value());
      fileSize += node.key().length + node.value().length;
      written++;
      if (fileSize >= fileBytes) {
        finishFile();
      }
    }
  }

  private void finishFile() {
    if (file != null) {
      file.finish();
      file.close();
      file = null;
    }
  }

  /**
   * Runs {@code task} on {@code executor}, in order, once fewer than {@link #QUEUED} tasks wait
   * there. A failure is kept and skips the tasks after it.
   */
  private void submit(final ExecutorService executor, final Semaphore queued, final Runnable task) {
    throwIfFailed();
    queued.acquireUninterruptibly();
    executor.execute(
        () -> {
          try {
            if (failure.get() == null) {
              task.run();
            }
          } catch (final Throwable t) {
            failure.compareAndSet(null, t);
          } finally {
            queued.release();
          }
        });
  }

  private static void awaitQueued(final Semaphore queued) {
    queued.acquireUninterruptibly(QUEUED);
    queued.release(QUEUED);
  }

  private void throwIfFailed() {
    final Throwable t = failure.get();
    if (t instanceof RuntimeException e) {
      throw e;
    }
    if (t != null) {
      throw new IllegalStateException("PBT column writer failed", t);
    }
  }

  private static ExecutorService singleThread(final String name) {
    return Executors.newSingleThreadExecutor(
        task -> {
          final Thread thread = new Thread(task, name);
          thread.setDaemon(true);
          return thread;
        });
  }
}
