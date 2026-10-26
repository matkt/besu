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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * External sort of {@code (key, value)} records by unsigned key, in bounded heap: full buffers are
 * sorted and written as runs on a background thread, then merged in passes of at most {@link
 * #MAX_FAN_IN} runs. Equal keys are all kept, except with {@link #distinctKeys}. Call {@link #add},
 * then {@link #sorted} once, then {@link #close}.
 */
public final class Eip8347ExternalSorter implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(Eip8347ExternalSorter.class);

  /** Default in-heap buffer per sorter before spilling a run. */
  public static final long DEFAULT_BUFFER_BYTES = 64L << 20;

  /** Runs merged per pass, at most: bounds open files and merge buffers (512 × 64 KiB). */
  public static final int MAX_FAN_IN = 512;

  /** Approximate heap cost of one buffered record beyond its payload. */
  private static final int RECORD_OVERHEAD_BYTES = 64;

  /** Buffer of each run file stream. */
  private static final int RUN_IO_BUFFER_BYTES = 1 << 16;

  private static final Comparator<byte[]> KEY_ORDER = Arrays::compareUnsigned;

  /** One sorted record. */
  public record Entry(byte[] key, byte[] value) {}

  private final Path workDir;
  private final String name;
  private final long maxBufferedBytes;
  private final ForkJoinPool pool;
  private final boolean distinctKeys;
  private final int partitions;
  private List<Entry> buffer = new ArrayList<>();

  /** The runs written, per partition. */
  private final List<List<Path>> runs = new ArrayList<>();

  private final List<RunReader> openReaders = new ArrayList<>();
  private long bufferedBytes;
  private long added;
  private int runSeq;
  private boolean spilled;
  private boolean sorted;

  /** Writes runs in the background; created with the first run. */
  private ExecutorService spiller;

  /** The runs being written, one per partition, null for a partition with no record. */
  private CompletableFuture<Path[]> pendingRun;

  public Eip8347ExternalSorter(final Path workDir, final String name, final long maxBufferedBytes) {
    this(workDir, name, maxBufferedBytes, ForkJoinPool.commonPool());
  }

  /** Same, sorting each buffer on {@code pool}. */
  public Eip8347ExternalSorter(
      final Path workDir, final String name, final long maxBufferedBytes, final ForkJoinPool pool) {
    this(workDir, name, maxBufferedBytes, pool, false, 1);
  }

  private Eip8347ExternalSorter(
      final Path workDir,
      final String name,
      final long maxBufferedBytes,
      final ForkJoinPool pool,
      final boolean distinctKeys,
      final int partitions) {
    if (maxBufferedBytes < 1) {
      throw new IllegalArgumentException("maxBufferedBytes must be positive");
    }
    if (partitions < 1 || partitions > 256) {
      throw new IllegalArgumentException("partitions must be between 1 and 256");
    }
    this.workDir = workDir;
    this.name = name;
    this.maxBufferedBytes = maxBufferedBytes;
    this.pool = pool;
    this.distinctKeys = distinctKeys;
    this.partitions = partitions;
    for (int partition = 0; partition < partitions; partition++) {
      runs.add(new ArrayList<>());
    }
  }

  /** A sorter that keeps one record per key, for records whose value follows from the key. */
  public static Eip8347ExternalSorter distinctKeys(
      final Path workDir, final String name, final long maxBufferedBytes, final ForkJoinPool pool) {
    return new Eip8347ExternalSorter(workDir, name, maxBufferedBytes, pool, true, 1);
  }

  /**
   * A sorter split in {@code partitions} by the key's first byte. A partition's runs are deleted
   * once it is read, so the files shrink as the records are consumed.
   */
  public static Eip8347ExternalSorter partitioned(
      final Path workDir,
      final String name,
      final long maxBufferedBytes,
      final ForkJoinPool pool,
      final int partitions) {
    return new Eip8347ExternalSorter(workDir, name, maxBufferedBytes, pool, false, partitions);
  }

  public void add(final byte[] key, final byte[] value) throws IOException {
    if (sorted) {
      throw new IllegalStateException(name + " sorter already sorted");
    }
    buffer.add(new Entry(key, value));
    added++;
    bufferedBytes += key.length + value.length + RECORD_OVERHEAD_BYTES;
    if (bufferedBytes >= maxBufferedBytes) {
      spillBuffer();
    }
  }

  /** Records added so far, repeated keys included. */
  public long added() {
    return added;
  }

  /** Ends the add phase and returns every record in ascending key order. Call once. */
  public Iterator<Entry> sorted() throws IOException {
    if (sorted) {
      throw new IllegalStateException(name + " sorter already sorted");
    }
    sorted = true;
    if (!spilled) {
      return distinct(Arrays.asList(sort(buffer)).iterator());
    }
    if (!buffer.isEmpty()) {
      spillBuffer();
    }
    awaitPendingRun();
    return new PartitionMerge();
  }

  /** The records of every partition, in order; a partition's runs are deleted once read. */
  private final class PartitionMerge implements Iterator<Entry> {
    private int partition;
    private Iterator<Entry> records;

    PartitionMerge() throws IOException {
      records = mergePartition(0);
    }

    @Override
    public boolean hasNext() {
      while (!records.hasNext() && partition + 1 < partitions) {
        try {
          closeReaders();
          delete(runs.get(partition));
          records = mergePartition(++partition);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      return records.hasNext();
    }

    @Override
    public Entry next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return records.next();
    }
  }

  /** The records of a partition: its runs merged in passes, then the last merge streamed. */
  private Iterator<Entry> mergePartition(final int partition) throws IOException {
    final List<Path> partitionRuns = runs.get(partition);
    while (partitionRuns.size() > MAX_FAN_IN) {
      final List<Path> batch = new ArrayList<>(partitionRuns.subList(0, MAX_FAN_IN));
      partitionRuns.subList(0, MAX_FAN_IN).clear();
      final Path merged = nextRunPath();
      try (final RunWriter out = new RunWriter(merged)) {
        final Iterator<Entry> it = merge(batch);
        while (it.hasNext()) {
          out.write(it.next());
        }
      }
      closeReaders();
      delete(batch);
      partitionRuns.add(merged);
    }
    return merge(partitionRuns);
  }

  @Override
  public void close() throws IOException {
    buffer.clear();
    if (pendingRun != null) {
      // Wait for the writer so its files can be deleted; add() or sorted() reported its failure.
      final Path[] written = pendingRun.exceptionally(failure -> null).join();
      if (written != null) {
        addRuns(written);
      }
      pendingRun = null;
    }
    if (spiller != null) {
      spiller.shutdownNow();
    }
    closeReaders();
    runs.forEach(this::delete);
  }

  /** Hands the full buffer to the background writer, once the previous run is written. */
  private void spillBuffer() throws IOException {
    awaitPendingRun();
    final List<Entry> full = buffer;
    final int run = runSeq++;
    buffer = new ArrayList<>();
    bufferedBytes = 0;
    spilled = true;
    if (spiller == null) {
      spiller =
          Executors.newSingleThreadExecutor(
              task -> {
                final Thread thread = new Thread(task, "eip8347-sort-" + name);
                thread.setDaemon(true);
                return thread;
              });
    }
    pendingRun =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return writeRun(run, full);
              } catch (final IOException e) {
                throw new UncheckedIOException(e);
              }
            },
            spiller);
  }

  /** Writes {@code records}, sorted, as one run file per partition they fall in. */
  private Path[] writeRun(final int run, final List<Entry> records) throws IOException {
    final Path[] written = new Path[partitions];
    final Iterator<Entry> entries = distinct(Arrays.asList(sort(records)).iterator());
    RunWriter out = null;
    try {
      while (entries.hasNext()) {
        final Entry entry = entries.next();
        final int partition = partitionOf(entry.key());
        if (written[partition] == null) {
          if (out != null) {
            out.close();
          }
          written[partition] =
              workDir.resolve(String.format("%s-%06d-%03d.run", name, run, partition));
          out = new RunWriter(written[partition]);
        }
        out.write(entry);
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
    return written;
  }

  /** The partition of a key, by its first byte. */
  private int partitionOf(final byte[] key) {
    return key.length == 0 ? 0 : (key[0] & 0xFF) * partitions / 256;
  }

  private void awaitPendingRun() throws IOException {
    if (pendingRun == null) {
      return;
    }
    final CompletableFuture<Path[]> run = pendingRun;
    pendingRun = null;
    addRuns(Eip8347Pipelines.await(run));
  }

  private void addRuns(final Path[] written) {
    for (int partition = 0; partition < partitions; partition++) {
      if (written[partition] != null) {
        runs.get(partition).add(written[partition]);
      }
    }
  }

  /** Deletes the files of {@code partitionRuns}, then forgets them. */
  private void delete(final List<Path> partitionRuns) {
    for (final Path run : partitionRuns) {
      try {
        Files.deleteIfExists(run);
      } catch (final IOException e) {
        LOG.warn("Failed to delete {}", run, e);
      }
    }
    partitionRuns.clear();
  }

  private Entry[] sort(final List<Entry> entries) {
    final Entry[] sorted = entries.toArray(Entry[]::new);
    // A parallel sort forks its tasks into the pool of the thread that runs it.
    pool.invoke(
        ForkJoinTask.adapt(
            () -> Arrays.parallelSort(sorted, Comparator.comparing(Entry::key, KEY_ORDER))));
    return sorted;
  }

  private Path nextRunPath() {
    return workDir.resolve(String.format("%s-%06d.run", name, runSeq++));
  }

  /** {@code sorted} without repeated keys. */
  private Iterator<Entry> distinct(final Iterator<Entry> sorted) {
    if (!distinctKeys) {
      return sorted;
    }
    final PeekingIterator<Entry> entries = Iterators.peekingIterator(sorted);
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return entries.hasNext();
      }

      @Override
      public Entry next() {
        final Entry entry = entries.next();
        while (entries.hasNext() && Arrays.equals(entries.peek().key(), entry.key())) {
          entries.next();
        }
        return entry;
      }
    };
  }

  private Iterator<Entry> merge(final List<Path> inputs) throws IOException {
    final PriorityQueue<RunReader> heap =
        new PriorityQueue<>(
            Math.max(1, inputs.size()), Comparator.comparing(r -> r.head.key(), KEY_ORDER));
    for (final Path input : inputs) {
      final RunReader reader = new RunReader(input);
      openReaders.add(reader);
      if (reader.advance()) {
        heap.add(reader);
      }
    }
    return distinct(merged(heap));
  }

  private static Iterator<Entry> merged(final PriorityQueue<RunReader> heap) {
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return !heap.isEmpty();
      }

      @Override
      public Entry next() {
        final RunReader best = heap.poll();
        if (best == null) {
          throw new NoSuchElementException();
        }
        final Entry entry = best.head;
        try {
          if (best.advance()) {
            heap.add(best);
          }
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
        return entry;
      }
    };
  }

  private void closeReaders() throws IOException {
    for (final RunReader reader : openReaders) {
      reader.close();
    }
    openReaders.clear();
  }

  /**
   * Run record: {@code shared[varint] | suffixLen[varint] | suffix | valueLen[varint] | value}.
   * Keys are front-coded: {@code shared} leading bytes are those of the previous key in the run,
   * which is sorted, so consecutive storage keys of one account drop their 33-byte account prefix.
   */
  private static final class RunWriter implements Closeable {
    private final DataOutputStream out;
    private byte[] previousKey = new byte[0];

    RunWriter(final Path path) throws IOException {
      this.out =
          new DataOutputStream(
              new BufferedOutputStream(Files.newOutputStream(path), RUN_IO_BUFFER_BYTES));
    }

    void write(final Entry entry) throws IOException {
      final byte[] key = entry.key();
      final int shared = sharedPrefix(previousKey, key);
      writeLength(shared);
      writeLength(key.length - shared);
      out.write(key, shared, key.length - shared);
      writeLength(entry.value().length);
      out.write(entry.value());
      previousKey = key;
    }

    private static int sharedPrefix(final byte[] a, final byte[] b) {
      final int mismatch = Arrays.mismatch(a, b);
      return mismatch < 0 ? a.length : mismatch;
    }

    /** Unsigned LEB128: one byte below 128, which covers nearly every length here. */
    private void writeLength(final int length) throws IOException {
      int remaining = length;
      while ((remaining & ~0x7F) != 0) {
        out.write((remaining & 0x7F) | 0x80);
        remaining >>>= 7;
      }
      out.write(remaining);
    }

    @Override
    public void close() throws IOException {
      out.close();
    }
  }

  private static final class RunReader implements Closeable {
    private final DataInputStream in;
    private Entry head;

    RunReader(final Path path) throws IOException {
      this.in =
          new DataInputStream(
              new BufferedInputStream(Files.newInputStream(path), RUN_IO_BUFFER_BYTES));
    }

    boolean advance() throws IOException {
      final int first = in.read();
      if (first < 0) {
        head = null;
        return false;
      }
      final int shared = readLength(first);
      final byte[] key = new byte[shared + readLength(in.readUnsignedByte())];
      System.arraycopy(head == null ? key : head.key(), 0, key, 0, shared);
      in.readFully(key, shared, key.length - shared);
      final byte[] value = new byte[readLength(in.readUnsignedByte())];
      in.readFully(value);
      head = new Entry(key, value);
      return true;
    }

    private int readLength(final int firstByte) throws IOException {
      int length = firstByte & 0x7F;
      int b = firstByte;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
        b = in.readUnsignedByte();
        length |= (b & 0x7F) << shift;
      }
      return length;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }

  /** Deletes {@code dir} and everything under it, if it exists; a failure is logged. */
  public static void deleteRecursively(final Path dir) {
    if (!Files.exists(dir)) {
      return;
    }
    try {
      MoreFiles.deleteRecursively(dir, RecursiveDeleteOption.ALLOW_INSECURE);
    } catch (final IOException e) {
      LOG.warn("Failed to delete {}", dir, e);
    }
  }
}
