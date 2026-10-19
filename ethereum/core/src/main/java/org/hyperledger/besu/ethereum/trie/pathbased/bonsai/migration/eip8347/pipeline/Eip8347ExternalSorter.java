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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded-memory external sort of {@code (key, value)} byte records, ordered by unsigned
 * byte-lexicographic key (the order EIP-8347 uses for every sorted artifact).
 *
 * <p>Records are buffered until {@code maxBufferedBytes}, then written as a sorted run. Runs are
 * merged at most {@link #MAX_FAN_IN} at a time, in as many passes as needed, so neither heap nor
 * open file handles grow with the input. Equal keys are all kept; callers decide what a duplicate
 * means.
 *
 * <p>A full buffer is sorted (in parallel) and written on a background thread while the next one
 * fills, so the producer only waits when it fills a buffer before the previous run is written. Heap
 * is therefore up to two buffers.
 *
 * <p>Usage: {@link #add} any number of times, then {@link #sorted} once, then {@link #close}.
 */
public final class Eip8347ExternalSorter implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(Eip8347ExternalSorter.class);

  /** Default in-heap buffer per sorter before spilling a run. */
  public static final long DEFAULT_BUFFER_BYTES = 64L << 20;

  /**
   * Maximum runs merged at once (bounds open files and merge buffers: 512 × 64 KiB). High enough
   * that a mainnet-sized sort merges in a single pass.
   */
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
  private List<Entry> buffer = new ArrayList<>();
  private final List<Path> runs = new ArrayList<>();
  private final List<RunReader> openReaders = new ArrayList<>();
  private long bufferedBytes;
  private int runSeq;
  private boolean sorted;

  /** Writes runs in the background; created with the first run. */
  private ExecutorService spiller;

  /** The run being written, if any. */
  private CompletableFuture<Path> pendingRun;

  public Eip8347ExternalSorter(final Path workDir, final String name, final long maxBufferedBytes) {
    if (maxBufferedBytes < 1) {
      throw new IllegalArgumentException("maxBufferedBytes must be positive");
    }
    this.workDir = workDir;
    this.name = name;
    this.maxBufferedBytes = maxBufferedBytes;
  }

  public void add(final byte[] key, final byte[] value) throws IOException {
    if (sorted) {
      throw new IllegalStateException(name + " sorter already sorted");
    }
    buffer.add(new Entry(key, value));
    bufferedBytes += key.length + value.length + RECORD_OVERHEAD_BYTES;
    if (bufferedBytes >= maxBufferedBytes) {
      spillBuffer();
    }
  }

  /** Ends the add phase and returns every record in ascending key order. Call once. */
  public Iterator<Entry> sorted() throws IOException {
    if (sorted) {
      throw new IllegalStateException(name + " sorter already sorted");
    }
    sorted = true;
    if (runs.isEmpty() && pendingRun == null) {
      return Arrays.asList(sort(buffer)).iterator();
    }
    if (!buffer.isEmpty()) {
      spillBuffer();
    }
    awaitPendingRun();
    while (runs.size() > MAX_FAN_IN) {
      final List<Path> batch = new ArrayList<>(runs.subList(0, MAX_FAN_IN));
      runs.subList(0, MAX_FAN_IN).clear();
      final Path merged = nextRunPath();
      try (final RunWriter out = new RunWriter(merged)) {
        final Iterator<Entry> it = merge(batch);
        while (it.hasNext()) {
          out.write(it.next());
        }
      }
      closeReaders();
      for (final Path run : batch) {
        Files.deleteIfExists(run);
      }
      runs.add(merged);
    }
    return merge(runs);
  }

  @Override
  public void close() throws IOException {
    buffer.clear();
    if (pendingRun != null) {
      // Wait for the writer so its file can be deleted. A failure was reported by add() or
      // sorted().
      final Path run = pendingRun.exceptionally(failure -> null).join();
      if (run != null) {
        runs.add(run);
      }
      pendingRun = null;
    }
    if (spiller != null) {
      spiller.shutdownNow();
    }
    closeReaders();
    for (final Path run : runs) {
      Files.deleteIfExists(run);
    }
    runs.clear();
  }

  /** Hands the full buffer to the background writer, once the previous run is written. */
  private void spillBuffer() throws IOException {
    awaitPendingRun();
    final List<Entry> full = buffer;
    final Path run = nextRunPath();
    buffer = new ArrayList<>();
    bufferedBytes = 0;
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
              try (final RunWriter out = new RunWriter(run)) {
                for (final Entry entry : sort(full)) {
                  out.write(entry);
                }
                return run;
              } catch (final IOException e) {
                throw new UncheckedIOException(e);
              }
            },
            spiller);
  }

  private void awaitPendingRun() throws IOException {
    if (pendingRun == null) {
      return;
    }
    final CompletableFuture<Path> run = pendingRun;
    pendingRun = null;
    runs.add(Eip8347Pipelines.await(run));
  }

  private static Entry[] sort(final List<Entry> entries) {
    final Entry[] sorted = entries.toArray(Entry[]::new);
    Arrays.parallelSort(sorted, Comparator.comparing(Entry::key, KEY_ORDER));
    return sorted;
  }

  private Path nextRunPath() {
    return workDir.resolve(String.format("%s-%06d.run", name, runSeq++));
  }

  private Iterator<Entry> merge(final List<Path> inputs) throws IOException {
    final PriorityQueue<RunReader> heap =
        new PriorityQueue<>(inputs.size(), Comparator.comparing(r -> r.head.key(), KEY_ORDER));
    for (final Path input : inputs) {
      final RunReader reader = new RunReader(input);
      openReaders.add(reader);
      if (reader.advance()) {
        heap.add(reader);
      }
    }
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

  /** Deletes {@code dir} and everything under it; missing or undeletable paths are ignored. */
  public static void deleteRecursively(final Path dir) {
    if (!Files.exists(dir)) {
      return;
    }
    try (final var walk = Files.walk(dir)) {
      walk.sorted(Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (final IOException e) {
                  LOG.debug("failed deleting spill path {}", p, e);
                }
              });
    } catch (final IOException e) {
      LOG.debug("failed walking spill dir {}", dir, e);
    }
  }
}
