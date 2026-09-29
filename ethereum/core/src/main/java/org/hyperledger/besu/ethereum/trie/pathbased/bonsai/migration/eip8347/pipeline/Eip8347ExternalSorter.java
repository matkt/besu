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
import java.io.EOFException;
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

/**
 * Bounded-memory external sort of {@code (key, value)} byte records, ordered by unsigned
 * byte-lexicographic key (the order EIP-8347 uses for every sorted artifact).
 *
 * <p>Records are buffered until {@code maxBufferedBytes}, then written as a sorted run. Runs are
 * merged at most {@code maxFanIn} at a time, in as many passes as needed, so neither heap nor open
 * file handles grow with the input. Equal keys are all kept; callers decide what a duplicate means.
 *
 * <p>Usage: {@link #add} any number of times, then {@link #sorted} once, then {@link #close}.
 */
public final class Eip8347ExternalSorter implements Closeable {

  /** Default in-heap buffer per sorter before spilling a run. */
  public static final long DEFAULT_BUFFER_BYTES = 64L << 20;

  /** Maximum runs merged at once (bounds open files and merge buffers). */
  public static final int MAX_FAN_IN = 64;

  /** Approximate heap cost of one buffered record beyond its payload. */
  private static final int RECORD_OVERHEAD_BYTES = 64;

  private static final Comparator<byte[]> KEY_ORDER = Arrays::compareUnsigned;

  /** One sorted record. */
  public record Entry(byte[] key, byte[] value) {}

  private final Path workDir;
  private final String name;
  private final long maxBufferedBytes;
  private final List<Entry> buffer = new ArrayList<>();
  private final List<Path> runs = new ArrayList<>();
  private final List<RunReader> openReaders = new ArrayList<>();
  private long bufferedBytes;
  private int runSeq;
  private boolean sorted;

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
    if (runs.isEmpty()) {
      buffer.sort(Comparator.comparing(Entry::key, KEY_ORDER));
      final Iterator<Entry> inMemory = List.copyOf(buffer).iterator();
      buffer.clear();
      return inMemory;
    }
    if (!buffer.isEmpty()) {
      spillBuffer();
    }
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
    closeReaders();
    for (final Path run : runs) {
      Files.deleteIfExists(run);
    }
    runs.clear();
  }

  private void spillBuffer() throws IOException {
    buffer.sort(Comparator.comparing(Entry::key, KEY_ORDER));
    final Path run = nextRunPath();
    try (final RunWriter out = new RunWriter(run)) {
      for (final Entry entry : buffer) {
        out.write(entry);
      }
    }
    runs.add(run);
    buffer.clear();
    bufferedBytes = 0;
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

  /** Run record: {@code keyLen[4] | key | valueLen[4] | value}. */
  private static final class RunWriter implements Closeable {
    private final DataOutputStream out;

    RunWriter(final Path path) throws IOException {
      this.out =
          new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path), 1 << 16));
    }

    void write(final Entry entry) throws IOException {
      out.writeInt(entry.key().length);
      out.write(entry.key());
      out.writeInt(entry.value().length);
      out.write(entry.value());
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
      this.in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path), 1 << 16));
    }

    boolean advance() throws IOException {
      final int keyLength;
      try {
        keyLength = in.readInt();
      } catch (final EOFException endOfRun) {
        head = null;
        return false;
      }
      final byte[] key = new byte[keyLength];
      in.readFully(key);
      final byte[] value = new byte[in.readInt()];
      in.readFully(value);
      head = new Entry(key, value);
      return true;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }
}
