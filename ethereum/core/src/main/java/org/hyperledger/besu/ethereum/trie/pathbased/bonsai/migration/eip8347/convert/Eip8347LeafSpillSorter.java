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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.convert;

import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieConstants;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.AscendingCollapseBinaryTrie;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Bounded-memory external sort of PBT leaves for EIP-8347 snapshot generation.
 *
 * <p>Leaves are buffered up to {@code runCapacity}; each full buffer is flushed as a key-sorted run
 * on disk, then k-way merged into the snapshot while hashing. Heap holds at most one run of leaves
 * plus one record per open run during the merge — never the full leaf set.
 *
 * <p>Spill record: {@code keyLen[2 BE] | key | value[32]}.
 */
final class Eip8347LeafSpillSorter implements Closeable {

  /** Default leaves per sorted run. */
  static final int DEFAULT_RUN_CAPACITY = 65_536;

  private final Path workDir;
  private final int runCapacity;
  private final List<SpilledLeaf> buffer = new ArrayList<>();
  private final List<Path> runs = new ArrayList<>();
  private int runSeq;
  private boolean closed;

  Eip8347LeafSpillSorter(final Path workDir, final int runCapacity) throws IOException {
    if (runCapacity < 1) {
      throw new IllegalArgumentException("runCapacity must be positive");
    }
    this.workDir = workDir;
    this.runCapacity = runCapacity;
    Files.createDirectories(workDir);
  }

  void accept(final Bytes key, final Bytes32 value) throws IOException {
    ensureOpen();
    if (Bytes32.ZERO.equals(value)) {
      throw new Eip8347ArtifactVerificationException(
          "refusing to spill zero-valued PBT leaf for key " + key.toHexString());
    }
    buffer.add(new SpilledLeaf(key.copy(), value));
    if (buffer.size() >= runCapacity) {
      flushRun();
    }
  }

  /**
   * Flushes remaining buffer, k-way merges runs into {@code snapshotPath} (placeholder header then
   * patched), hashing with {@link AscendingCollapseBinaryTrie} in the same pass.
   */
  Eip8347SnapshotGenerator.Result finishToSnapshot(final Path snapshotPath) throws IOException {
    ensureOpen();
    if (!buffer.isEmpty()) {
      flushRun();
    }
    if (runs.isEmpty()) {
      try (final var out = Files.newOutputStream(snapshotPath)) {
        out.write(TrieConstants.EMPTY_TRIE_ROOT.toArray());
        out.write(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(0L).array());
      }
      return new Eip8347SnapshotGenerator.Result(TrieConstants.EMPTY_TRIE_ROOT, 0);
    }
    return mergeRunsToSnapshot(snapshotPath);
  }

  @Override
  public void close() throws IOException {
    closed = true;
    buffer.clear();
    IOException first = null;
    for (final Path run : runs) {
      try {
        Files.deleteIfExists(run);
      } catch (final IOException e) {
        if (first == null) {
          first = e;
        } else {
          first.addSuppressed(e);
        }
      }
    }
    runs.clear();
    if (first != null) {
      throw first;
    }
  }

  private void flushRun() throws IOException {
    buffer.sort(Comparator.comparing(SpilledLeaf::key));
    final Path runPath = workDir.resolve(String.format("run-%05d.bin", runSeq++));
    try (final DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(runPath)))) {
      SpilledLeaf pending = null;
      for (final SpilledLeaf leaf : buffer) {
        if (pending == null) {
          pending = leaf;
          continue;
        }
        final int cmp = leaf.key().compareTo(pending.key());
        if (cmp == 0) {
          if (!leaf.value().equals(pending.value())) {
            throw new Eip8347ArtifactVerificationException(
                "duplicate PBT key with conflicting values: " + leaf.key().toHexString());
          }
        } else if (cmp > 0) {
          writeRecord(out, pending);
          pending = leaf;
        } else {
          throw new IllegalStateException("run buffer not sorted");
        }
      }
      if (pending != null) {
        writeRecord(out, pending);
      }
    }
    buffer.clear();
    runs.add(runPath);
  }

  private Eip8347SnapshotGenerator.Result mergeRunsToSnapshot(final Path snapshotPath)
      throws IOException {
    final List<RunCursor> cursors = new ArrayList<>(runs.size());
    try {
      for (final Path run : runs) {
        final RunCursor cursor = RunCursor.open(run);
        if (cursor.leaf != null) {
          cursors.add(cursor);
        } else {
          cursor.close();
        }
      }

      final PriorityQueue<RunCursor> heap =
          new PriorityQueue<>(Comparator.comparing(c -> c.leaf.key()));
      heap.addAll(cursors);

      try (final RandomAccessFile raf = new RandomAccessFile(snapshotPath.toFile(), "rw")) {
        raf.setLength(0);
        raf.write(new byte[40]); // placeholder: pbtRoot[32] | leafCount[8]

        final AscendingCollapseBinaryTrie pbt = new AscendingCollapseBinaryTrie();
        long leafCount = 0;
        Bytes lastKey = null;
        Bytes32 lastValue = null;

        while (!heap.isEmpty()) {
          final RunCursor best = heap.poll();
          final SpilledLeaf leaf = best.leaf;

          if (lastKey != null) {
            final int cmp = leaf.key().compareTo(lastKey);
            if (cmp < 0) {
              throw new Eip8347ArtifactVerificationException(
                  "external merge produced non-ascending PBT keys");
            }
            if (cmp == 0) {
              if (!leaf.value().equals(lastValue)) {
                throw new Eip8347ArtifactVerificationException(
                    "duplicate PBT key with conflicting values: " + leaf.key().toHexString());
              }
              if (best.advance()) {
                heap.add(best);
              }
              continue;
            }
          }

          pbt.insert(leaf.key(), leaf.value());
          writeSnapshotLeafRlp(raf, leaf);
          lastKey = leaf.key();
          lastValue = leaf.value();
          leafCount++;

          if (best.advance()) {
            heap.add(best);
          }
        }

        final Bytes32 pbtRoot = leafCount == 0 ? TrieConstants.EMPTY_TRIE_ROOT : pbt.rootHash();
        raf.seek(0);
        raf.write(pbtRoot.toArrayUnsafe());
        raf.write(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(leafCount).array());
        return new Eip8347SnapshotGenerator.Result(pbtRoot, leafCount);
      }
    } finally {
      for (final RunCursor cursor : cursors) {
        cursor.closeQuietly();
      }
    }
  }

  private static void writeRecord(final DataOutputStream out, final SpilledLeaf leaf)
      throws IOException {
    final byte[] key = leaf.key().toArrayUnsafe();
    if (key.length > 0xFFFF) {
      throw new Eip8347ArtifactVerificationException("PBT key too long: " + key.length);
    }
    out.writeShort(key.length);
    out.write(key);
    out.write(leaf.value().toArrayUnsafe());
  }

  private static void writeSnapshotLeafRlp(final RandomAccessFile raf, final SpilledLeaf leaf)
      throws IOException {
    final Bytes value = leaf.value().trimLeadingZeros();
    if (value.isEmpty()) {
      throw new Eip8347ArtifactVerificationException("cannot write zero-valued snapshot leaf");
    }
    final Bytes encoded =
        RLP.encode(
            rlp -> {
              rlp.startList();
              rlp.writeBytes(leaf.key());
              rlp.writeBytes(value);
              rlp.endList();
            });
    raf.write(encoded.toArrayUnsafe());
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("leaf spill sorter already closed");
    }
  }

  private record SpilledLeaf(Bytes key, Bytes32 value) {}

  private static final class RunCursor implements Closeable {
    private final DataInputStream in;
    private SpilledLeaf leaf;

    private RunCursor(final DataInputStream in) {
      this.in = in;
    }

    static RunCursor open(final Path path) throws IOException {
      final RunCursor cursor =
          new RunCursor(new DataInputStream(new BufferedInputStream(Files.newInputStream(path))));
      cursor.advance();
      return cursor;
    }

    boolean advance() throws IOException {
      try {
        final int keyLen = in.readUnsignedShort();
        final byte[] keyBytes = in.readNBytes(keyLen);
        if (keyBytes.length != keyLen) {
          throw new EOFException("truncated spill key");
        }
        final byte[] valueBytes = in.readNBytes(32);
        if (valueBytes.length != 32) {
          throw new EOFException("truncated spill value");
        }
        leaf = new SpilledLeaf(Bytes.wrap(keyBytes), Bytes32.wrap(valueBytes));
        return true;
      } catch (final EOFException e) {
        leaf = null;
        return false;
      }
    }

    void closeQuietly() {
      try {
        close();
      } catch (final IOException ignored) {
        // best-effort
      }
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }
}
