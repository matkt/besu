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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact;

import org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.Group;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.HeaderRecord;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.Leaf;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.google.common.io.CountingOutputStream;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Canonical writer for EIP-8347 typed snapshots, fed leaves in strictly ascending PBT key order.
 *
 * <p>Heap holds one stem (≤256 leaves) plus at most {@link PendingGroups#MEMORY_LIMIT} bytes of the
 * current storage record; larger storage records spill to a temp file in the given spill directory.
 * The root and section counts are patched in place once the stream is complete.
 */
public final class Eip8347SnapshotWriter implements Closeable {

  private static final int HEADER = 0;
  private static final int CODE = 1;
  private static final int STORAGE = 2;
  private static final int SECTIONS = 3;

  /** A key is {@code zone[1] | hash[32] | ...}: the address hash, or a code group's stem hash. */
  private static final int HASH_OFFSET = 1;

  /** A storage key is {@code zone[1] | addressHash[32] | stemHash[32] | subIndex[1]}. */
  private static final int STORAGE_STEM_HASH_OFFSET = HASH_OFFSET + Bytes32.SIZE;

  private final Path path;
  private final CountingOutputStream out;
  private final PendingGroups pendingGroups;
  private final long[] countPositions = new long[SECTIONS];
  private final long[] counts = new long[SECTIONS];

  private int section = HEADER;
  private Bytes previousKey;
  private final List<Leaf> stem = new ArrayList<>();
  private Bytes32 storageAddressHash;
  private boolean finished;

  private Eip8347SnapshotWriter(final Path path, final Path spillDir) throws IOException {
    this.path = path;
    this.out =
        new CountingOutputStream(
            new BufferedOutputStream(
                Files.newOutputStream(path), Eip8347TypedSnapshotCodec.IO_BUFFER_BYTES));
    this.pendingGroups = new PendingGroups(spillDir);
    out.write(new byte[Bytes32.SIZE]); // pbtRoot, patched by finish()
    countPositions[HEADER] = out.getCount();
    Eip8347TypedSnapshotCodec.writeCount(out, 0);
  }

  public static Eip8347SnapshotWriter open(final Path path, final Path spillDir)
      throws IOException {
    return new Eip8347SnapshotWriter(path, spillDir);
  }

  /** Writes a whole snapshot from an unsorted, in-memory leaf list (tests and small fixtures). */
  public static void write(final Path path, final Bytes32 pbtRoot, final List<Leaf> leaves)
      throws IOException {
    final List<Leaf> sorted = new ArrayList<>(leaves);
    sorted.sort(Comparator.comparing(Leaf::key, Eip8347TypedSnapshotCodec::compare));
    try (final Eip8347SnapshotWriter writer = open(path, path.toAbsolutePath().getParent())) {
      for (final Leaf leaf : sorted) {
        writer.accept(leaf);
      }
      writer.finish(pbtRoot);
    }
  }

  public void accept(final Leaf leaf) throws IOException {
    ensureWritable();
    final Bytes key = leaf.key();
    if (previousKey != null && Eip8347TypedSnapshotCodec.compare(previousKey, key) >= 0) {
      throw new Eip8347ArtifactVerificationException(
          "duplicate or non-ascending snapshot leaf key " + key.toHexString());
    }
    previousKey = key;
    advanceTo(sectionOf(key));
    if (!stem.isEmpty()
        && !Eip8347TypedSnapshotCodec.stemOf(stem.getFirst().key())
            .equals(Eip8347TypedSnapshotCodec.stemOf(key))) {
      flushStem();
    }
    stem.add(leaf);
  }

  /** Completes the artifact with {@code pbtRoot}. */
  public void finish(final Bytes32 pbtRoot) throws IOException {
    ensureWritable();
    advanceTo(STORAGE);
    flushStem();
    flushStorageRecord();
    finished = true;
    out.close();
    try (final FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
      writeAt(channel, 0, pbtRoot.toArrayUnsafe());
      for (int s = HEADER; s <= STORAGE; s++) {
        final ByteArrayOutputStream count = new ByteArrayOutputStream(Long.BYTES);
        Eip8347TypedSnapshotCodec.writeCount(count, counts[s]);
        writeAt(channel, countPositions[s], count.toByteArray());
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (!finished) {
      finished = true;
      out.close();
    }
    pendingGroups.close();
  }

  /** Closes the current section and opens the empty ones up to {@code target}. */
  private void advanceTo(final int target) throws IOException {
    if (target == section) {
      return;
    }
    if (target < section) {
      throw new Eip8347ArtifactVerificationException("snapshot leaves go back to an earlier zone");
    }
    flushStem();
    flushStorageRecord();
    while (section < target) {
      section++;
      countPositions[section] = out.getCount();
      Eip8347TypedSnapshotCodec.writeCount(out, 0);
    }
  }

  private void flushStem() throws IOException {
    if (stem.isEmpty()) {
      return;
    }
    final Bytes key = stem.getFirst().key();
    final Bytes32 firstHash = Bytes32.wrap(key.slice(HASH_OFFSET, Bytes32.SIZE));
    switch (section) {
      case HEADER -> {
        Eip8347TypedSnapshotCodec.writeHeaderRecord(out, HeaderRecord.fromLeaves(firstHash, stem));
        counts[HEADER]++;
      }
      case CODE -> {
        Eip8347TypedSnapshotCodec.writeGroup(out, Group.fromLeaves(firstHash, stem));
        counts[CODE]++;
      }
      default -> {
        if (!firstHash.equals(storageAddressHash)) {
          flushStorageRecord();
          storageAddressHash = firstHash;
        }
        pendingGroups.add(
            Group.fromLeaves(
                Bytes32.wrap(key.slice(STORAGE_STEM_HASH_OFFSET, Bytes32.SIZE)), stem));
      }
    }
    stem.clear();
  }

  private void flushStorageRecord() throws IOException {
    if (pendingGroups.count() == 0) {
      return;
    }
    out.write(storageAddressHash.toArrayUnsafe());
    Eip8347TypedSnapshotCodec.writeUint(
        out, pendingGroups.count(), Eip8347TypedSnapshotCodec.GROUP_COUNT_WIDTH);
    pendingGroups.drainTo(out);
    counts[STORAGE]++;
  }

  private void ensureWritable() {
    if (finished) {
      throw new IllegalStateException("snapshot writer already finished or closed");
    }
  }

  private static int sectionOf(final Bytes key) {
    return switch (key.get(0) & 0xFF) {
      case EmbeddingParameters.ACCOUNT_ZONE -> HEADER;
      case EmbeddingParameters.CODE_ZONE -> CODE;
      case EmbeddingParameters.STORAGE_ZONE -> STORAGE;
      default ->
          throw new Eip8347ArtifactVerificationException(
              "unexpected zone 0x" + Integer.toHexString(key.get(0) & 0xFF));
    };
  }

  private static void writeAt(final FileChannel channel, final long position, final byte[] bytes)
      throws IOException {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    long at = position;
    while (buffer.hasRemaining()) {
      at += channel.write(buffer, at);
    }
  }

  /**
   * Encoded groups of the storage record being built: its {@code groupCount} precedes them, so they
   * are held until the record ends. Past {@link #MEMORY_LIMIT} bytes they move to a temp file.
   */
  private static final class PendingGroups implements Closeable {
    static final int MEMORY_LIMIT = 8 << 20;

    private final Path tempDir;
    private final ByteArrayOutputStream memory = new ByteArrayOutputStream();
    private Path spillFile;
    private OutputStream spill;
    private long count;

    PendingGroups(final Path tempDir) {
      this.tempDir = tempDir;
    }

    long count() {
      return count;
    }

    void add(final Group group) throws IOException {
      if (spill == null && memory.size() >= MEMORY_LIMIT) {
        spillFile = Files.createTempFile(tempDir, "eip8347-storage-record-", ".tmp");
        spill =
            new BufferedOutputStream(
                Files.newOutputStream(spillFile), Eip8347TypedSnapshotCodec.IO_BUFFER_BYTES);
        memory.writeTo(spill);
        memory.reset();
      }
      Eip8347TypedSnapshotCodec.writeGroup(spill != null ? spill : memory, group);
      count++;
    }

    void drainTo(final OutputStream target) throws IOException {
      if (spill != null) {
        spill.close();
        try (final InputStream in = Files.newInputStream(spillFile)) {
          in.transferTo(target);
        }
        deleteSpill();
      } else {
        memory.writeTo(target);
        memory.reset();
      }
      count = 0;
    }

    @Override
    public void close() throws IOException {
      if (spill != null) {
        spill.close();
        deleteSpill();
      }
    }

    private void deleteSpill() throws IOException {
      Files.deleteIfExists(spillFile);
      spill = null;
      spillFile = null;
    }
  }
}
