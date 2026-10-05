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
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Canonical writer for EIP-8347 typed snapshots, fed leaves in strictly ascending PBT key order.
 *
 * <p>Records are tagged, so each is written as soon as its stem is complete and the file is written
 * in one pass, then closed by the end tag and the PBT root. Heap holds one stem (≤ 256 leaves).
 */
public final class Eip8347SnapshotWriter implements Closeable {

  /** A key is {@code zone[1] | hash[32] | ...}: the address hash, or a code group's stem hash. */
  private static final int HASH_OFFSET = 1;

  /** A storage key is {@code zone[1] | addressHash[32] | stemHash[32] | subIndex[1]}. */
  private static final int STORAGE_STEM_HASH_OFFSET = HASH_OFFSET + Bytes32.SIZE;

  private final OutputStream out;
  private final List<Leaf> stem = new ArrayList<>();
  private Bytes previousKey;
  private Bytes32 storageAddressHash;
  private boolean finished;

  private Eip8347SnapshotWriter(final Path path) throws IOException {
    this.out =
        new BufferedOutputStream(
            Files.newOutputStream(path), Eip8347TypedSnapshotCodec.IO_BUFFER_BYTES);
  }

  public static Eip8347SnapshotWriter open(final Path path) throws IOException {
    return new Eip8347SnapshotWriter(path);
  }

  /** Writes a whole snapshot from an unsorted, in-memory leaf list (tests and small fixtures). */
  public static void write(final Path path, final Bytes32 pbtRoot, final List<Leaf> leaves)
      throws IOException {
    final List<Leaf> sorted = new ArrayList<>(leaves);
    sorted.sort(Comparator.comparing(Leaf::key, Eip8347TypedSnapshotCodec::compare));
    try (final Eip8347SnapshotWriter writer = open(path)) {
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
    if (!stem.isEmpty() && !sameStem(stem.getFirst().key(), key)) {
      flushStem();
    }
    stem.add(leaf);
  }

  /** Completes the artifact: the last record, the end tag, then {@code pbtRoot}. */
  public void finish(final Bytes32 pbtRoot) throws IOException {
    ensureWritable();
    flushStem();
    out.write(Eip8347TypedSnapshotCodec.TAG_END);
    out.write(pbtRoot.toArrayUnsafe());
    finished = true;
    out.close();
  }

  @Override
  public void close() throws IOException {
    if (!finished) {
      finished = true;
      out.close();
    }
  }

  /**
   * Writes the completed stem as its record. Zones come in key order (account, code, storage), so
   * the records do too.
   */
  private void flushStem() throws IOException {
    if (stem.isEmpty()) {
      return;
    }
    final Bytes key = stem.getFirst().key();
    final Bytes32 firstHash = Bytes32.wrap(key.slice(HASH_OFFSET, Bytes32.SIZE));
    switch (key.get(0) & 0xFF) {
      case EmbeddingParameters.ACCOUNT_ZONE ->
          Eip8347TypedSnapshotCodec.writeHeaderRecord(
              out, HeaderRecord.fromLeaves(firstHash, stem));
      case EmbeddingParameters.CODE_ZONE ->
          Eip8347TypedSnapshotCodec.writeCodeGroup(out, Group.fromLeaves(firstHash, stem));
      case EmbeddingParameters.STORAGE_ZONE -> {
        if (!firstHash.equals(storageAddressHash)) {
          Eip8347TypedSnapshotCodec.writeStorageAccount(out, firstHash);
          storageAddressHash = firstHash;
        }
        Eip8347TypedSnapshotCodec.writeStorageGroup(
            out,
            Group.fromLeaves(
                Bytes32.wrap(key.slice(STORAGE_STEM_HASH_OFFSET, Bytes32.SIZE)), stem));
      }
      default ->
          throw new Eip8347ArtifactVerificationException(
              "unexpected zone 0x" + Integer.toHexString(key.get(0) & 0xFF));
    }
    stem.clear();
  }

  /** Whether two keys share their stem: every byte but the last. */
  private static boolean sameStem(final Bytes a, final Bytes b) {
    final byte[] x = a.toArrayUnsafe();
    final byte[] y = b.toArrayUnsafe();
    return x.length == y.length && Arrays.equals(x, 0, x.length - 1, y, 0, y.length - 1);
  }

  private void ensureWritable() {
    if (finished) {
      throw new IllegalStateException("snapshot writer already finished or closed");
    }
  }
}
