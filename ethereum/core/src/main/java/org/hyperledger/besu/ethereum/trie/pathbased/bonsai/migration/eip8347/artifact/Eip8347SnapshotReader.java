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

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Sequential reader of an EIP-8347 typed snapshot, one record ("unit") at a time.
 *
 * <p>{@code record * | end | pbtRoot[32]}, each record opening with its tag (see {@link
 * Eip8347TypedSnapshotCodec}). Enforces the byte-canonical layout: units strictly ascending by
 * stem, so derived leaves are strictly ascending in PBT key order and records come in zone order
 * (account {@code 0x00…}, code {@code 0x01…}, storage {@code 0xff…}); storage accounts strictly
 * ascending by {@code addressHash} and each followed by at least one storage group, the one-leaf
 * storage group encoding for exactly the one-leaf groups, and no trailing byte. Memory is one unit.
 */
public final class Eip8347SnapshotReader implements Closeable {

  /** One typed record together with the PBT leaves it derives. */
  public sealed interface Unit permits HeaderUnit, CodeUnit, StorageUnit {
    Bytes stem();

    List<Leaf> leaves();

    int leafCount();
  }

  public record HeaderUnit(HeaderRecord header) implements Unit {
    @Override
    public Bytes stem() {
      return header.stem();
    }

    @Override
    public List<Leaf> leaves() {
      return header.leaves();
    }

    @Override
    public int leafCount() {
      return header.leafCount();
    }
  }

  public record CodeUnit(Group group) implements Unit {
    private static final Bytes PREFIX = Bytes.of((byte) EmbeddingParameters.CODE_ZONE);

    @Override
    public Bytes stem() {
      return Bytes.concatenate(PREFIX, group.stemHash());
    }

    @Override
    public List<Leaf> leaves() {
      return group.leaves(PREFIX);
    }

    @Override
    public int leafCount() {
      return group.entries().size();
    }
  }

  /** One group of a storage record; {@code addressHash} is the record's. */
  public record StorageUnit(Bytes32 addressHash, Group group) implements Unit {
    @Override
    public Bytes stem() {
      return Bytes.concatenate(
          Eip8347TypedSnapshotCodec.storagePrefix(addressHash), group.stemHash());
    }

    @Override
    public List<Leaf> leaves() {
      return group.leaves(Eip8347TypedSnapshotCodec.storagePrefix(addressHash));
    }

    @Override
    public int leafCount() {
      return group.entries().size();
    }
  }

  private final InputStream in;

  /** The storage account the next storage groups belong to, and whether one has been read yet. */
  private Bytes32 storageAddressHash;

  private boolean storageAccountHasGroup = true;
  private Bytes previousStem;
  private long leafCount;
  private Bytes32 claimedRoot;

  public Eip8347SnapshotReader(final Path path) throws IOException {
    this.in =
        new BufferedInputStream(
            Files.newInputStream(path), Eip8347TypedSnapshotCodec.IO_BUFFER_BYTES);
  }

  /**
   * The PBT root the snapshot claims, read after its last record.
   *
   * @throws IllegalStateException before {@link #next} has returned {@code null}
   */
  public Bytes32 claimedRoot() {
    if (claimedRoot == null) {
      throw new IllegalStateException("the claimed root follows the last record");
    }
    return claimedRoot;
  }

  /** Leaves derived by the units read so far. */
  public long leafCount() {
    return leafCount;
  }

  /** Next unit in file order, or {@code null} once the snapshot is fully and validly consumed. */
  public Unit next() throws IOException {
    if (claimedRoot != null) {
      return null;
    }
    while (true) {
      final int tag = Eip8347TypedSnapshotCodec.readByte(in);
      switch (tag) {
        case Eip8347TypedSnapshotCodec.KIND_NONE,
            Eip8347TypedSnapshotCodec.KIND_CODE,
            Eip8347TypedSnapshotCodec.KIND_DELEGATION -> {
          requireStorageAccountHasGroup();
          return accept(new HeaderUnit(Eip8347TypedSnapshotCodec.readHeaderRecord(in, tag)));
        }
        case Eip8347TypedSnapshotCodec.TAG_CODE_GROUP -> {
          requireStorageAccountHasGroup();
          return accept(new CodeUnit(Eip8347TypedSnapshotCodec.readGroup(in)));
        }
        case Eip8347TypedSnapshotCodec.TAG_STORAGE_ACCOUNT -> beginStorageAccount();
        case Eip8347TypedSnapshotCodec.TAG_SINGLE_STORAGE_GROUP -> {
          return accept(storageGroup(Eip8347TypedSnapshotCodec.readSingleGroup(in)));
        }
        case Eip8347TypedSnapshotCodec.TAG_STORAGE_GROUP -> {
          final Group group = Eip8347TypedSnapshotCodec.readGroup(in);
          if (group.entries().size() == 1) {
            throw new Eip8347ArtifactVerificationException(
                "a one-leaf storage group must use the single-group encoding");
          }
          return accept(storageGroup(group));
        }
        case Eip8347TypedSnapshotCodec.TAG_END -> {
          requireStorageAccountHasGroup();
          claimedRoot = Bytes32.wrap(Eip8347TypedSnapshotCodec.readFully(in, Bytes32.SIZE));
          if (in.read() != -1) {
            throw new Eip8347ArtifactVerificationException("trailing bytes after snapshot");
          }
          return null;
        }
        default ->
            throw new Eip8347ArtifactVerificationException(
                "invalid snapshot record tag 0x" + Integer.toHexString(tag));
      }
    }
  }

  /** The remaining units as an iterator over {@link #next}; I/O failures are unchecked. */
  public Iterator<Unit> units() {
    return new Iterator<>() {
      private Unit pending;

      @Override
      public boolean hasNext() {
        if (pending == null && claimedRoot == null) {
          try {
            pending = Eip8347SnapshotReader.this.next();
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        }
        return pending != null;
      }

      @Override
      public Unit next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final Unit unit = pending;
        pending = null;
        return unit;
      }
    };
  }

  /** Fails unless {@link #next} has already returned {@code null}. */
  public void ensureExhausted() {
    if (claimedRoot == null) {
      throw new Eip8347ArtifactVerificationException("snapshot has unread records");
    }
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  private void beginStorageAccount() throws IOException {
    requireStorageAccountHasGroup();
    final Bytes32 addressHash = Bytes32.wrap(Eip8347TypedSnapshotCodec.readFully(in, Bytes32.SIZE));
    if (storageAddressHash != null
        && Eip8347TypedSnapshotCodec.compare(storageAddressHash, addressHash) >= 0) {
      throw new Eip8347ArtifactVerificationException(
          "storage records are not strictly ascending by addressHash at "
              + addressHash.toHexString());
    }
    storageAddressHash = addressHash;
    storageAccountHasGroup = false;
  }

  private StorageUnit storageGroup(final Group group) {
    if (storageAddressHash == null) {
      throw new Eip8347ArtifactVerificationException(
          "storage group without a storage account before it");
    }
    storageAccountHasGroup = true;
    return new StorageUnit(storageAddressHash, group);
  }

  private void requireStorageAccountHasGroup() {
    if (!storageAccountHasGroup) {
      throw new Eip8347ArtifactVerificationException(
          "storage account "
              + storageAddressHash.toHexString()
              + " is not followed by a storage group");
    }
  }

  private Unit accept(final Unit unit) {
    final Bytes stem = unit.stem();
    if (previousStem != null && Eip8347TypedSnapshotCodec.compare(previousStem, stem) >= 0) {
      throw new Eip8347ArtifactVerificationException(
          "snapshot records are not strictly ascending in PBT key order at " + stem.toHexString());
    }
    previousStem = stem;
    leafCount += unit.leafCount();
    return unit;
  }
}
