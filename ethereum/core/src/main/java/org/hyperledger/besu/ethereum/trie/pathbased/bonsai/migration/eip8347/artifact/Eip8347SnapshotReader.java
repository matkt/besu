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
 * <p>{@code pbtRoot[32] | headerCount[8] | headers | codeCount[8] | code groups | storageCount[8] |
 * storage records}. Enforces the byte-canonical layout: units strictly ascending by stem (so
 * derived leaves are strictly ascending in PBT key order), storage records strictly ascending by
 * {@code addressHash} with a non-zero {@code groupCount}, and no trailing byte. Memory is one unit.
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

  private enum Section {
    HEADER,
    CODE,
    STORAGE,
    DONE
  }

  private final InputStream in;
  private final Bytes32 claimedRoot;

  private Section section = Section.HEADER;
  private long recordsLeft;
  private long groupsLeft;
  private Bytes32 storageAddressHash;
  private Bytes previousStem;
  private long leafCount;

  public Eip8347SnapshotReader(final Path path) throws IOException {
    this.in =
        new BufferedInputStream(
            Files.newInputStream(path), Eip8347TypedSnapshotCodec.IO_BUFFER_BYTES);
    this.claimedRoot = Bytes32.wrap(Eip8347TypedSnapshotCodec.readFully(in, Bytes32.SIZE));
    this.recordsLeft = Eip8347TypedSnapshotCodec.readCount(in);
  }

  public Bytes32 claimedRoot() {
    return claimedRoot;
  }

  /** Leaves derived by the units read so far. */
  public long leafCount() {
    return leafCount;
  }

  /** Next unit in file order, or {@code null} once the snapshot is fully and validly consumed. */
  public Unit next() throws IOException {
    while (true) {
      switch (section) {
        case HEADER -> {
          if (recordsLeft > 0) {
            recordsLeft--;
            return accept(new HeaderUnit(Eip8347TypedSnapshotCodec.readHeaderRecord(in)));
          }
          enter(Section.CODE);
        }
        case CODE -> {
          if (recordsLeft > 0) {
            recordsLeft--;
            return accept(new CodeUnit(Eip8347TypedSnapshotCodec.readGroup(in)));
          }
          enter(Section.STORAGE);
        }
        case STORAGE -> {
          if (groupsLeft > 0) {
            groupsLeft--;
            return accept(
                new StorageUnit(storageAddressHash, Eip8347TypedSnapshotCodec.readGroup(in)));
          }
          if (recordsLeft > 0) {
            recordsLeft--;
            beginStorageRecord();
          } else {
            section = Section.DONE;
            if (in.read() != -1) {
              throw new Eip8347ArtifactVerificationException("trailing bytes after snapshot");
            }
          }
        }
        case DONE -> {
          return null;
        }
      }
    }
  }

  /** The remaining units as an iterator over {@link #next}; I/O failures are unchecked. */
  public Iterator<Unit> units() {
    return new Iterator<>() {
      private Unit pending;

      @Override
      public boolean hasNext() {
        if (pending == null && section != Section.DONE) {
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
    if (section != Section.DONE) {
      throw new Eip8347ArtifactVerificationException(
          "snapshot has unread records in section " + section);
    }
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  private void enter(final Section next) throws IOException {
    section = next;
    recordsLeft = Eip8347TypedSnapshotCodec.readCount(in);
  }

  private void beginStorageRecord() throws IOException {
    final Bytes32 addressHash = Bytes32.wrap(Eip8347TypedSnapshotCodec.readFully(in, Bytes32.SIZE));
    if (storageAddressHash != null
        && Eip8347TypedSnapshotCodec.compare(storageAddressHash, addressHash) >= 0) {
      throw new Eip8347ArtifactVerificationException(
          "storage records are not strictly ascending by addressHash at "
              + addressHash.toHexString());
    }
    storageAddressHash = addressHash;
    groupsLeft =
        Eip8347TypedSnapshotCodec.readUint(in, Eip8347TypedSnapshotCodec.GROUP_COUNT_WIDTH);
    if (groupsLeft == 0L) {
      throw new Eip8347ArtifactVerificationException(
          "storage groupCount must be non-zero for " + addressHash.toHexString());
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
