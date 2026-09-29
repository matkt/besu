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

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

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

    List<Eip8347SnapshotLeaf> leaves();

    int leafCount();
  }

  public record HeaderUnit(HeaderRecord header) implements Unit {
    @Override
    public Bytes stem() {
      return header.stem();
    }

    @Override
    public List<Eip8347SnapshotLeaf> leaves() {
      return header.leaves();
    }

    @Override
    public int leafCount() {
      return 2 + header.slots().size();
    }
  }

  public record CodeUnit(Group group) implements Unit {
    private static final Bytes PREFIX = Bytes.of((byte) EmbeddingParameters.CODE_ZONE);

    @Override
    public Bytes stem() {
      return Bytes.concatenate(PREFIX, group.stemHash());
    }

    @Override
    public List<Eip8347SnapshotLeaf> leaves() {
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
    public List<Eip8347SnapshotLeaf> leaves() {
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
    this.in = new BufferedInputStream(Files.newInputStream(path), 1 << 16);
    this.claimedRoot = Bytes32.wrap(Eip8347TypedSnapshotCodec.readFully(in, 32));
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

  /** Streams every derived leaf in PBT key order. */
  public void forEach(final Consumer<Eip8347SnapshotLeaf> consumer) throws IOException {
    for (Unit unit = next(); unit != null; unit = next()) {
      unit.leaves().forEach(consumer);
    }
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
    final Bytes32 addressHash = Bytes32.wrap(Eip8347TypedSnapshotCodec.readFully(in, 32));
    if (storageAddressHash != null
        && Eip8347TypedSnapshotCodec.compare(storageAddressHash, addressHash) >= 0) {
      throw new Eip8347ArtifactVerificationException(
          "storage records are not strictly ascending by addressHash at "
              + addressHash.toHexString());
    }
    storageAddressHash = addressHash;
    groupsLeft = Eip8347TypedSnapshotCodec.readUint(in, 8);
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
