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

import static org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters.ACCOUNT_ZONE;
import static org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters.BASIC_DATA_LEAF_KEY;
import static org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters.CODE_HASH_LEAF_KEY;
import static org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters.CODE_ZONE;
import static org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters.DELEGATION_CODE_SIZE;
import static org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters.DELEGATION_LEAF_KEY;
import static org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters.HEADER_STORAGE_OFFSET;
import static org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters.HEADER_STORAGE_SLOTS;
import static org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters.STEM_SUBTREE_WIDTH;
import static org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters.STORAGE_ZONE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.BasicDataEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.DelegationEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Typed, stem-grouped records of the EIP-8347 PBT snapshot and their leaf derivation.
 *
 * <pre>
 * headerRecord  = addressHash[32] | nonce[≤8] | balance[≤16] | kind[1] | codeRef
 *               | slotCount[1] | (slot[1] | value[≤32]) * slotCount
 * group         = stemHash[32] | n[1] | (subIndex[1] | value[≤32]) * (n + 1)
 * storageRecord = addressHash[32] | groupCount[≤8] | group * groupCount
 * </pre>
 */
public final class Eip8347TypedSnapshotCodec {

  public static final int KIND_NONE = 0x00;
  public static final int KIND_CODE = 0x01;
  public static final int KIND_DELEGATION = 0x02;

  private Eip8347TypedSnapshotCodec() {}

  // ---- Integers: fixed name[n], and name[≤w] = length byte | minimal big-endian bytes ----

  /** Writes {@code value}, read as an unsigned 64-bit integer, as {@code [≤maxWidth]}. */
  public static void writeUint(final OutputStream out, final long value, final int maxWidth)
      throws IOException {
    final int len = (Long.SIZE - Long.numberOfLeadingZeros(value) + 7) / Byte.SIZE;
    if (len > maxWidth) {
      throw new Eip8347ArtifactVerificationException(
          "typed uint does not fit in " + maxWidth + " bytes: " + Long.toUnsignedString(value));
    }
    out.write(len);
    for (int i = len - 1; i >= 0; i--) {
      out.write((int) (value >>> (i * Byte.SIZE)));
    }
  }

  /**
   * Writes a big-endian integer (leading zeros allowed in {@code value}) as {@code [≤maxWidth]}.
   */
  public static void writeUint(final OutputStream out, final Bytes value, final int maxWidth)
      throws IOException {
    final Bytes trimmed = value.trimLeadingZeros();
    if (trimmed.size() > maxWidth) {
      throw new Eip8347ArtifactVerificationException(
          "typed uint does not fit in " + maxWidth + " bytes: " + value.toHexString());
    }
    out.write(trimmed.size());
    out.write(trimmed.toArrayUnsafe());
  }

  /** Reads a {@code [≤maxWidth]} integer with {@code maxWidth ≤ 8} as an unsigned 64-bit value. */
  public static long readUint(final InputStream in, final int maxWidth) throws IOException {
    final byte[] be = readUintBytes(in, maxWidth);
    long value = 0L;
    for (final byte b : be) {
      value = (value << Byte.SIZE) | (b & 0xFF);
    }
    return value;
  }

  /** Reads a {@code [≤maxWidth]} integer as its minimal big-endian bytes (empty for zero). */
  public static byte[] readUintBytes(final InputStream in, final int maxWidth) throws IOException {
    final int len = readByte(in);
    if (len > maxWidth) {
      throw new Eip8347ArtifactVerificationException(
          "typed uint length " + len + " exceeds max " + maxWidth);
    }
    final byte[] be = readFully(in, len);
    if (len > 0 && be[0] == 0) {
      throw new Eip8347ArtifactVerificationException("typed uint has a leading zero byte");
    }
    return be;
  }

  /** Writes a fixed 8-byte big-endian section count. */
  public static void writeCount(final OutputStream out, final long count) throws IOException {
    if (count < 0) {
      throw new Eip8347ArtifactVerificationException("section count is negative");
    }
    for (int i = 7; i >= 0; i--) {
      out.write((int) (count >>> (i * Byte.SIZE)));
    }
  }

  public static long readCount(final InputStream in) throws IOException {
    long count = 0L;
    for (final byte b : readFully(in, 8)) {
      count = (count << Byte.SIZE) | (b & 0xFF);
    }
    if (count < 0) {
      throw new Eip8347ArtifactVerificationException("section count is negative");
    }
    return count;
  }

  public static int readByte(final InputStream in) throws IOException {
    final int b = in.read();
    if (b < 0) {
      throw new Eip8347ArtifactVerificationException("unexpected EOF in snapshot");
    }
    return b;
  }

  public static byte[] readFully(final InputStream in, final int len) throws IOException {
    final byte[] buf = in.readNBytes(len);
    if (buf.length != len) {
      throw new Eip8347ArtifactVerificationException(
          "unexpected EOF (wanted " + len + " bytes, got " + buf.length + ")");
    }
    return buf;
  }

  /**
   * One account's header stem. {@code codeHash}/{@code codeSize} only for kind 0x01, {@code target}
   * only for 0x02.
   */
  @SuppressWarnings("MethodInputParametersMustBeFinal") // compact record constructor
  public record HeaderRecord(
      Bytes32 addressHash,
      long nonce,
      UInt256 balance,
      int kind,
      Bytes32 codeHash,
      long codeSize,
      Bytes target,
      NavigableMap<Integer, Bytes32> slots) {

    public HeaderRecord {
      switch (kind) {
        case KIND_NONE -> {
          if (nonce == 0L && balance.isZero()) {
            throw invalid("empty account excluded by EIP-7523", addressHash);
          }
        }
        case KIND_CODE -> {
          if (codeHash == null || codeSize == 0L) {
            throw invalid("kind 0x01 needs a codeHash and a non-zero codeSize", addressHash);
          }
        }
        case KIND_DELEGATION -> {
          if (target == null || target.size() != 20) {
            throw invalid("kind 0x02 needs a 20-byte target", addressHash);
          }
        }
        default -> throw invalid("invalid header kind 0x" + Integer.toHexString(kind), addressHash);
      }
      for (final var slot : slots.entrySet()) {
        if (slot.getKey() < 0 || slot.getKey() >= HEADER_STORAGE_SLOTS) {
          throw invalid(
              "header slot " + slot.getKey() + " is not below HEADER_STORAGE_SLOTS", addressHash);
        }
        requireNonZero(slot.getValue());
      }
    }

    public Bytes stem() {
      return headerStem(addressHash);
    }

    /** Header-stem leaves per EIP-8347 leaf derivation, in sub-index order. */
    public List<Eip8347SnapshotLeaf> leaves() {
      final List<Eip8347SnapshotLeaf> leaves = new ArrayList<>(2 + slots.size());
      final long basicDataCodeSize =
          switch (kind) {
            case KIND_CODE -> codeSize;
            case KIND_DELEGATION -> DELEGATION_CODE_SIZE;
            default -> 0L;
          };
      leaves.add(
          leaf(
              BASIC_DATA_LEAF_KEY,
              BasicDataEncoder.encodeBasicData(basicDataCodeSize, nonce, balance)));
      if (kind == KIND_DELEGATION) {
        leaves.add(leaf(DELEGATION_LEAF_KEY, DelegationEncoder.encodeDelegation(target)));
      } else {
        leaves.add(
            leaf(
                CODE_HASH_LEAF_KEY,
                kind == KIND_CODE ? codeHash : TrieKeyDerivation.EMPTY_CODE_HASH));
      }
      slots.forEach((slot, value) -> leaves.add(leaf(HEADER_STORAGE_OFFSET + slot, value)));
      return leaves;
    }

    /** The {@code code_hash} the MPT commits for this account. */
    public Hash mptCodeHash() {
      return switch (kind) {
        case KIND_CODE -> Hash.wrap(codeHash);
        case KIND_DELEGATION ->
            Hash.hash(Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, target));
        default -> Hash.EMPTY;
      };
    }

    private Eip8347SnapshotLeaf leaf(final int subIndex, final Bytes32 value) {
      return new Eip8347SnapshotLeaf(Bytes.concatenate(stem(), Bytes.of((byte) subIndex)), value);
    }

    /**
     * Packs one header stem's leaves (convert path). Rejects leaves that would not derive back byte
     * for byte, e.g. a non-zero basic-data version or reserved byte.
     */
    public static HeaderRecord fromLeaves(
        final Bytes32 addressHash, final List<Eip8347SnapshotLeaf> stemLeaves) {
      final NavigableMap<Integer, Bytes32> bySub = bySubIndex(stemLeaves);
      final Bytes32 basicDataValue = bySub.remove(BASIC_DATA_LEAF_KEY);
      if (basicDataValue == null) {
        throw invalid("header stem missing basic-data leaf", addressHash);
      }
      final BasicDataEncoder.BasicData basicData;
      try {
        basicData = BasicDataEncoder.decodeBasicData(basicDataValue);
      } catch (final IllegalArgumentException e) {
        throw new Eip8347ArtifactVerificationException(e.getMessage(), e);
      }
      final Bytes32 codeHashLeaf = bySub.remove(CODE_HASH_LEAF_KEY);
      final Bytes32 delegationLeaf = bySub.remove(DELEGATION_LEAF_KEY);
      final NavigableMap<Integer, Bytes32> slots = new TreeMap<>();
      bySub.forEach(
          (sub, value) -> {
            if (sub < HEADER_STORAGE_OFFSET) {
              throw invalid("header stem has unexpected sub-index " + sub, addressHash);
            }
            slots.put(sub - HEADER_STORAGE_OFFSET, value);
          });

      final HeaderRecord header;
      if (delegationLeaf != null && codeHashLeaf == null) {
        final Bytes target = delegationLeaf.slice(3, 20);
        header =
            new HeaderRecord(
                addressHash,
                basicData.nonce(),
                basicData.balance(),
                KIND_DELEGATION,
                null,
                0L,
                target,
                slots);
      } else if (codeHashLeaf != null && delegationLeaf == null) {
        final boolean hasCode = basicData.codeSize() != 0L;
        header =
            new HeaderRecord(
                addressHash,
                basicData.nonce(),
                basicData.balance(),
                hasCode ? KIND_CODE : KIND_NONE,
                hasCode ? codeHashLeaf : null,
                basicData.codeSize(),
                null,
                slots);
      } else {
        throw invalid("header stem needs exactly one of code_hash and delegation", addressHash);
      }
      if (!header.leaves().equals(stemLeaves)) {
        throw invalid("header stem leaves do not match their typed derivation", addressHash);
      }
      return header;
    }
  }

  /** One stem's leaves in the CODE_ZONE or STORAGE_ZONE section. */
  @SuppressWarnings("MethodInputParametersMustBeFinal") // compact record constructor
  public record Group(Bytes32 stemHash, NavigableMap<Integer, Bytes32> entries) {

    public Group {
      if (entries.isEmpty() || entries.size() > STEM_SUBTREE_WIDTH) {
        throw new Eip8347ArtifactVerificationException(
            "group leaf count must be in 1..256, got " + entries.size());
      }
      entries.values().forEach(Eip8347TypedSnapshotCodec::requireNonZero);
    }

    /** Leaves under {@code stemPrefix || stemHash}, in sub-index order. */
    public List<Eip8347SnapshotLeaf> leaves(final Bytes stemPrefix) {
      final Bytes stem = Bytes.concatenate(stemPrefix, stemHash);
      final List<Eip8347SnapshotLeaf> leaves = new ArrayList<>(entries.size());
      entries.forEach(
          (sub, value) ->
              leaves.add(
                  new Eip8347SnapshotLeaf(
                      Bytes.concatenate(stem, Bytes.of(sub.byteValue())), value)));
      return leaves;
    }

    public static Group fromLeaves(
        final Bytes32 stemHash, final List<Eip8347SnapshotLeaf> stemLeaves) {
      return new Group(stemHash, bySubIndex(stemLeaves));
    }
  }

  public static Bytes headerStem(final Bytes32 addressHash) {
    return Bytes.concatenate(Bytes.of((byte) ACCOUNT_ZONE), addressHash);
  }

  public static Bytes codeStem(final Bytes32 stemHash) {
    return Bytes.concatenate(Bytes.of((byte) CODE_ZONE), stemHash);
  }

  /** Prefix of every storage-zone key of one account: {@code STORAGE_ZONE || addressHash}. */
  public static Bytes storagePrefix(final Bytes32 addressHash) {
    return Bytes.concatenate(Bytes.of((byte) STORAGE_ZONE), addressHash);
  }

  public static Bytes stemOf(final Bytes key) {
    return key.slice(0, key.size() - 1);
  }

  public static int subIndexOf(final Bytes key) {
    return key.get(key.size() - 1) & 0xFF;
  }

  /** Unsigned byte-lexicographic order, as the spec sorts every artifact. */
  public static int compare(final Bytes a, final Bytes b) {
    return Arrays.compareUnsigned(a.toArrayUnsafe(), b.toArrayUnsafe());
  }

  public static void writeHeaderRecord(final OutputStream out, final HeaderRecord header)
      throws IOException {
    out.write(header.addressHash().toArrayUnsafe());
    writeUint(out, header.nonce(), 8);
    writeUint(out, header.balance(), 16);
    out.write(header.kind());
    if (header.kind() == KIND_CODE) {
      out.write(header.codeHash().toArrayUnsafe());
      writeUint(out, header.codeSize(), 4);
    } else if (header.kind() == KIND_DELEGATION) {
      out.write(header.target().toArrayUnsafe());
    }
    writeEntries(out, header.slots(), header.slots().size());
  }

  public static HeaderRecord readHeaderRecord(final InputStream in) throws IOException {
    final Bytes32 addressHash = Bytes32.wrap(readFully(in, 32));
    final long nonce = readUint(in, 8);
    final UInt256 balance = UInt256.fromBytes(Bytes.wrap(readUintBytes(in, 16)));
    final int kind = readByte(in);
    Bytes32 codeHash = null;
    long codeSize = 0L;
    Bytes target = null;
    if (kind == KIND_CODE) {
      codeHash = Bytes32.wrap(readFully(in, 32));
      codeSize = readUint(in, 4);
    } else if (kind == KIND_DELEGATION) {
      target = Bytes.wrap(readFully(in, 20));
    }
    final int slotCount = readByte(in);
    return new HeaderRecord(
        addressHash, nonce, balance, kind, codeHash, codeSize, target, readEntries(in, slotCount));
  }

  public static void writeGroup(final OutputStream out, final Group group) throws IOException {
    out.write(group.stemHash().toArrayUnsafe());
    writeEntries(out, group.entries(), group.entries().size() - 1);
  }

  public static Group readGroup(final InputStream in) throws IOException {
    final Bytes32 stemHash = Bytes32.wrap(readFully(in, 32));
    final int count = readByte(in) + 1;
    return new Group(stemHash, readEntries(in, count));
  }

  /** {@code countByte | (index[1] | value[≤32]) * entries}; map order keeps indices ascending. */
  private static void writeEntries(
      final OutputStream out, final NavigableMap<Integer, Bytes32> entries, final int countByte)
      throws IOException {
    out.write(countByte);
    for (final var entry : entries.entrySet()) {
      out.write(entry.getKey());
      writeUint(out, entry.getValue(), 32);
    }
  }

  private static NavigableMap<Integer, Bytes32> readEntries(final InputStream in, final int count)
      throws IOException {
    final NavigableMap<Integer, Bytes32> entries = new TreeMap<>();
    int previous = -1;
    for (int i = 0; i < count; i++) {
      final int index = readByte(in);
      if (index <= previous) {
        throw new Eip8347ArtifactVerificationException(
            "record entries are not strictly ascending by index");
      }
      entries.put(index, Bytes32.leftPad(Bytes.wrap(readUintBytes(in, 32))));
      previous = index;
    }
    return entries;
  }

  private static NavigableMap<Integer, Bytes32> bySubIndex(
      final List<Eip8347SnapshotLeaf> stemLeaves) {
    final NavigableMap<Integer, Bytes32> bySub = new TreeMap<>();
    int previous = -1;
    for (final Eip8347SnapshotLeaf leaf : stemLeaves) {
      final int sub = subIndexOf(leaf.key());
      if (sub <= previous) {
        throw new Eip8347ArtifactVerificationException(
            "stem leaves are not strictly ascending by sub-index");
      }
      bySub.put(sub, leaf.value());
      previous = sub;
    }
    return bySub;
  }

  private static void requireNonZero(final Bytes32 value) {
    if (value.isZero()) {
      throw new Eip8347ArtifactVerificationException(
          "snapshot must not contain a zero-valued leaf (EIP-8297 absence rule)");
    }
  }

  private static Eip8347ArtifactVerificationException invalid(
      final String message, final Bytes32 addressHash) {
    return new Eip8347ArtifactVerificationException(message + " for " + addressHash.toHexString());
  }
}
