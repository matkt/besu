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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347;

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;

/**
 * Stem → offset seek index over an EIP-8347 snapshot.
 *
 * <p>Phase-1 records one entry per PBT stem ({@code key} without the trailing sub-index) while the
 * snapshot is streamed. Phase-2 lookups binary-search that <em>account-/stem-sized</em> table (not
 * the leaf set), seek once, then scan only the contiguous leaves of that stem. Values stay in the
 * original snapshot file — no second leaf spill and no full sort.
 */
public final class Eip8347SnapshotLeafIndex implements Closeable {

  private final Path snapshotPath;
  private final List<StemEntry> stems = new ArrayList<>();
  private RandomAccessFile file;
  private BitSet consumed;
  private long leafCount;
  private long consumedCount;
  private boolean sealed;
  private Bytes previousKey;
  private Bytes previousStem;

  public Eip8347SnapshotLeafIndex(final Path snapshotPath) {
    this.snapshotPath = snapshotPath;
  }

  /**
   * Records the byte offset of a leaf in the original snapshot. Keys must arrive in strictly
   * ascending PBT order. A new stem entry is appended only when {@code stem(key)} changes.
   */
  public void record(final Bytes key, final long offset) {
    if (sealed) {
      throw new IllegalStateException("leaf index already sealed");
    }
    if (key.size() < 2) {
      throw new Eip8347ArtifactVerificationException("snapshot leaf key too short: " + key.size());
    }
    if (previousKey != null && previousKey.compareTo(key) >= 0) {
      throw new Eip8347ArtifactVerificationException(
          "leaf index requires strictly ascending PBT keys");
    }
    previousKey = key;
    final Bytes stem = stemOf(key);
    if (previousStem == null || !previousStem.equals(stem)) {
      stems.add(new StemEntry(stem.copy(), offset, leafCount));
      previousStem = stem;
    }
    leafCount++;
  }

  public void seal() throws IOException {
    sealed = true;
    if (leafCount > Integer.MAX_VALUE) {
      throw new Eip8347ArtifactVerificationException("too many leaves for consumption bitset");
    }
    consumed = new BitSet((int) leafCount);
    file = new RandomAccessFile(snapshotPath.toFile(), "r");
  }

  public long size() {
    return leafCount;
  }

  public int stemCount() {
    return stems.size();
  }

  public synchronized Optional<Bytes32> get(final Bytes key) throws IOException {
    requireSealed();
    final Found found = find(key);
    if (found == null) {
      return Optional.empty();
    }
    markConsumed(found.index);
    return Optional.of(found.value);
  }

  public Bytes32 require(final Bytes key) throws IOException {
    return get(key)
        .orElseThrow(
            () ->
                new Eip8347ArtifactVerificationException(
                    "missing snapshot leaf for key " + key.toHexString()));
  }

  /**
   * Seeks to {@code stem} and returns every leaf under it (sub-index → value), marking them
   * consumed. One binary search + one seek for the whole stem.
   */
  public synchronized Map<Integer, Bytes32> requireStem(final Bytes stem) throws IOException {
    requireSealed();
    final int stemIdx = findStemIndex(stem);
    if (stemIdx < 0) {
      throw new Eip8347ArtifactVerificationException("missing snapshot stem " + stem.toHexString());
    }
    final StemEntry entry = stems.get(stemIdx);
    final long endIndex =
        stemIdx + 1 < stems.size() ? stems.get(stemIdx + 1).firstLeafIndex : leafCount;
    file.seek(entry.offset);
    final Map<Integer, Bytes32> out = new LinkedHashMap<>();
    long index = entry.firstLeafIndex;
    while (index < endIndex) {
      final ParsedLeaf leaf = readLeafAtCursor();
      if (!stemOf(leaf.key).equals(stem)) {
        throw new Eip8347ArtifactVerificationException(
            "stem scan drifted at index " + index + " for " + stem.toHexString());
      }
      final int sub = leaf.key.get(leaf.key.size() - 1) & 0xFF;
      out.put(sub, leaf.value);
      markConsumed((int) index);
      index++;
    }
    return out;
  }

  public synchronized void ensureAllConsumed() {
    requireSealed();
    if (consumedCount != leafCount) {
      throw new Eip8347ArtifactVerificationException(
          "snapshot has "
              + (leafCount - consumedCount)
              + " leaf(s) not covered by consensus anchoring");
    }
  }

  private Found find(final Bytes key) throws IOException {
    if (leafCount == 0 || stems.isEmpty()) {
      return null;
    }
    final Bytes stem = stemOf(key);
    final int stemIdx = findStemIndex(stem);
    if (stemIdx < 0) {
      return null;
    }
    final StemEntry entry = stems.get(stemIdx);
    final long endIndex =
        stemIdx + 1 < stems.size() ? stems.get(stemIdx + 1).firstLeafIndex : leafCount;
    file.seek(entry.offset);
    long index = entry.firstLeafIndex;
    while (index < endIndex) {
      final ParsedLeaf leaf = readLeafAtCursor();
      final int cmp = leaf.key.compareTo(key);
      if (cmp == 0) {
        return new Found((int) index, leaf.value);
      }
      if (cmp > 0) {
        return null;
      }
      index++;
    }
    return null;
  }

  private int findStemIndex(final Bytes stem) {
    int lo = 0;
    int hi = stems.size() - 1;
    while (lo <= hi) {
      final int mid = (lo + hi) >>> 1;
      final int cmp = stems.get(mid).stem.compareTo(stem);
      if (cmp == 0) {
        return mid;
      }
      if (cmp < 0) {
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return -1;
  }

  private void markConsumed(final int index) {
    if (!consumed.get(index)) {
      consumed.set(index);
      consumedCount++;
    }
  }

  static Bytes stemOf(final Bytes key) {
    return key.slice(0, key.size() - 1);
  }

  private ParsedLeaf readLeafAtCursor() throws IOException {
    final Bytes encoded = readRlpItem();
    final Bytes key;
    final Bytes rawValue;
    try {
      final RLPInput input = RLP.input(encoded);
      input.enterList();
      if (input.isEndOfCurrentList()) {
        throw new Eip8347ArtifactVerificationException(
            "snapshot leaf record must be a key-value pair");
      }
      key = input.readBytes();
      if (input.isEndOfCurrentList()) {
        throw new Eip8347ArtifactVerificationException(
            "snapshot leaf record must be a key-value pair");
      }
      rawValue = input.readBytes();
      if (!input.isEndOfCurrentList()) {
        throw new Eip8347ArtifactVerificationException(
            "snapshot leaf record has trailing RLP elements");
      }
      input.leaveList();
    } catch (final org.hyperledger.besu.ethereum.rlp.RLPException e) {
      throw new Eip8347ArtifactVerificationException("malformed snapshot leaf RLP", e);
    }
    if (rawValue.isEmpty()) {
      throw new Eip8347ArtifactVerificationException("snapshot leaf value must not be empty RLP");
    }
    if (rawValue.get(0) == 0) {
      throw new Eip8347ArtifactVerificationException(
          "snapshot leaf value has a leading zero byte (non-canonical RLP integer)");
    }
    if (rawValue.size() > 32) {
      throw new Eip8347ArtifactVerificationException(
          "snapshot leaf value exceeds 32 bytes: " + rawValue.size());
    }
    return new ParsedLeaf(key, Bytes32.leftPad(rawValue));
  }

  private Bytes readRlpItem() throws IOException {
    final int prefix = file.read();
    if (prefix < 0) {
      throw new Eip8347ArtifactVerificationException("unexpected EOF reading snapshot leaf");
    }
    if (prefix <= 0x7f) {
      return Bytes.of((byte) prefix);
    }
    if (prefix <= 0xb7) {
      final int len = prefix - 0x80;
      final byte[] payload = readFully(len);
      final MutableBytes out = MutableBytes.create(1 + len);
      out.set(0, (byte) prefix);
      out.set(1, Bytes.wrap(payload));
      return out;
    }
    if (prefix <= 0xbf) {
      final int lenOfLen = prefix - 0xb7;
      final byte[] lenBytes = readFully(lenOfLen);
      final int len = decodeLength(lenBytes);
      if (len < 56) {
        throw new Eip8347ArtifactVerificationException(
            "non-canonical RLP: long-form length for short string");
      }
      final byte[] payload = readFully(len);
      final MutableBytes out = MutableBytes.create(1 + lenOfLen + len);
      out.set(0, (byte) prefix);
      out.set(1, Bytes.wrap(lenBytes));
      out.set(1 + lenOfLen, Bytes.wrap(payload));
      return out;
    }
    if (prefix <= 0xf7) {
      final int len = prefix - 0xc0;
      final byte[] payload = readFully(len);
      final MutableBytes out = MutableBytes.create(1 + len);
      out.set(0, (byte) prefix);
      out.set(1, Bytes.wrap(payload));
      return out;
    }
    final int lenOfLen = prefix - 0xf7;
    final byte[] lenBytes = readFully(lenOfLen);
    final int len = decodeLength(lenBytes);
    if (len < 56) {
      throw new Eip8347ArtifactVerificationException(
          "non-canonical RLP: long-form length for short list");
    }
    final byte[] payload = readFully(len);
    final MutableBytes out = MutableBytes.create(1 + lenOfLen + len);
    out.set(0, (byte) prefix);
    out.set(1, Bytes.wrap(lenBytes));
    out.set(1 + lenOfLen, Bytes.wrap(payload));
    return out;
  }

  private static int decodeLength(final byte[] lenBytes) {
    if (lenBytes.length == 0 || (lenBytes[0] == 0)) {
      throw new Eip8347ArtifactVerificationException("non-canonical RLP length encoding");
    }
    long len = 0;
    for (final byte b : lenBytes) {
      len = (len << 8) | (b & 0xFF);
      if (len > Integer.MAX_VALUE) {
        throw new Eip8347ArtifactVerificationException("RLP item too large");
      }
    }
    return (int) len;
  }

  private byte[] readFully(final int len) throws IOException {
    final byte[] buf = new byte[len];
    int off = 0;
    while (off < len) {
      final int n = file.read(buf, off, len - off);
      if (n < 0) {
        throw new Eip8347ArtifactVerificationException(
            "unexpected EOF (wanted " + len + " bytes, got " + off + ")");
      }
      off += n;
    }
    return buf;
  }

  private void requireSealed() {
    if (!sealed) {
      throw new IllegalStateException("leaf index not sealed");
    }
  }

  @Override
  public void close() throws IOException {
    if (file != null) {
      file.close();
      file = null;
    }
  }

  private record StemEntry(Bytes stem, long offset, long firstLeafIndex) {}

  private record Found(int index, Bytes32 value) {}

  private record ParsedLeaf(Bytes key, Bytes32 value) {}
}
