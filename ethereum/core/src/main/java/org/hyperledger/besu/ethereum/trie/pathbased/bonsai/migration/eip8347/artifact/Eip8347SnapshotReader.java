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

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;

/**
 * Streaming reader for an EIP-8347 PBT snapshot.
 *
 * <p>Format: {@code pbtRoot[32] | leafCount[8, BE] | RLP([key, value]) * leafCount}. Values are
 * canonical RLP integers (no leading zero); this reader left-pads them to 32 bytes.
 */
public final class Eip8347SnapshotReader implements Closeable {

  private final InputStream in;
  private final Bytes32 claimedRoot;
  private final long leafCount;
  private long readCount;
  private long position;
  private long lastLeafOffset;
  private Bytes previousKey;

  public Eip8347SnapshotReader(final Path path) throws IOException {
    this.in = Files.newInputStream(path);
    final byte[] header = readFully(40);
    this.claimedRoot = Bytes32.wrap(header, 0);
    this.leafCount = ByteBuffer.wrap(header, 32, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    if (leafCount < 0) {
      throw new Eip8347ArtifactVerificationException("snapshot leafCount is negative");
    }
  }

  /**
   * Byte offset in the snapshot file of the leaf most recently returned by {@link #iterator()} /
   * {@link #forEach(Consumer)}. Valid only after at least one leaf has been read.
   */
  public long lastLeafOffset() {
    return lastLeafOffset;
  }

  public Bytes32 claimedRoot() {
    return claimedRoot;
  }

  public long leafCount() {
    return leafCount;
  }

  /** Streams every leaf in order, invoking {@code consumer} once per leaf. */
  public void forEach(final Consumer<Eip8347SnapshotLeaf> consumer) throws IOException {
    final Iterator<Eip8347SnapshotLeaf> it = iterator();
    while (it.hasNext()) {
      consumer.accept(it.next());
    }
  }

  public Iterator<Eip8347SnapshotLeaf> iterator() {
    return new Iterator<>() {
      private Eip8347SnapshotLeaf next;

      @Override
      public boolean hasNext() {
        if (next != null) {
          return true;
        }
        if (readCount >= leafCount) {
          return false;
        }
        try {
          next = readOne();
          return true;
        } catch (final IOException e) {
          throw new Eip8347ArtifactVerificationException("failed reading snapshot leaf", e);
        }
      }

      @Override
      public Eip8347SnapshotLeaf next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final Eip8347SnapshotLeaf leaf = next;
        next = null;
        return leaf;
      }
    };
  }

  private Eip8347SnapshotLeaf readOne() throws IOException {
    lastLeafOffset = position;
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
    final Bytes32 value = Bytes32.leftPad(rawValue);
    if (previousKey != null && previousKey.compareTo(key) >= 0) {
      throw new Eip8347ArtifactVerificationException(
          "snapshot leaves are not strictly ascending in PBT key order");
    }
    previousKey = key;
    readCount++;
    return new Eip8347SnapshotLeaf(key, value);
  }

  /** Reads one top-level RLP item from the stream without buffering the remainder of the file. */
  private Bytes readRlpItem() throws IOException {
    final int prefix = readByte();
    if (prefix < 0) {
      throw new Eip8347ArtifactVerificationException(
          "unexpected EOF reading snapshot leaf " + readCount + " of " + leafCount);
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

  private int readByte() throws IOException {
    final int b = in.read();
    if (b >= 0) {
      position++;
    }
    return b;
  }

  private byte[] readFully(final int len) throws IOException {
    final byte[] buf = new byte[len];
    int off = 0;
    while (off < len) {
      final int n = in.read(buf, off, len - off);
      if (n < 0) {
        throw new Eip8347ArtifactVerificationException(
            "unexpected EOF (wanted " + len + " bytes, got " + off + ")");
      }
      off += n;
    }
    position += len;
    return buf;
  }

  public void ensureExhausted() throws IOException {
    if (readCount != leafCount) {
      throw new Eip8347ArtifactVerificationException(
          "read " + readCount + " leaves but header leafCount is " + leafCount);
    }
    if (in.read() != -1) {
      throw new Eip8347ArtifactVerificationException("trailing bytes after snapshot leaf stream");
    }
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
