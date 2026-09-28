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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Streaming reader for the EIP-8347 preimage file.
 *
 * <p>Concatenation of fixed-width records with no framing: {@code address[20] | slotCount[4, BE] |
 * slotKey[32] * slotCount}. Sorted by {@code keccak256(address)}; slots by {@code
 * keccak256(slotKey)}.
 */
public final class Eip8347PreimageReader implements Closeable {

  private final InputStream in;
  private Hash previousAddressHash;
  private boolean exhausted;

  public Eip8347PreimageReader(final Path path) throws IOException {
    this.in = Files.newInputStream(path);
  }

  public void forEach(final Consumer<Eip8347PreimageRecord> consumer) throws IOException {
    final Iterator<Eip8347PreimageRecord> it = iterator();
    while (it.hasNext()) {
      consumer.accept(it.next());
    }
  }

  public Iterator<Eip8347PreimageRecord> iterator() {
    return new Iterator<>() {
      private Eip8347PreimageRecord next;

      @Override
      public boolean hasNext() {
        if (next != null) {
          return true;
        }
        if (exhausted) {
          return false;
        }
        try {
          next = readOneOrNull();
          return next != null;
        } catch (final IOException e) {
          throw new Eip8347ArtifactVerificationException("failed reading preimage record", e);
        }
      }

      @Override
      public Eip8347PreimageRecord next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final Eip8347PreimageRecord record = next;
        next = null;
        return record;
      }
    };
  }

  private Eip8347PreimageRecord readOneOrNull() throws IOException {
    final byte[] addressBytes = new byte[20];
    final int n = readFullyOrEof(addressBytes);
    if (n == 0) {
      exhausted = true;
      return null;
    }
    if (n != 20) {
      throw new Eip8347ArtifactVerificationException(
          "truncated preimage address (got " + n + " bytes)");
    }
    final byte[] slotCountBytes = readFully(4);
    final long slotCountLong =
        ByteBuffer.wrap(slotCountBytes).order(ByteOrder.BIG_ENDIAN).getInt() & 0xFFFFFFFFL;
    if (slotCountLong > Integer.MAX_VALUE) {
      throw new Eip8347ArtifactVerificationException(
          "preimage slotCount too large: " + slotCountLong);
    }
    final int slotCount = (int) slotCountLong;
    final List<Bytes32> slots = new ArrayList<>(slotCount);
    for (int i = 0; i < slotCount; i++) {
      slots.add(Bytes32.wrap(readFully(32)));
    }
    final Address address = Address.wrap(Bytes.wrap(addressBytes));
    final Eip8347PreimageRecord record = new Eip8347PreimageRecord(address, slots);
    Hash previousSlotHash = null;
    for (final Hash slotHash : record.slotKeyHashes()) {
      if (previousSlotHash != null && previousSlotHash.compareTo(slotHash) >= 0) {
        throw new Eip8347ArtifactVerificationException(
            "preimage slot keys are not strictly ascending by keccak256(slotKey)");
      }
      previousSlotHash = slotHash;
    }
    if (previousAddressHash != null && previousAddressHash.compareTo(record.addressHash()) >= 0) {
      throw new Eip8347ArtifactVerificationException(
          "preimage records are not strictly ascending by keccak256(address)");
    }
    previousAddressHash = record.addressHash();
    return record;
  }

  private int readFullyOrEof(final byte[] buf) throws IOException {
    int off = 0;
    while (off < buf.length) {
      final int r = in.read(buf, off, buf.length - off);
      if (r < 0) {
        return off;
      }
      off += r;
    }
    return off;
  }

  private byte[] readFully(final int len) throws IOException {
    final byte[] buf = new byte[len];
    int off = 0;
    while (off < len) {
      final int r = in.read(buf, off, len - off);
      if (r < 0) {
        throw new Eip8347ArtifactVerificationException(
            "unexpected EOF in preimage file (wanted " + len + ", got " + off + ")");
      }
      off += r;
    }
    return buf;
  }

  public void ensureExhausted() throws IOException {
    if (!exhausted && in.read() != -1) {
      throw new Eip8347ArtifactVerificationException("trailing bytes after preimage records");
    }
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
