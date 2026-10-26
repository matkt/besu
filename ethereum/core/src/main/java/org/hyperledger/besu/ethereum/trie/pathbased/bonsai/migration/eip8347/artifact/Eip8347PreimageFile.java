/*
 * Copyright contributors to Besu.
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

import org.hyperledger.besu.config.GenesisAccount;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * The EIP-8347 preimage file, {@code address[20] | slotCount[4] | slotKey[32] * slotCount} per
 * account, accounts by {@code keccak256(address)} and slots by {@code keccak256(slotKey)}: a
 * streaming reader holding one slot or one bounded batch, and writers. {@link #withoutOrderCheck}
 * skips the keccak order check, for a pass whose order another pass checks.
 */
public final class Eip8347PreimageFile implements Closeable {

  /**
   * One record header; its {@code slotCount} slots follow via {@link #nextSlot()}. {@code
   * addressHash} is null when the file is read {@link #withoutOrderCheck}.
   */
  public record Account(Address address, Hash addressHash, int slotCount) {}

  /**
   * One storage-slot preimage with its MPT path. {@code keyHash} is null when the file is read
   * {@link #withoutOrderCheck}.
   */
  public record Slot(Bytes32 key, Hash keyHash) {}

  /** One whole record: an address and its slot keys (fixtures, genesis, small files). */
  public record AccountPreimages(Address address, List<Bytes32> slotKeys) {
    public Hash addressHash() {
      return address.addressHash();
    }
  }

  private final CountingInputStream file;
  private final InputStream in;
  private final boolean checkOrder;
  private long position;
  private Bytes previousAddressHash;
  private Bytes previousSlotHash;
  private int slotsLeft;
  private long accountsRead;
  private boolean exhausted;

  /** Writes {@code accounts} in canonical order (records and slots sorted by keccak). */
  public static void write(final Path path, final List<AccountPreimages> accounts)
      throws IOException {
    final List<AccountPreimages> sorted = new ArrayList<>(accounts);
    sorted.sort(Comparator.comparing(account -> account.addressHash().getBytes()));
    try (final DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
      for (final AccountPreimages account : sorted) {
        final List<Bytes32> slots = new ArrayList<>(account.slotKeys());
        slots.sort(Comparator.comparing(slot -> Hash.hash(slot).getBytes()));
        out.write(account.address().getBytes().toArrayUnsafe());
        out.writeInt(slots.size());
        for (final Bytes32 slot : slots) {
          out.write(slot.toArrayUnsafe());
        }
      }
    }
  }

  /**
   * Writes the preimages of a genesis {@code alloc} (Hive / small chains: Bonsai keeps no keccak
   * preimage store). Zero-valued slots are skipped, as the MPT holds no leaf for them.
   *
   * @return number of accounts written
   */
  public static int writeGenesis(final Path path, final Stream<GenesisAccount> allocations)
      throws IOException {
    final List<AccountPreimages> accounts =
        allocations
            .map(
                account ->
                    new AccountPreimages(
                        account.address(),
                        account.storage().entrySet().stream()
                            .filter(slot -> !slot.getValue().isZero())
                            .map(slot -> Bytes32.leftPad(slot.getKey()))
                            .toList()))
            .toList();
    write(path, accounts);
    return accounts.size();
  }

  public Eip8347PreimageFile(final Path path) throws IOException {
    this(Files.newInputStream(path), 0, true);
  }

  private Eip8347PreimageFile(
      final InputStream records, final long start, final boolean checkOrder) {
    this.file = new CountingInputStream(records);
    this.in = new BufferedInputStream(file, Eip8347TypedSnapshotCodec.IO_BUFFER_BYTES);
    this.checkOrder = checkOrder;
    this.position = start;
  }

  /** Reads the records between offsets {@code start} and {@code end}, both record boundaries. */
  public static Eip8347PreimageFile section(final Path path, final long start, final long end)
      throws IOException {
    final InputStream records = Files.newInputStream(path);
    try {
      records.skipNBytes(start);
    } catch (final IOException e) {
      records.close();
      throw e;
    }
    return new Eip8347PreimageFile(ByteStreams.limit(records, end - start), start, true);
  }

  /** Bytes read from the file so far, one buffer ahead of the records handed out at most. */
  public long bytesRead() {
    return file.getCount();
  }

  /** Reads the file without computing keccak hashes, so without checking the keccak order. */
  public static Eip8347PreimageFile withoutOrderCheck(final Path path) throws IOException {
    return new Eip8347PreimageFile(Files.newInputStream(path), 0, false);
  }

  /** Offset in the file of the next record or slot to read. */
  public long position() {
    return position;
  }

  /** Next record header, or {@code null} at a clean end of file. */
  public Account nextAccount() throws IOException {
    if (slotsLeft != 0) {
      throw new IllegalStateException(slotsLeft + " slot(s) of the previous account left unread");
    }
    final byte[] addressBytes = in.readNBytes(Address.SIZE);
    if (addressBytes.length == 0) {
      exhausted = true;
      return null;
    }
    if (addressBytes.length != Address.SIZE) {
      throw new Eip8347ArtifactVerificationException(
          "truncated preimage address (got " + addressBytes.length + " bytes)");
    }
    final long slotCount = Integer.toUnsignedLong(readInt());
    if (slotCount > Integer.MAX_VALUE) {
      throw new Eip8347ArtifactVerificationException("preimage slotCount too large: " + slotCount);
    }
    final Address address = Address.wrap(Bytes.wrap(addressBytes));
    slotsLeft = (int) slotCount;
    position += Address.SIZE + Integer.BYTES;
    accountsRead++;
    if (!checkOrder) {
      return new Account(address, null, slotsLeft);
    }
    // Not address.addressHash(): its shared cache only costs here, every address comes once.
    final Hash addressHash = Hash.hash(address.getBytes());
    checkAccountOrder(previousAddressHash, addressHash.getBytes());
    previousAddressHash = addressHash.getBytes();
    previousSlotHash = null;
    return new Account(address, addressHash, slotsLeft);
  }

  /** Next slot of the current account; call exactly {@link Account#slotCount()} times. */
  public Slot nextSlot() throws IOException {
    if (slotsLeft == 0) {
      throw new IllegalStateException("no slot left for the current account");
    }
    final byte[] keyBytes = in.readNBytes(Bytes32.SIZE);
    if (keyBytes.length != Bytes32.SIZE) {
      throw new Eip8347ArtifactVerificationException(
          "truncated preimage slot key (got " + keyBytes.length + " bytes)");
    }
    final Bytes32 key = Bytes32.wrap(keyBytes);
    slotsLeft--;
    position += Bytes32.SIZE;
    if (!checkOrder) {
      return new Slot(key, null);
    }
    final Hash keyHash = Hash.hash(key);
    checkSlotOrder(previousSlotHash, keyHash.getBytes());
    previousSlotHash = keyHash.getBytes();
    return new Slot(key, keyHash);
  }

  /** Whether the current account has a slot left to read. */
  public boolean hasNextSlot() {
    return slotsLeft > 0;
  }

  /** Skips the current account's remaining slots. */
  public void skipSlots() throws IOException {
    final long bytes = (long) slotsLeft * Bytes32.SIZE;
    try {
      in.skipNBytes(bytes);
    } catch (final EOFException e) {
      throw new Eip8347ArtifactVerificationException("truncated preimage slot keys");
    }
    position += bytes;
    slotsLeft = 0;
  }

  /** Rejects an account whose keccak path is not after {@code previous} (null for the first). */
  public static void checkAccountOrder(final Bytes previous, final Bytes addressHash) {
    if (previous != null && previous.compareTo(addressHash) >= 0) {
      throw new Eip8347ArtifactVerificationException(
          "preimage records are not strictly ascending by keccak256(address)");
    }
  }

  /** Rejects a slot whose keccak path is not after {@code previous} (null for the first). */
  public static void checkSlotOrder(final Bytes previous, final Bytes keyHash) {
    if (previous != null && previous.compareTo(keyHash) >= 0) {
      throw new Eip8347ArtifactVerificationException(
          "preimage slot keys are not strictly ascending by keccak256(slotKey)");
    }
  }

  /**
   * One account's slots, at most {@code maxSlots} at a time. {@code first} marks the batch that
   * opens the account (it may hold no slot).
   */
  public record Batch(Account account, boolean first, List<Slot> slots) {}

  /** Streams every record as bounded {@link Batch}es, in file order. */
  public Iterator<Batch> batches(final int maxSlots) {
    return new Iterator<>() {
      private Account current;
      private Batch next;

      @Override
      public boolean hasNext() {
        if (next == null && !exhausted) {
          try {
            next = readBatch();
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        }
        return next != null;
      }

      @Override
      public Batch next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final Batch batch = next;
        next = null;
        return batch;
      }

      private Batch readBatch() throws IOException {
        final boolean first = slotsLeft == 0;
        if (first) {
          current = nextAccount();
          if (current == null) {
            return null;
          }
        }
        final List<Slot> slots = new ArrayList<>(Math.min(slotsLeft, maxSlots));
        while (slotsLeft > 0 && slots.size() < maxSlots) {
          slots.add(nextSlot());
        }
        return new Batch(current, first, slots);
      }
    };
  }

  /** Accounts read so far. */
  public long accountsRead() {
    return accountsRead;
  }

  /** Fails unless every record has been read. */
  public void ensureExhausted() throws IOException {
    if (!exhausted && (slotsLeft != 0 || in.read() != -1)) {
      throw new Eip8347ArtifactVerificationException("trailing bytes after preimage records");
    }
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  private int readInt() throws IOException {
    final byte[] be = in.readNBytes(Integer.BYTES);
    if (be.length != Integer.BYTES) {
      throw new Eip8347ArtifactVerificationException(
          "truncated preimage slotCount (got " + be.length + " bytes)");
    }
    return ByteBuffer.wrap(be).getInt();
  }
}
