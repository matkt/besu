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

import org.hyperledger.besu.config.GenesisAccount;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
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

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * The EIP-8347 preimage file: streaming reader, plus {@link #write} / {@link #writeGenesis}.
 *
 * <p>Concatenation of records with no framing: {@code address[20] | slotCount[4, BE] | slotKey[32]
 * * slotCount}. Records strictly ascending by {@code keccak256(address)}; slots strictly ascending
 * by {@code keccak256(slotKey)}; no trailing byte.
 *
 * <p>{@link #nextAccount()} / {@link #nextSlot()} keep one slot in memory, and {@link #batches} at
 * most a bounded batch, whatever the account's storage size.
 *
 * <p>Checking the order costs a keccak per address and slot. A pass that needs neither the keccak
 * nor the check, because a later pass over the same file makes it, can open the file with {@link
 * #withoutOrderCheck}.
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

  private final InputStream in;
  private final boolean checkOrder;
  private Hash previousAddressHash;
  private Hash previousSlotHash;
  private int slotsLeft;
  private boolean exhausted;

  /** Writes {@code accounts} in canonical order (records and slots sorted by keccak). */
  public static void write(final Path path, final List<AccountPreimages> accounts)
      throws IOException {
    final List<AccountPreimages> sorted = new ArrayList<>(accounts);
    sorted.sort(Comparator.comparing(AccountPreimages::addressHash));
    try (final DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
      for (final AccountPreimages account : sorted) {
        final List<Bytes32> slots = new ArrayList<>(account.slotKeys());
        slots.sort(Comparator.comparing(Hash::hash));
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
    this(path, true);
  }

  private Eip8347PreimageFile(final Path path, final boolean checkOrder) throws IOException {
    this.in =
        new BufferedInputStream(
            Files.newInputStream(path), Eip8347TypedSnapshotCodec.IO_BUFFER_BYTES);
    this.checkOrder = checkOrder;
  }

  /** Reads the file without computing keccak hashes, so without checking the keccak order. */
  public static Eip8347PreimageFile withoutOrderCheck(final Path path) throws IOException {
    return new Eip8347PreimageFile(path, false);
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
    if (!checkOrder) {
      return new Account(address, null, slotsLeft);
    }
    // Not address.addressHash(): its shared cache only costs here, every address comes once.
    final Hash addressHash = Hash.hash(address.getBytes());
    if (previousAddressHash != null && previousAddressHash.compareTo(addressHash) >= 0) {
      throw new Eip8347ArtifactVerificationException(
          "preimage records are not strictly ascending by keccak256(address)");
    }
    previousAddressHash = addressHash;
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
    if (!checkOrder) {
      return new Slot(key, null);
    }
    final Hash keyHash = Hash.hash(key);
    if (previousSlotHash != null && previousSlotHash.compareTo(keyHash) >= 0) {
      throw new Eip8347ArtifactVerificationException(
          "preimage slot keys are not strictly ascending by keccak256(slotKey)");
    }
    previousSlotHash = keyHash;
    return new Slot(key, keyHash);
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
