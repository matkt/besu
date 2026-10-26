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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.selfconvert;

import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.selfconvert.FlatState.RANGES;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter.Entry;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.selfconvert.StateLeaves.Converted;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Joins one key range of the flat database, at the target block, with its records in the preimage
 * file. Both are in key order, so the file is read once, forward.
 */
final class RangeScan implements Iterator<Converted> {

  private static final Logger LOG = LoggerFactory.getLogger(RangeScan.class);

  /** One account in this many is hashed while finding where the ranges start. */
  private static final int STRIDE = 1024;

  private final int range;
  private final FlatState.Batches batches;
  private final TargetPriors priors;
  private final Eip8347PreimageFile preimages;
  private final Runnable rangeDone;
  private boolean done;
  private long orphans;

  /** The last account read, and its address when the file has it. */
  private byte[] accountKey;

  private Address address;

  /** The file's current account, null past the range, and its last slot read. */
  private Eip8347PreimageFile.Account fileAccount;

  private Eip8347PreimageFile.Slot fileSlot;

  /** Reads {@code range} and its preimages; {@code rangeDone} runs once the range is converted. */
  RangeScan(
      final int range,
      final FlatState.Batches batches,
      final TargetPriors priors,
      final Eip8347PreimageFile preimages,
      final Runnable rangeDone)
      throws IOException {
    this.range = range;
    this.batches = batches;
    this.priors = priors;
    this.preimages = preimages;
    this.rangeDone = rangeDone;
    try {
      nextFileAccount();
    } catch (final IOException | RuntimeException e) {
      preimages.close();
      throw e;
    }
  }

  @Override
  public boolean hasNext() {
    return !done;
  }

  @Override
  public Converted next() {
    if (done) {
      throw new NoSuchElementException();
    }
    try {
      // the conversion was stopped
      if (Thread.currentThread().isInterrupted()) {
        throw new CancellationException("range " + range + " interrupted");
      }
      final FlatState.Batch batch = batches.next();
      priors.advanceTo(batch.block());
      final StateLeaves leaves = new StateLeaves();
      for (final Entry entry : priors.atTarget(range, batch.entries())) {
        join(entry, leaves);
      }
      if (batch.last()) {
        for (final Entry entry : priors.rest(range)) {
          join(entry, leaves);
        }
        finish();
        rangeDone.run();
      }
      return leaves.converted();
    } catch (final IOException e) {
      finish();
      throw new UncheckedIOException(e);
    } catch (final RuntimeException e) {
      finish();
      throw e;
    }
  }

  private void join(final Entry entry, final StateLeaves leaves) throws IOException {
    final byte[] key = entry.key();
    if (FlatState.isAccount(key)) {
      accountKey = key;
      address = fileAddress(key);
      if (address != null) {
        leaves.account(address, entry.value());
      } else {
        leaves.miss(entry);
      }
    } else if (accountKey == null || !FlatState.isSlotOf(key, accountKey)) {
      // left by an account that no longer exists: not in the state
      orphans++;
    } else if (address != null && fileHasSlot(key)) {
      leaves.slot(address, fileSlot.key(), entry.value());
    } else {
      leaves.miss(entry);
    }
  }

  /** The address of the account keyed {@code key}, if the file has it; moves the file to it. */
  private Address fileAddress(final byte[] key) throws IOException {
    while (fileAccount != null && compare(fileAccount.addressHash().getBytes(), key, 0) < 0) {
      nextFileAccount();
    }
    return fileAccount != null && compare(fileAccount.addressHash().getBytes(), key, 0) == 0
        ? fileAccount.address()
        : null;
  }

  /** Whether the file has the slot keyed {@code key}; if so, {@link #fileSlot} holds it. */
  private boolean fileHasSlot(final byte[] key) throws IOException {
    while ((fileSlot == null || compare(fileSlot.keyHash().getBytes(), key, Bytes32.SIZE) < 0)
        && preimages.hasNextSlot()) {
      fileSlot = preimages.nextSlot();
    }
    return fileSlot != null && compare(fileSlot.keyHash().getBytes(), key, Bytes32.SIZE) == 0;
  }

  private void nextFileAccount() throws IOException {
    preimages.skipSlots();
    fileAccount = preimages.nextAccount();
    fileSlot = null;
    if (fileAccount != null
        && FlatState.rangeOf(fileAccount.addressHash().getBytes().toArrayUnsafe()) != range) {
      throw new Eip8347ArtifactVerificationException(
          "preimage records are not strictly ascending by keccak256(address)");
    }
  }

  /** {@code hash} against the 32 bytes of {@code key} at {@code offset}, unsigned. */
  private static int compare(final Bytes hash, final byte[] key, final int offset) {
    return Arrays.compareUnsigned(
        hash.toArrayUnsafe(), 0, Bytes32.SIZE, key, offset, offset + Bytes32.SIZE);
  }

  private void finish() {
    done = true;
    try {
      preimages.close();
    } catch (final IOException e) {
      LOG.debug("Failed to close the preimages of range {}", range, e);
    }
    if (orphans > 0) {
      LOG.debug("Range {}: {} slot(s) of no account skipped", range, orphans);
    }
  }

  /**
   * The offset where each range starts in a sorted preimage file, then the file size: range {@code
   * r} spans {@code [starts[r], starts[r + 1])}. Hashes about one account in {@link #STRIDE}.
   */
  static long[] starts(final Path path) throws IOException {
    final long[] starts = new long[RANGES + 1];
    final long[] offsets = new long[STRIDE];
    final Address[] addresses = new Address[STRIDE];
    int nextRange = 0;
    try (final Eip8347PreimageFile file = Eip8347PreimageFile.withoutOrderCheck(path)) {
      int read;
      do {
        read = readStride(file, offsets, addresses);
        // the ranges starting in this stride, found by bisection
        final int last = read == 0 ? -1 : rangeOf(addresses[read - 1]);
        while (nextRange <= last) {
          starts[nextRange] = offsets[firstFrom(addresses, read, nextRange)];
          nextRange++;
        }
      } while (read == STRIDE);
      Arrays.fill(starts, nextRange, RANGES + 1, file.position());
    }
    // bisection is only right on sorted records
    for (int range = 0; range < RANGES; range++) {
      if (starts[range] > starts[range + 1]) {
        throw new Eip8347ArtifactVerificationException(
            "preimage records are not strictly ascending by keccak256(address)");
      }
    }
    return starts;
  }

  /** Reads up to {@link #STRIDE} accounts, their offsets and addresses, skipping their slots. */
  private static int readStride(
      final Eip8347PreimageFile file, final long[] offsets, final Address[] addresses)
      throws IOException {
    int read = 0;
    boolean more = true;
    while (more && read < STRIDE) {
      offsets[read] = file.position();
      final Eip8347PreimageFile.Account account = file.nextAccount();
      more = account != null;
      if (more) {
        file.skipSlots();
        addresses[read++] = account.address();
      }
    }
    return read;
  }

  /** The first of {@code count} sorted addresses whose range is at least {@code range}. */
  private static int firstFrom(final Address[] addresses, final int count, final int range) {
    int low = 0;
    int high = count - 1;
    while (low < high) {
      final int middle = (low + high) >>> 1;
      if (rangeOf(addresses[middle]) < range) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  private static int rangeOf(final Address address) {
    return FlatState.rangeOf(FlatState.account(address));
  }
}
