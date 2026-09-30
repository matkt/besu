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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.verify;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader.StorageUnit;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.HeaderRecord;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.Leaf;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter.Entry;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347Pipelines;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Consensus-anchoring join between the preimages and the snapshot's header and storage records.
 *
 * <p>Preimages are keccak-ordered, snapshot records {@code key_hash}-ordered (BLAKE3), so every
 * preimage account and slot is first turned into a <em>request</em> keyed by its PBT position and
 * externally sorted. The snapshot is then walked once, in the same order, as a merge: each record
 * and header slot / storage entry must meet exactly one request, and each request exactly one
 * snapshot entry. A snapshot entry without a request is a surplus leaf (e.g. an extra header slot);
 * a request without a snapshot entry is a missing leaf. No index and no per-leaf bitset is kept.
 *
 * <p>The link back to MPT order is the entry's <em>ordinal</em>: its rank in the preimage file,
 * which is already in MPT order. Each account's slots are numbered first and the account right
 * after them, the order {@link Eip8347DualCheckVerifier#rebuildMptRoot} consumes (a storage root
 * before its account). Matched values are re-emitted keyed by that ordinal, so neither sort carries
 * a keccak: the MPT rebuild re-reads the preimage file for the paths. Contract code is handed to
 * {@link Eip8347CodeCheck}.
 *
 * <pre>
 * request key                            request value
 *   ACCOUNT_ZONE|addressHash (33 B)        ordinal[8]
 *   header or storage leaf key (34/66 B)   ordinal[8]
 * MPT value key                          MPT value
 *   ordinal[8] of a slot                   value, leading zeros trimmed
 *   ordinal[8] of an account               {@code AccountFields}
 * </pre>
 */
final class Eip8347AnchorJoin {

  /** An account's MPT fields as sorted: {@code nonce[8] | codeHash[32] | balance (trimmed)}. */
  record AccountFields(long nonce, Hash codeHash, Wei balance) {

    byte[] encode() {
      final byte[] trimmedBalance = balance.toBytes().trimLeadingZeros().toArrayUnsafe();
      return ByteBuffer.allocate(Long.BYTES + Bytes32.SIZE + trimmedBalance.length)
          .putLong(nonce)
          .put(codeHash.getBytes().toArrayUnsafe())
          .put(trimmedBalance)
          .array();
    }

    static AccountFields decode(final byte[] encoded) {
      final ByteBuffer buffer = ByteBuffer.wrap(encoded);
      final long nonce = buffer.getLong();
      final byte[] codeHash = new byte[Bytes32.SIZE];
      buffer.get(codeHash);
      final byte[] balance = new byte[buffer.remaining()];
      buffer.get(balance);
      return new AccountFields(
          nonce,
          Hash.wrap(Bytes32.wrap(codeHash)),
          Wei.wrap(UInt256.fromBytes(Bytes.wrap(balance))));
    }
  }

  private final Path preimagesPath;
  private final PeekingIterator<Entry> preimageRequests;
  private final Eip8347ExternalSorter mptValues;
  private final Eip8347CodeCheck codeCheck;

  Eip8347AnchorJoin(
      final Path preimagesPath,
      final Iterator<Entry> sortedRequests,
      final Eip8347ExternalSorter mptValues,
      final Eip8347CodeCheck codeCheck) {
    this.preimagesPath = preimagesPath;
    this.preimageRequests = Iterators.peekingIterator(sortedRequests);
    this.mptValues = mptValues;
    this.codeCheck = codeCheck;
  }

  /**
   * Reads the preimage file into {@code preimageRequests}, hashing PBT keys in parallel. The keccak
   * order is not checked here: {@link Eip8347DualCheckVerifier#rebuildMptRoot} reads the file again
   * and checks it, and nothing before relies on it.
   */
  static void sortPreimageRequests(
      final Path preimagesPath, final Eip8347ExternalSorter preimageRequests) throws IOException {
    try (final Eip8347PreimageFile preimages =
        Eip8347PreimageFile.withoutOrderCheck(preimagesPath)) {
      Eip8347Pipelines.run(
          Eip8347Pipelines.from("eip8347-verify-preimages", numbered(preimages.batches(1024)))
              .thenProcessInParallel(
                  "eip8347-verify-preimage-requests",
                  Eip8347AnchorJoin::requestsOf,
                  Eip8347Pipelines.PARALLELISM)
              .andFinishWith(
                  "eip8347-verify-sort-preimage-requests",
                  batch -> {
                    try {
                      for (final Entry entry : batch) {
                        preimageRequests.add(entry.key(), entry.value());
                      }
                    } catch (final IOException e) {
                      throw new UncheckedIOException(e);
                    }
                  }));
      preimages.ensureExhausted();
    }
  }

  /** A batch with the ordinal of its first slot and of its account. */
  private record NumberedBatch(
      Eip8347PreimageFile.Batch batch, long firstSlotOrdinal, long accountOrdinal) {}

  /** Numbers batches in file order (sequential); an account takes the ordinal after its slots. */
  private static Iterator<NumberedBatch> numbered(final Iterator<Eip8347PreimageFile.Batch> in) {
    return new Iterator<>() {
      private long nextOrdinal;
      private long nextSlotOrdinal;
      private long accountOrdinal;

      @Override
      public boolean hasNext() {
        return in.hasNext();
      }

      @Override
      public NumberedBatch next() {
        final Eip8347PreimageFile.Batch batch = in.next();
        if (batch.first()) {
          nextSlotOrdinal = nextOrdinal;
          accountOrdinal = nextOrdinal + batch.account().slotCount();
          nextOrdinal = accountOrdinal + 1;
        }
        final NumberedBatch numbered = new NumberedBatch(batch, nextSlotOrdinal, accountOrdinal);
        nextSlotOrdinal += batch.slots().size();
        return numbered;
      }
    };
  }

  private static List<Entry> requestsOf(final NumberedBatch numbered) {
    final Eip8347PreimageFile.Batch batch = numbered.batch();
    final Bytes32 address32 =
        TrieKeyDerivation.address20ToAddress32(batch.account().address().getBytes());
    final Bytes32 addressKeyHash = TrieKeyDerivation.keyHash(address32);
    final List<Entry> entries = new ArrayList<>(batch.slots().size() + 1);
    if (batch.first()) {
      entries.add(
          new Entry(
              Eip8347TypedSnapshotCodec.headerStem(addressKeyHash).toArray(),
              ordinal(numbered.accountOrdinal())));
    }
    long slotOrdinal = numbered.firstSlotOrdinal();
    for (final Eip8347PreimageFile.Slot slot : batch.slots()) {
      entries.add(
          new Entry(
              TrieKeyDerivation.getTreeKeyForStorageSlot(
                      address32, addressKeyHash, UInt256.fromBytes(slot.key()))
                  .toArray(),
              ordinal(slotOrdinal++)));
    }
    return entries;
  }

  void header(final HeaderRecord header) throws IOException {
    final Bytes stem = header.stem();
    final byte[] accountOrdinal = match(stem, "header record").value();
    for (final var slot : header.slots().entrySet()) {
      final Bytes key =
          Bytes.concatenate(
              stem, Bytes.of((byte) (EmbeddingParameters.HEADER_STORAGE_OFFSET + slot.getKey())));
      addSlot(match(key, "header slot"), slot.getValue());
    }
    if (header.kind() == Eip8347TypedSnapshotCodec.KIND_CODE) {
      codeCheck.request(header.codeHash(), header.codeSize());
    }
    mptValues.add(
        accountOrdinal,
        new AccountFields(header.nonce(), header.mptCodeHash(), Wei.of(header.balance())).encode());
  }

  void storage(final StorageUnit unit) throws IOException {
    for (final Leaf leaf : unit.leaves()) {
      addSlot(match(leaf.key(), "storage leaf"), leaf.value());
    }
  }

  /** Fails if a preimage account or slot met no snapshot entry. */
  void finish() throws IOException {
    if (preimageRequests.hasNext()) {
      throw missing(preimageRequests.peek());
    }
  }

  /** Consumes the request for {@code snapshotKey}; rejects surplus and missing entries. */
  private Entry match(final Bytes snapshotKey, final String what) throws IOException {
    final byte[] key = snapshotKey.toArrayUnsafe();
    final int cmp =
        preimageRequests.hasNext() ? Arrays.compareUnsigned(preimageRequests.peek().key(), key) : 1;
    if (cmp < 0) {
      throw missing(preimageRequests.peek());
    }
    if (cmp > 0) {
      throw new Eip8347ArtifactVerificationException(
          "snapshot "
              + what
              + " "
              + snapshotKey.toHexString()
              + " is not covered by consensus anchoring (no matching preimage)");
    }
    return preimageRequests.next();
  }

  private void addSlot(final Entry request, final Bytes32 value) throws IOException {
    mptValues.add(request.value(), value.trimLeadingZeros().toArray());
  }

  /** Rejects a request without snapshot entry, naming it by re-reading the preimage file. */
  private Eip8347ArtifactVerificationException missing(final Entry request) throws IOException {
    return new Eip8347ArtifactVerificationException(
        "preimage "
            + describe(preimagesPath, ByteBuffer.wrap(request.value()).getLong())
            + " has no snapshot leaf");
  }

  /** The account or slot at {@code ordinal} (only used to report a rejection). */
  private static String describe(final Path preimagesPath, final long ordinal) throws IOException {
    try (final Eip8347PreimageFile preimages = new Eip8347PreimageFile(preimagesPath)) {
      long next = 0;
      Eip8347PreimageFile.Account account;
      while ((account = preimages.nextAccount()) != null) {
        final long accountOrdinal = next + account.slotCount();
        if (ordinal == accountOrdinal) {
          return "account " + account.address().toHexString();
        }
        for (int i = 0; i < account.slotCount(); i++) {
          final Eip8347PreimageFile.Slot slot = preimages.nextSlot();
          if (next + i == ordinal) {
            return "slot " + slot.key().toHexString() + " of " + account.address().toHexString();
          }
        }
        next = accountOrdinal + 1;
      }
    }
    return "#" + ordinal;
  }

  private static byte[] ordinal(final long ordinal) {
    return ByteBuffer.allocate(Long.BYTES).putLong(ordinal).array();
  }
}
