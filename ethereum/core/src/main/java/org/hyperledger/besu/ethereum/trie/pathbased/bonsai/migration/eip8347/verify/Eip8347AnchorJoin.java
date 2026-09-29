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

import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotLeaf;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader.StorageUnit;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.HeaderRecord;
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
 * <p>Matched values are re-emitted keyed by their MPT path for {@link
 * Eip8347DualCheckVerifier#rebuildMptRoot}, and contract code is handed to {@link
 * Eip8347CodeCheck}.
 *
 * <pre>
 * request key                            request value
 *   ACCOUNT_ZONE|addressHash (33 B)        keccak(address)[32] | address[20]
 *   header or storage leaf key (34/66 B)   keccak(address)[32] | keccak(slotKey)[32]
 * MPT entry key                          MPT entry value
 *   keccak(address) | 0x00 | keccak(slot)  value, leading zeros trimmed
 *   keccak(address) | 0x01                 nonce[8] | codeHash[32] | balance (trimmed)
 * </pre>
 *
 * <p>The {@code 0x00}/{@code 0x01} tag sorts an account's storage slots before its account entry.
 */
final class Eip8347AnchorJoin {

  static final byte SLOT_TAG = 0x00;
  static final byte ACCOUNT_TAG = 0x01;

  private final PeekingIterator<Entry> requests;
  private final Eip8347ExternalSorter mptEntries;
  private final Eip8347CodeCheck codeCheck;

  Eip8347AnchorJoin(
      final Iterator<Entry> sortedRequests,
      final Eip8347ExternalSorter mptEntries,
      final Eip8347CodeCheck codeCheck) {
    this.requests = Iterators.peekingIterator(sortedRequests);
    this.mptEntries = mptEntries;
    this.codeCheck = codeCheck;
  }

  /** Reads the preimage file into {@code requests}, hashing PBT keys in parallel. */
  static void sortRequests(final Path preimagesPath, final Eip8347ExternalSorter requests)
      throws IOException {
    try (final Eip8347PreimageFile preimages = new Eip8347PreimageFile(preimagesPath)) {
      Eip8347Pipelines.run(
          Eip8347Pipelines.from("eip8347-verify-preimages", preimages.batches(1024))
              .thenProcessInParallel(
                  "eip8347-verify-requests",
                  Eip8347AnchorJoin::requestsOf,
                  Eip8347Pipelines.PARALLELISM)
              .andFinishWith(
                  "eip8347-verify-sort-requests",
                  batch -> {
                    try {
                      for (final Entry entry : batch) {
                        requests.add(entry.key(), entry.value());
                      }
                    } catch (final IOException e) {
                      throw new UncheckedIOException(e);
                    }
                  }));
      preimages.ensureExhausted();
    }
  }

  private static List<Entry> requestsOf(final Eip8347PreimageFile.Batch batch) {
    final Eip8347PreimageFile.Account account = batch.account();
    final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(account.address().getBytes());
    final byte[] keccakAddress = account.addressHash().getBytes().toArrayUnsafe();
    final List<Entry> entries = new ArrayList<>(batch.slots().size() + 1);
    if (batch.first()) {
      entries.add(
          new Entry(
              Eip8347TypedSnapshotCodec.headerStem(TrieKeyDerivation.keyHash(address32)).toArray(),
              concat(keccakAddress, account.address().getBytes().toArrayUnsafe())));
    }
    for (final Eip8347PreimageFile.Slot slot : batch.slots()) {
      entries.add(
          new Entry(
              TrieKeyDerivation.getTreeKeyForStorageSlot(address32, UInt256.fromBytes(slot.key()))
                  .toArray(),
              concat(keccakAddress, slot.keyHash().getBytes().toArrayUnsafe())));
    }
    return entries;
  }

  void header(final HeaderRecord header) throws IOException {
    final Bytes stem = header.stem();
    final byte[] keccakAddress = Arrays.copyOf(match(stem, "header record").value(), 32);
    for (final var slot : header.slots().entrySet()) {
      final Bytes key =
          Bytes.concatenate(
              stem, Bytes.of((byte) (EmbeddingParameters.HEADER_STORAGE_OFFSET + slot.getKey())));
      addSlot(match(key, "header slot"), slot.getValue());
    }
    if (header.kind() == Eip8347TypedSnapshotCodec.KIND_CODE) {
      codeCheck.request(header.codeHash(), header.codeSize());
    }
    mptEntries.add(
        concat(keccakAddress, new byte[] {ACCOUNT_TAG}),
        concat(
            ByteBuffer.allocate(Long.BYTES).putLong(header.nonce()).array(),
            header.mptCodeHash().getBytes().toArrayUnsafe(),
            header.balance().toBytes().trimLeadingZeros().toArrayUnsafe()));
  }

  void storage(final StorageUnit unit) throws IOException {
    for (final Eip8347SnapshotLeaf leaf : unit.leaves()) {
      addSlot(match(leaf.key(), "storage leaf"), leaf.value());
    }
  }

  /** Fails if a preimage account or slot met no snapshot entry. */
  void finish() {
    if (requests.hasNext()) {
      throw missing(requests.peek());
    }
  }

  /** Consumes the request for {@code snapshotKey}; rejects surplus and missing entries. */
  private Entry match(final Bytes snapshotKey, final String what) {
    final byte[] key = snapshotKey.toArrayUnsafe();
    final int cmp = requests.hasNext() ? Arrays.compareUnsigned(requests.peek().key(), key) : 1;
    if (cmp < 0) {
      throw missing(requests.peek());
    }
    if (cmp > 0) {
      throw new Eip8347ArtifactVerificationException(
          "snapshot "
              + what
              + " "
              + snapshotKey.toHexString()
              + " is not covered by consensus anchoring (no matching preimage)");
    }
    return requests.next();
  }

  private void addSlot(final Entry request, final Bytes32 value) throws IOException {
    final byte[] mptPath = request.value();
    mptEntries.add(
        concat(
            Arrays.copyOf(mptPath, 32), new byte[] {SLOT_TAG}, Arrays.copyOfRange(mptPath, 32, 64)),
        value.trimLeadingZeros().toArray());
  }

  private static Eip8347ArtifactVerificationException missing(final Entry request) {
    final Bytes value = Bytes.wrap(request.value());
    final String what =
        request.key().length == 33
            ? "account " + value.slice(32, 20).toHexString()
            : "slot with keccak "
                + value.slice(32, 32).toHexString()
                + " of account keccak "
                + value.slice(0, 32).toHexString();
    return new Eip8347ArtifactVerificationException("preimage " + what + " has no snapshot leaf");
  }

  private static byte[] concat(final byte[]... parts) {
    int length = 0;
    for (final byte[] part : parts) {
      length += part.length;
    }
    final byte[] out = new byte[length];
    int at = 0;
    for (final byte[] part : parts) {
      System.arraycopy(part, 0, out, at, part.length);
      at += part.length;
    }
    return out;
  }
}
