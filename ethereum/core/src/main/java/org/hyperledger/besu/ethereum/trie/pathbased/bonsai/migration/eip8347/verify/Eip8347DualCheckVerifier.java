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
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieConstants;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.AscendingCollapseBinaryTrie;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotLeaf;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader.CodeUnit;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader.HeaderUnit;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader.StorageUnit;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader.Unit;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter.Entry;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347Pipelines;
import org.hyperledger.besu.ethereum.trie.patricia.AscendingCollapsePatriciaTrie;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EIP-8347 dual-check verifier: internal PBT consistency plus consensus anchoring against an MPT
 * {@code stateRoot}, in bounded memory.
 *
 * <p>Nothing is indexed or random-accessed. Every step streams, and the two order changes the check
 * needs (keccak preimages → PBT order, then PBT order → MPT order) go through external sorts in a
 * temp directory next to the snapshot:
 *
 * <ol>
 *   <li>Preimages → join requests sorted by PBT key ({@link Eip8347AnchorJoin#sortRequests}).
 *   <li>One pass over the snapshot, as a pipeline: read units → PBT insert → join (headers and
 *       storage against the requests, code groups against the code requests).
 *   <li>Code check per {@code code_hash} ({@link Eip8347CodeCheck}).
 *   <li>MPT rebuild from the matched values, sorted by MPT path ({@link #rebuildMptRoot}).
 * </ol>
 */
public final class Eip8347DualCheckVerifier {

  private static final Logger LOG = LoggerFactory.getLogger(Eip8347DualCheckVerifier.class);

  private Eip8347DualCheckVerifier() {}

  /**
   * Verifies a snapshot/preimage pair against {@code expectedMptStateRoot}.
   *
   * @throws Eip8347ArtifactVerificationException if either check fails
   */
  public static void verify(
      final Path snapshotPath, final Path preimagesPath, final Bytes32 expectedMptStateRoot)
      throws IOException {
    verify(
        snapshotPath,
        preimagesPath,
        expectedMptStateRoot,
        Eip8347ExternalSorter.DEFAULT_BUFFER_BYTES);
  }

  /**
   * Same as {@link #verify(Path, Path, Bytes32)} with an explicit in-heap buffer per external sort
   * (tests use a tiny one to force multi-run, multi-pass merges).
   */
  public static void verify(
      final Path snapshotPath,
      final Path preimagesPath,
      final Bytes32 expectedMptStateRoot,
      final long sortBufferBytes)
      throws IOException {
    Objects.requireNonNull(snapshotPath, "snapshotPath");
    Objects.requireNonNull(preimagesPath, "preimagesPath");
    Objects.requireNonNull(expectedMptStateRoot, "expectedMptStateRoot");

    final Path workDir =
        Files.createTempDirectory(snapshotPath.toAbsolutePath().getParent(), "eip8347-verify-");
    try (final Eip8347ExternalSorter requests = sorter(workDir, "requests", sortBufferBytes);
        final Eip8347ExternalSorter mptEntries = sorter(workDir, "mpt", sortBufferBytes);
        final Eip8347ExternalSorter codeRequests =
            sorter(workDir, "code-requests", sortBufferBytes);
        final Eip8347ExternalSorter codeGroups = sorter(workDir, "code-groups", sortBufferBytes)) {

      Eip8347AnchorJoin.sortRequests(preimagesPath, requests);

      final Eip8347CodeCheck codeCheck = new Eip8347CodeCheck(codeRequests, codeGroups);
      final Eip8347AnchorJoin anchorJoin =
          new Eip8347AnchorJoin(requests.sorted(), mptEntries, codeCheck);
      final AscendingCollapseBinaryTrie pbt = new AscendingCollapseBinaryTrie();
      final Bytes32 claimedRoot;
      final long leafCount;
      try (final Eip8347SnapshotReader snapshot = new Eip8347SnapshotReader(snapshotPath)) {
        Eip8347Pipelines.run(
            Eip8347Pipelines.from("eip8347-verify-snapshot", units(snapshot))
                .thenProcess("eip8347-verify-pbt", unit -> insertLeaves(pbt, unit))
                .andFinishWith("eip8347-verify-join", unit -> join(unit, anchorJoin, codeCheck)));
        snapshot.ensureExhausted();
        claimedRoot = snapshot.claimedRoot();
        leafCount = snapshot.leafCount();
      }
      final Bytes32 computedPbtRoot =
          leafCount == 0 ? TrieConstants.EMPTY_TRIE_ROOT : pbt.rootHash();
      if (!computedPbtRoot.equals(claimedRoot)) {
        throw new Eip8347ArtifactVerificationException(
            "PBT root mismatch: computed "
                + computedPbtRoot.toHexString()
                + " claimed "
                + claimedRoot.toHexString());
      }
      LOG.info(
          "EIP-8347 internal PBT consistency OK (leaves={}, root={})",
          leafCount,
          computedPbtRoot.toHexString());

      anchorJoin.finish();
      codeCheck.finish();
      final Bytes32 computedMptRoot = rebuildMptRoot(mptEntries.sorted());
      if (!computedMptRoot.equals(expectedMptStateRoot)) {
        throw new Eip8347ArtifactVerificationException(
            "MPT stateRoot mismatch: computed "
                + computedMptRoot.toHexString()
                + " expected "
                + expectedMptStateRoot.toHexString());
      }
      LOG.info("EIP-8347 consensus anchoring OK (stateRoot={})", computedMptRoot.toHexString());
    } finally {
      deleteRecursively(workDir);
    }
  }

  private static Unit insertLeaves(final AscendingCollapseBinaryTrie pbt, final Unit unit) {
    for (final Eip8347SnapshotLeaf leaf : unit.leaves()) {
      try {
        pbt.insert(leaf.key(), leaf.value());
      } catch (final IllegalArgumentException e) {
        throw new Eip8347ArtifactVerificationException(e.getMessage(), e);
      }
    }
    return unit;
  }

  private static void join(
      final Unit unit, final Eip8347AnchorJoin anchorJoin, final Eip8347CodeCheck codeCheck) {
    try {
      switch (unit) {
        case HeaderUnit header -> anchorJoin.header(header.header());
        case CodeUnit code -> codeCheck.group(code);
        case StorageUnit storage -> anchorJoin.storage(storage);
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Streams MPT entries in MPT order ({@code keccak(address)}, storage slots before the account)
   * into ascending-collapse Patricia tries: one storage trie at a time, one account trie.
   */
  static Bytes32 rebuildMptRoot(final Iterator<Entry> entries) {
    final AscendingCollapsePatriciaTrie accountTrie = new AscendingCollapsePatriciaTrie();
    AscendingCollapsePatriciaTrie storageTrie = null;
    Bytes32 storageOwner = null;
    try {
      while (entries.hasNext()) {
        final Entry entry = entries.next();
        final Bytes32 addressPath = Bytes32.wrap(entry.key(), 0);
        if (entry.key()[32] == Eip8347AnchorJoin.SLOT_TAG) {
          if (!addressPath.equals(storageOwner)) {
            storageTrie = new AscendingCollapsePatriciaTrie();
            storageOwner = addressPath;
          }
          final Bytes value = Bytes.wrap(entry.value());
          storageTrie.insert(
              Bytes.wrap(entry.key(), 33, 32), RLP.encode(out -> out.writeBytes(value)));
          continue;
        }
        final Hash storageRoot =
            addressPath.equals(storageOwner)
                ? Hash.wrap(storageTrie.rootHash())
                : Hash.EMPTY_TRIE_HASH;
        storageTrie = null;
        storageOwner = null;
        final ByteBuffer value = ByteBuffer.wrap(entry.value());
        final long nonce = value.getLong();
        final byte[] codeHash = new byte[32];
        value.get(codeHash);
        final Wei balance =
            Wei.wrap(UInt256.fromBytes(Bytes.wrap(entry.value(), 40, entry.value().length - 40)));
        final PatriciaTrieAccountValue account =
            new PatriciaTrieAccountValue(
                nonce, balance, storageRoot, Hash.wrap(Bytes32.wrap(codeHash)));
        accountTrie.insert(addressPath, RLP.encode(account::writeTo));
      }
    } catch (final IllegalArgumentException e) {
      throw new Eip8347ArtifactVerificationException(e.getMessage(), e);
    }
    return accountTrie.rootHash();
  }

  private static Iterator<Unit> units(final Eip8347SnapshotReader snapshot) {
    return new Iterator<>() {
      private Unit next;
      private boolean done;

      @Override
      public boolean hasNext() {
        if (next == null && !done) {
          try {
            next = snapshot.next();
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
          done = next == null;
        }
        return next != null;
      }

      @Override
      public Unit next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final Unit unit = next;
        next = null;
        return unit;
      }
    };
  }

  private static Eip8347ExternalSorter sorter(
      final Path workDir, final String name, final long bufferBytes) {
    return new Eip8347ExternalSorter(workDir, name, bufferBytes);
  }

  private static void deleteRecursively(final Path dir) {
    try (final var walk = Files.walk(dir)) {
      walk.sorted(Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (final IOException e) {
                  LOG.debug("failed deleting verify work path {}", p, e);
                }
              });
    } catch (final IOException e) {
      LOG.debug("failed walking verify work dir {}", dir, e);
    }
  }
}
