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
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.AscendingCollapseBinaryTrie;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader.CodeUnit;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader.HeaderUnit;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader.StorageUnit;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader.Unit;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.Leaf;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter.Entry;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347Pipelines;
import org.hyperledger.besu.ethereum.trie.patricia.AscendingCollapsePatriciaTrie;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EIP-8347 dual-check verifier: internal PBT consistency plus consensus anchoring against an MPT
 * {@code stateRoot}, in bounded memory.
 *
 * <p>Nothing is indexed or random-accessed. Every step streams, and the two order changes the check
 * needs (keccak preimages → PBT order, then PBT order → MPT order) go through external sorts in a
 * work directory:
 *
 * <ol>
 *   <li>Preimages → join requests sorted by PBT key ({@link
 *       Eip8347AnchorJoin#sortPreimageRequests}).
 *   <li>One pass over the snapshot, as a pipeline: read units → PBT insert → join (headers and
 *       storage against the requests, code groups against the code requests).
 *   <li>Code check per {@code code_hash} ({@link Eip8347CodeCheck}).
 *   <li>MPT rebuild from the preimage file and the matched values sorted by ordinal ({@link
 *       #rebuildMptRoot}).
 * </ol>
 *
 * <p>The code check (3) and the MPT rebuild (4) are independent and run at the same time.
 *
 * <p>{@link #verify} only checks. {@link #verifyAndLoad} also writes the PBT during the same read:
 * the PBT-insert step persists every node through a {@link NodeUpdater}, and the code check writes
 * the code reference counts the live trie needs. It writes no cursor, so on a rejection the caller
 * discards what was written.
 */
public final class Eip8347DualCheckVerifier {

  private static final Logger LOG = LoggerFactory.getLogger(Eip8347DualCheckVerifier.class);

  private Eip8347DualCheckVerifier() {}

  /**
   * What {@link #verifyAndLoad} wrote.
   *
   * @param pbtRoot PBT root, equal to the snapshot's claimed root
   * @param leafCount leaves written
   * @param codeHashes distinct code hashes given a reference count
   */
  public record Loaded(Bytes32 pbtRoot, long leafCount, long codeHashes) {}

  /**
   * Verifies a snapshot/preimage pair against {@code expectedMptStateRoot}.
   *
   * @param workDir where the external sorts spill; a subdirectory is created and deleted
   * @throws Eip8347ArtifactVerificationException if either check fails
   */
  public static void verify(
      final Path snapshotPath,
      final Path preimagesPath,
      final Bytes32 expectedMptStateRoot,
      final Path workDir)
      throws IOException {
    verify(
        snapshotPath,
        preimagesPath,
        expectedMptStateRoot,
        workDir,
        Eip8347ExternalSorter.DEFAULT_BUFFER_BYTES);
  }

  /**
   * Same as {@link #verify(Path, Path, Bytes32, Path)} with an explicit in-heap buffer per external
   * sort (tests use a tiny one to force multi-run, multi-pass merges).
   */
  static void verify(
      final Path snapshotPath,
      final Path preimagesPath,
      final Bytes32 expectedMptStateRoot,
      final Path workDir,
      final long sortBufferBytes)
      throws IOException {
    run(
        snapshotPath,
        preimagesPath,
        expectedMptStateRoot,
        workDir,
        sortBufferBytes,
        Optional.empty());
  }

  /**
   * Verifies like {@link #verify(Path, Path, Bytes32, Path)} and, in the same read of the snapshot,
   * writes the PBT to {@code nodes}: every node at its location, and the code reference counts. No
   * cursor is written; on an exception the caller discards the writes.
   *
   * @throws Eip8347ArtifactVerificationException if either check fails
   */
  public static Loaded verifyAndLoad(
      final Path snapshotPath,
      final Path preimagesPath,
      final Bytes32 expectedMptStateRoot,
      final Path workDir,
      final NodeUpdater nodes)
      throws IOException {
    return verifyAndLoad(
        snapshotPath,
        preimagesPath,
        expectedMptStateRoot,
        workDir,
        nodes,
        Eip8347ExternalSorter.DEFAULT_BUFFER_BYTES);
  }

  /**
   * Same as {@link #verifyAndLoad(Path, Path, Bytes32, Path, NodeUpdater)} with an explicit sort
   * buffer.
   */
  static Loaded verifyAndLoad(
      final Path snapshotPath,
      final Path preimagesPath,
      final Bytes32 expectedMptStateRoot,
      final Path workDir,
      final NodeUpdater nodes,
      final long sortBufferBytes)
      throws IOException {
    return run(
        snapshotPath,
        preimagesPath,
        expectedMptStateRoot,
        workDir,
        sortBufferBytes,
        Optional.of(Objects.requireNonNull(nodes, "nodes")));
  }

  private static Loaded run(
      final Path snapshotPath,
      final Path preimagesPath,
      final Bytes32 expectedMptStateRoot,
      final Path workDir,
      final long sortBufferBytes,
      final Optional<NodeUpdater> nodes)
      throws IOException {
    Objects.requireNonNull(snapshotPath, "snapshotPath");
    Objects.requireNonNull(preimagesPath, "preimagesPath");
    Objects.requireNonNull(expectedMptStateRoot, "expectedMptStateRoot");

    final Path sortDir =
        Files.createTempDirectory(Files.createDirectories(workDir), "eip8347-verify-");
    try (final Eip8347ExternalSorter preimageRequests =
            new Eip8347ExternalSorter(sortDir, "preimage-requests", sortBufferBytes);
        final Eip8347ExternalSorter mptValues =
            new Eip8347ExternalSorter(sortDir, "mpt-values", sortBufferBytes);
        final Eip8347ExternalSorter codeGroupRequests =
            new Eip8347ExternalSorter(sortDir, "code-group-requests", sortBufferBytes);
        final Eip8347ExternalSorter codeGroupsByCode =
            new Eip8347ExternalSorter(sortDir, "code-groups-by-code", sortBufferBytes)) {

      Eip8347AnchorJoin.sortPreimageRequests(preimagesPath, preimageRequests);

      final Eip8347CodeCheck codeCheck = new Eip8347CodeCheck(codeGroupRequests, codeGroupsByCode);
      final Eip8347AnchorJoin anchorJoin =
          new Eip8347AnchorJoin(preimagesPath, preimageRequests.sorted(), mptValues, codeCheck);
      final AscendingCollapseBinaryTrie pbt =
          nodes.map(AscendingCollapseBinaryTrie::new).orElseGet(AscendingCollapseBinaryTrie::new);
      final Bytes32 claimedRoot;
      final long leafCount;
      try (final Eip8347SnapshotReader snapshot = new Eip8347SnapshotReader(snapshotPath)) {
        // A unit is one whole stem: its leaves are hashed (and encoded) in parallel, and only the
        // stem's top node is attached on the PBT thread.
        Eip8347Pipelines.run(
            Eip8347Pipelines.from("eip8347-verify-snapshot", snapshot.units())
                .thenProcessAsyncOrdered(
                    "eip8347-verify-hash-stems",
                    Eip8347Pipelines.async(unit -> new HashedUnit(unit, prepare(pbt, unit))),
                    Eip8347Pipelines.PARALLELISM)
                .thenProcess("eip8347-verify-pbt", hashed -> attach(pbt, hashed))
                .andFinishWith("eip8347-verify-join", unit -> join(unit, anchorJoin, codeCheck)));
        snapshot.ensureExhausted();
        claimedRoot = snapshot.claimedRoot();
        leafCount = snapshot.leafCount();
      }
      final Bytes32 computedPbtRoot = pbt.rootHash();
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
      preimageRequests.close(); // consumed: free its runs before the MPT sort merges

      final CompletableFuture<Long> codeHashes =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return codeCheck.finish(nodes);
                } catch (final IOException e) {
                  throw new UncheckedIOException(e);
                }
              },
              task -> new Thread(task, "eip8347-verify-code-check").start());
      try {
        final Bytes32 computedMptRoot;
        try (final Eip8347PreimageFile preimages = new Eip8347PreimageFile(preimagesPath)) {
          computedMptRoot = rebuildMptRoot(preimages, mptValues.sorted());
        }
        if (!computedMptRoot.equals(expectedMptStateRoot)) {
          throw new Eip8347ArtifactVerificationException(
              "MPT stateRoot mismatch: computed "
                  + computedMptRoot.toHexString()
                  + " expected "
                  + expectedMptStateRoot.toHexString());
        }
        LOG.info("EIP-8347 consensus anchoring OK (stateRoot={})", computedMptRoot.toHexString());
        return new Loaded(computedPbtRoot, leafCount, Eip8347Pipelines.await(codeHashes));
      } finally {
        // The code check reads files under sortDir: let it end before they are deleted. Its own
        // failure, if the MPT already failed, is the second fault and is not reported.
        codeHashes.exceptionally(failure -> 0L).join();
      }
    } finally {
      Eip8347ExternalSorter.deleteRecursively(sortDir);
    }
  }

  /** A snapshot unit and its stem, prepared for the PBT. */
  private record HashedUnit(Unit unit, AscendingCollapseBinaryTrie.Subtree stem) {}

  private static AscendingCollapseBinaryTrie.Subtree prepare(
      final AscendingCollapseBinaryTrie pbt, final Unit unit) {
    final List<Leaf> leaves = unit.leaves();
    try {
      return pbt.prepare(
          leaves.stream().map(Leaf::key).toList(),
          leaves.stream().<Bytes>map(Leaf::value).toList());
    } catch (final IllegalArgumentException e) {
      throw new Eip8347ArtifactVerificationException(e.getMessage(), e);
    }
  }

  private static Unit attach(final AscendingCollapseBinaryTrie pbt, final HashedUnit hashed) {
    try {
      pbt.insert(hashed.stem());
    } catch (final IllegalArgumentException e) {
      throw new Eip8347ArtifactVerificationException(e.getMessage(), e);
    }
    return hashed.unit();
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
   * Rebuilds the MPT root from the preimage file (paths, in MPT order) and the matched values
   * sorted by ordinal (see {@link Eip8347AnchorJoin}): each account's slots feed one
   * ascending-collapse storage trie, then the account goes into the account trie. The join matched
   * every preimage entry exactly once, so the values are exactly the ordinals {@code 0..n-1}.
   */
  static Bytes32 rebuildMptRoot(final Eip8347PreimageFile preimages, final Iterator<Entry> values)
      throws IOException {
    final AscendingCollapsePatriciaTrie accountTrie = new AscendingCollapsePatriciaTrie();
    long ordinal = 0;
    try {
      Eip8347PreimageFile.Account account;
      while ((account = preimages.nextAccount()) != null) {
        final AscendingCollapsePatriciaTrie storageTrie = new AscendingCollapsePatriciaTrie();
        for (int i = 0; i < account.slotCount(); i++) {
          final Bytes value = Bytes.wrap(nextValue(values, ordinal++));
          storageTrie.insert(
              preimages.nextSlot().keyHash().getBytes(), RLP.encode(out -> out.writeBytes(value)));
        }
        final Hash storageRoot = Hash.wrap(storageTrie.rootHash());
        final Eip8347AnchorJoin.AccountFields fields =
            Eip8347AnchorJoin.AccountFields.decode(nextValue(values, ordinal++));
        final PatriciaTrieAccountValue value =
            new PatriciaTrieAccountValue(
                fields.nonce(), fields.balance(), storageRoot, fields.codeHash());
        accountTrie.insert(account.addressHash().getBytes(), RLP.encode(value::writeTo));
      }
    } catch (final IllegalArgumentException e) {
      throw new Eip8347ArtifactVerificationException(e.getMessage(), e);
    }
    if (values.hasNext()) {
      throw new IllegalStateException("MPT value beyond the last preimage entry");
    }
    return accountTrie.rootHash();
  }

  private static byte[] nextValue(final Iterator<Entry> values, final long expectedOrdinal) {
    final Entry entry = values.hasNext() ? values.next() : null;
    if (entry == null || ByteBuffer.wrap(entry.key()).getLong() != expectedOrdinal) {
      throw new IllegalStateException("no matched value for preimage entry #" + expectedOrdinal);
    }
    return entry.value();
  }
}
