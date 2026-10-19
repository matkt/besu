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

import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.AscendingCollapseBinaryTrie;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader.CodeUnit;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader.HeaderUnit;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader.StorageUnit;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader.Unit;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.Leaf;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347Pipelines;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EIP-8347 dual check, in bounded memory: internal PBT consistency, then consensus anchoring
 * against an MPT {@code stateRoot}. {@link #verifyAndLoad} also writes the PBT nodes and the code
 * reference counts during the same read, but no cursor: on an exception the caller discards them.
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
        ForkJoinPool.commonPool(),
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
        Eip8347ExternalSorter.DEFAULT_BUFFER_BYTES,
        ForkJoinPool.commonPool());
  }

  /**
   * Same as {@link #verifyAndLoad(Path, Path, Bytes32, Path, NodeUpdater)} with an explicit in-heap
   * buffer per external sort, and the parallel stages on {@code pool}.
   */
  public static Loaded verifyAndLoad(
      final Path snapshotPath,
      final Path preimagesPath,
      final Bytes32 expectedMptStateRoot,
      final Path workDir,
      final NodeUpdater nodes,
      final long sortBufferBytes,
      final ForkJoinPool pool)
      throws IOException {
    return run(
        snapshotPath,
        preimagesPath,
        expectedMptStateRoot,
        workDir,
        sortBufferBytes,
        pool,
        Optional.of(Objects.requireNonNull(nodes, "nodes")));
  }

  private static Loaded run(
      final Path snapshotPath,
      final Path preimagesPath,
      final Bytes32 expectedMptStateRoot,
      final Path workDir,
      final long sortBufferBytes,
      final ForkJoinPool pool,
      final Optional<NodeUpdater> nodes)
      throws IOException {
    Objects.requireNonNull(snapshotPath, "snapshotPath");
    Objects.requireNonNull(preimagesPath, "preimagesPath");
    Objects.requireNonNull(expectedMptStateRoot, "expectedMptStateRoot");

    final Path sortDir =
        Files.createTempDirectory(Files.createDirectories(workDir), "eip8347-verify-");
    try (final Eip8347ExternalSorter preimageRequests =
            new Eip8347ExternalSorter(sortDir, "preimage-requests", sortBufferBytes, pool);
        final Eip8347ExternalSorter mptValues =
            new Eip8347ExternalSorter(sortDir, "mpt-values", sortBufferBytes, pool);
        final Eip8347ExternalSorter codeGroupRequests =
            new Eip8347ExternalSorter(sortDir, "code-group-requests", sortBufferBytes, pool);
        final Eip8347ExternalSorter codeGroupsByCode =
            new Eip8347ExternalSorter(sortDir, "code-groups-by-code", sortBufferBytes, pool)) {

      final long accounts =
          Eip8347AnchorJoin.sortPreimageRequests(preimagesPath, preimageRequests, pool);

      final Eip8347CodeCheck codeCheck =
          new Eip8347CodeCheck(codeGroupRequests, codeGroupsByCode, pool);
      final Eip8347AnchorJoin anchorJoin =
          new Eip8347AnchorJoin(preimagesPath, preimageRequests.sorted(), mptValues, codeCheck);
      final AscendingCollapseBinaryTrie pbt =
          nodes.map(AscendingCollapseBinaryTrie::new).orElseGet(AscendingCollapseBinaryTrie::new);
      final Bytes32 claimedRoot;
      final long leafCount;
      try (final Eip8347SnapshotReader snapshot = new Eip8347SnapshotReader(snapshotPath)) {
        // A unit is one whole stem, hashed in parallel; only its top node is attached in order.
        Eip8347Pipelines.run(
            Eip8347Pipelines.fromLists("eip8347-verify-snapshot", snapshot.units(), Unit::leafCount)
                .thenProcessAsyncOrdered(
                    "eip8347-verify-hash-stems",
                    Eip8347Pipelines.async(
                        pool,
                        units ->
                            units.stream()
                                .map(unit -> new HashedUnit(unit, prepare(pbt, unit)))
                                .toList()),
                    pool.getParallelism())
                .thenProcess("eip8347-verify-pbt", hashed -> attach(pbt, hashed))
                .andFinishWith(
                    "eip8347-verify-join",
                    units -> units.forEach(unit -> join(unit, anchorJoin, codeCheck))));
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

      final CompletableFuture<Long> codeHashes = new CompletableFuture<>();
      final Thread codeCheckThread =
          new Thread(
              () -> {
                try {
                  codeHashes.complete(codeCheck.finish(nodes));
                } catch (final Throwable t) {
                  // An Error too: the verifying thread waits for this future.
                  codeHashes.completeExceptionally(t);
                }
              },
              "eip8347-verify-code-check");
      codeCheckThread.start();
      try {
        final Bytes32 computedMptRoot =
            Eip8347MptRebuild.rootHash(preimagesPath, mptValues.sorted(), accounts, pool);
        if (!computedMptRoot.equals(expectedMptStateRoot)) {
          throw new Eip8347ArtifactVerificationException(
              "MPT stateRoot mismatch: computed "
                  + computedMptRoot.toHexString()
                  + " expected "
                  + expectedMptStateRoot.toHexString());
        }
        LOG.info("EIP-8347 consensus anchoring OK (stateRoot={})", computedMptRoot.toHexString());
        return new Loaded(computedPbtRoot, leafCount, Eip8347Pipelines.await(codeHashes));
      } catch (final IOException | RuntimeException e) {
        // Rejected or interrupted: stop the code check instead of waiting for all of it.
        codeCheckThread.interrupt();
        throw e;
      } finally {
        // Let the code check, which reads files under sortDir, end; its later failure is dropped.
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

  private static List<Unit> attach(
      final AscendingCollapseBinaryTrie pbt, final List<HashedUnit> hashed) {
    try {
      hashed.forEach(unit -> pbt.insert(unit.stem()));
    } catch (final IllegalArgumentException e) {
      throw new Eip8347ArtifactVerificationException(e.getMessage(), e);
    }
    return hashed.stream().map(HashedUnit::unit).toList();
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
}
