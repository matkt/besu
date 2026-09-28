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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.convert;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.BasicDataEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.CodeChunkifier;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.DelegationEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageRecord;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EIP-8347 converter (snapshot half): preimages + anchor MPT state → byte-canonical PBT snapshot.
 *
 * <p>Does <strong>not</strong> hold the full leaf set in heap. Leaves are spilled to sorted runs on
 * disk ({@link Eip8347LeafSpillSorter}), then k-way merged into the snapshot while the claimed
 * {@code pbtRoot} is hashed with {@code AscendingCollapseBinaryTrie}.
 *
 * <p>CODE_ZONE chunks are content-addressed by {@code code_hash}: only a {@link Set} of emitted
 * hashes is retained (not the chunk payloads).
 */
public final class Eip8347SnapshotGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(Eip8347SnapshotGenerator.class);

  private Eip8347SnapshotGenerator() {}

  /**
   * Result of a snapshot generation: claimed PBT root and number of leaf records written.
   *
   * @param pbtRoot claimed EIP-8297 root written into the artifact header
   * @param leafCount number of leaf records
   */
  public record Result(Bytes32 pbtRoot, long leafCount) {}

  /**
   * Generates a PBT snapshot from a preimage file and an anchor-state view.
   *
   * @param preimagesPath EIP-8347 preimage artifact (input)
   * @param stateSource account/storage/code view at {@code ANCHOR_BLOCK}
   * @param snapshotPath output path for the snapshot artifact
   * @return claimed root and leaf count
   */
  public static Result generate(
      final Path preimagesPath, final Eip8347StateSource stateSource, final Path snapshotPath)
      throws IOException {
    return generate(
        preimagesPath, stateSource, snapshotPath, Eip8347LeafSpillSorter.DEFAULT_RUN_CAPACITY);
  }

  /**
   * Same as {@link #generate(Path, Eip8347StateSource, Path)} with an explicit spill-run capacity
   * (useful in tests to force multi-run merges).
   */
  public static Result generate(
      final Path preimagesPath,
      final Eip8347StateSource stateSource,
      final Path snapshotPath,
      final int runCapacity)
      throws IOException {
    Objects.requireNonNull(preimagesPath, "preimagesPath");
    Objects.requireNonNull(stateSource, "stateSource");
    Objects.requireNonNull(snapshotPath, "snapshotPath");

    final Path absoluteSnapshot = snapshotPath.toAbsolutePath();
    final Path snapshotParent = absoluteSnapshot.getParent();
    if (snapshotParent != null) {
      Files.createDirectories(snapshotParent);
    }
    final Path workDir =
        Files.createTempDirectory(
            snapshotParent != null ? snapshotParent : Path.of(System.getProperty("java.io.tmpdir")),
            "eip8347-spill-");
    long accountCount = 0;
    try (final Eip8347LeafSpillSorter spill = new Eip8347LeafSpillSorter(workDir, runCapacity)) {
      final Set<Bytes32> emittedCodeHashes = new HashSet<>();
      try (final Eip8347PreimageReader preimages = new Eip8347PreimageReader(preimagesPath)) {
        final Iterator<Eip8347PreimageRecord> it = preimages.iterator();
        while (it.hasNext()) {
          emitAccount(spill, emittedCodeHashes, stateSource, it.next());
          accountCount++;
        }
        preimages.ensureExhausted();
      }
      final Result result = spill.finishToSnapshot(snapshotPath);
      LOG.info(
          "EIP-8347 snapshot generated (accounts={}, leaves={}, root={})",
          accountCount,
          result.leafCount(),
          result.pbtRoot().toHexString());
      return result;
    } finally {
      deleteRecursively(workDir);
    }
  }

  private static void emitAccount(
      final Eip8347LeafSpillSorter spill,
      final Set<Bytes32> emittedCodeHashes,
      final Eip8347StateSource stateSource,
      final Eip8347PreimageRecord record)
      throws IOException {
    final Address address = record.address();
    final Eip8347StateSource.AccountView account =
        stateSource
            .getAccount(address)
            .orElseThrow(
                () ->
                    new Eip8347ArtifactVerificationException(
                        "preimage address " + address + " has no account in anchor state"));

    final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
    final Bytes code = account.code() == null ? Bytes.EMPTY : account.code();

    if (Eip8347StateSource.isDelegationCode(code)) {
      spill.accept(
          TrieKeyDerivation.getTreeKeyForBasicData(address32),
          BasicDataEncoder.encodeBasicData(
              EmbeddingParameters.DELEGATION_CODE_SIZE,
              account.nonce(),
              account.balance().toUInt256()));
      spill.accept(
          TrieKeyDerivation.getTreeKeyForDelegation(address32),
          DelegationEncoder.encodeDelegation(
              CodeDelegationHelper.getTargetAddress(code).getBytes()));
    } else {
      final Hash codeHash = Eip8347StateSource.codeHashOf(code);
      spill.accept(
          TrieKeyDerivation.getTreeKeyForBasicData(address32),
          BasicDataEncoder.encodeBasicData(
              code.size(), account.nonce(), account.balance().toUInt256()));
      spill.accept(
          TrieKeyDerivation.getTreeKeyForCodeHash(address32), Bytes32.wrap(codeHash.getBytes()));
      if (!code.isEmpty()) {
        final Bytes32 codeHash32 = Bytes32.wrap(codeHash.getBytes());
        if (emittedCodeHashes.add(codeHash32)) {
          final List<Bytes32> chunks = CodeChunkifier.chunkifyCode(code);
          for (int i = 0; i < chunks.size(); i++) {
            if (!Bytes32.ZERO.equals(chunks.get(i))) {
              spill.accept(TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash32, i), chunks.get(i));
            }
          }
        }
      }
    }

    // Slots in the preimage record are keccak-ordered; emit storage leaves (PBT key order differs).
    final List<Bytes32> slotKeys = record.slotKeys();
    for (final Bytes32 slotKey : slotKeys) {
      final UInt256 slot = UInt256.fromBytes(slotKey);
      final UInt256 value = stateSource.getStorage(address, slotKey);
      if (UInt256.ZERO.equals(value)) {
        throw new Eip8347ArtifactVerificationException(
            "preimage slot "
                + slot.toHexString()
                + " for "
                + address
                + " has zero value in anchor state (MPT leaf must be absent)");
      }
      spill.accept(
          TrieKeyDerivation.getTreeKeyForStorageSlot(address32, slot), Bytes32.leftPad(value));
    }
  }

  private static void deleteRecursively(final Path dir) {
    if (dir == null || !Files.exists(dir)) {
      return;
    }
    try (final var walk = Files.walk(dir)) {
      walk.sorted(Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (final IOException e) {
                  LOG.debug("failed deleting spill path {}", p, e);
                }
              });
    } catch (final IOException e) {
      LOG.debug("failed walking spill dir {}", dir, e);
    }
  }
}
