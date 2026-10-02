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
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.BasicDataEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.CodeChunkifier;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.DelegationEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.AscendingCollapseBinaryTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotWriter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.Leaf;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347Pipelines;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.evm.worldstate.WorldState;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EIP-8347 converter (snapshot half): preimages + anchor MPT state → byte-canonical PBT snapshot.
 *
 * <p>Bounded memory end to end: preimages stream in batches of at most {@link #SLOTS_PER_BATCH}
 * slots, leaves go through an external sort ({@link Eip8347ExternalSorter}), and the sorted leaves
 * are written and hashed in one pass. Pipeline stages ({@code services:pipeline}):
 *
 * <pre>
 * preimage batches → read anchor state (1 thread) → derive PBT leaves (N threads) → sort
 * sorted leaves → snapshot writer + AscendingCollapseBinaryTrie
 * </pre>
 *
 * <p>State reads stay on one thread because {@link StateSource} is not required to be thread-safe.
 * CODE_ZONE chunks are content-addressed by {@code code_hash}; shared bytecode is spilled once per
 * referencing account and coalesced by the merge.
 */
public final class Eip8347SnapshotGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(Eip8347SnapshotGenerator.class);

  /** Maximum preimage slots carried by one pipeline item. */
  static final int SLOTS_PER_BATCH = 1024;

  /**
   * Read-only view of the anchor MPT state used by {@link Eip8347SnapshotGenerator}: nonce,
   * balance, code and storage per preimage address. Production uses {@link #of(WorldState)}; tests
   * stub it. Called from a single thread, so implementations need not be thread-safe.
   */
  @FunctionalInterface
  public interface StateSource {

    /**
     * Account fields needed to emit EIP-8297 leaves for one address.
     *
     * @param nonce account nonce
     * @param balance account balance
     * @param code account code (empty for EOAs; may be an EIP-7702 delegation indicator)
     */
    record AccountView(long nonce, Wei balance, Bytes code) {}

    /** Returns the account at {@code address}, or empty if absent from the anchor state. */
    Optional<AccountView> getAccount(Address address);

    /** Storage value at {@code slotKey} (32-byte big-endian slot), or zero if unset. */
    default UInt256 getStorage(final Address address, final Bytes32 slotKey) {
      return UInt256.ZERO;
    }

    /**
     * View over a Besu {@link WorldState}. The generator reads an account and then its slots, so
     * the last resolved account is kept to avoid one world-state lookup per slot.
     */
    static StateSource of(final WorldState worldState) {
      Objects.requireNonNull(worldState, "worldState");
      return new StateSource() {
        private Address cachedAddress;
        private Account cachedAccount;

        @Override
        public Optional<AccountView> getAccount(final Address address) {
          return Optional.ofNullable(resolve(address))
              .map(a -> new AccountView(a.getNonce(), a.getBalance(), a.getCode()));
        }

        @Override
        public UInt256 getStorage(final Address address, final Bytes32 slotKey) {
          final Account account = resolve(address);
          return account == null
              ? UInt256.ZERO
              : account.getStorageValue(UInt256.fromBytes(slotKey));
        }

        private Account resolve(final Address address) {
          if (!address.equals(cachedAddress)) {
            cachedAddress = address;
            cachedAccount = worldState.get(address);
          }
          return cachedAccount;
        }
      };
    }
  }

  private Eip8347SnapshotGenerator() {}

  /**
   * Result of a snapshot generation: claimed PBT root and number of leaf records written.
   *
   * @param pbtRoot claimed EIP-8297 root written into the artifact header
   * @param leafCount number of leaf records
   */
  public record Result(Bytes32 pbtRoot, long leafCount) {}

  /** Anchor-state values of one preimage batch, read in file order. */
  private record StateBatch(
      Address address,
      StateSource.AccountView account,
      List<Bytes32> slotKeys,
      List<UInt256> values) {}

  /**
   * Generates a PBT snapshot from a preimage file and an anchor-state view.
   *
   * @param preimagesPath EIP-8347 preimage artifact (input)
   * @param stateSource account/storage/code view at {@code ANCHOR_BLOCK}
   * @param snapshotPath output path for the snapshot artifact
   * @param workDir where the external sort spills; a subdirectory is created and deleted
   * @return claimed root and leaf count
   */
  public static Result generate(
      final Path preimagesPath,
      final StateSource stateSource,
      final Path snapshotPath,
      final Path workDir)
      throws IOException {
    return generate(
        preimagesPath,
        stateSource,
        snapshotPath,
        workDir,
        Eip8347ExternalSorter.DEFAULT_BUFFER_BYTES);
  }

  /**
   * Same as {@link #generate(Path, StateSource, Path, Path)} with an explicit in-heap sort buffer
   * (tests use a tiny one to force multi-run, multi-pass merges).
   */
  static Result generate(
      final Path preimagesPath,
      final StateSource stateSource,
      final Path snapshotPath,
      final Path workDir,
      final long sortBufferBytes)
      throws IOException {
    Objects.requireNonNull(preimagesPath, "preimagesPath");
    Objects.requireNonNull(stateSource, "stateSource");
    Objects.requireNonNull(snapshotPath, "snapshotPath");

    Files.createDirectories(snapshotPath.toAbsolutePath().getParent());
    final Path spillDir =
        Files.createTempDirectory(Files.createDirectories(workDir), "eip8347-convert-");
    try (final Eip8347ExternalSorter leaves =
            new Eip8347ExternalSorter(spillDir, "leaves", sortBufferBytes);
        final Eip8347PreimageFile preimages = new Eip8347PreimageFile(preimagesPath)) {
      Eip8347Pipelines.run(
          Eip8347Pipelines.from("eip8347-convert-preimages", preimages.batches(SLOTS_PER_BATCH))
              .thenProcess("eip8347-convert-read-state", batch -> readState(stateSource, batch))
              .thenProcessInParallel(
                  "eip8347-convert-derive-leaves",
                  Eip8347SnapshotGenerator::deriveLeaves,
                  Eip8347Pipelines.PARALLELISM)
              .andFinishWith(
                  "eip8347-convert-sort-leaves",
                  batch -> {
                    try {
                      for (final Leaf leaf : batch) {
                        leaves.add(leaf.key().toArray(), leaf.value().toArray());
                      }
                    } catch (final IOException e) {
                      throw new UncheckedIOException(e);
                    }
                  }));
      preimages.ensureExhausted();
      final Result result = writeSnapshot(leaves.sorted(), snapshotPath);
      LOG.info(
          "EIP-8347 snapshot generated (leaves={}, root={})",
          result.leafCount(),
          result.pbtRoot().toHexString());
      return result;
    } finally {
      Eip8347ExternalSorter.deleteRecursively(spillDir);
    }
  }

  /**
   * Writes sorted leaves and hashes the PBT root in the same pass. Identical duplicates (shared
   * bytecode emitted per account) are coalesced; a key with two values is rejected.
   */
  private static Result writeSnapshot(
      final Iterator<Eip8347ExternalSorter.Entry> sorted, final Path snapshotPath)
      throws IOException {
    final AscendingCollapseBinaryTrie pbt = new AscendingCollapseBinaryTrie();
    long leafCount = 0;
    try (final Eip8347SnapshotWriter snapshot = Eip8347SnapshotWriter.open(snapshotPath)) {
      Eip8347ExternalSorter.Entry previous = null;
      while (sorted.hasNext()) {
        final Eip8347ExternalSorter.Entry entry = sorted.next();
        if (previous != null && Arrays.equals(previous.key(), entry.key())) {
          if (!Arrays.equals(previous.value(), entry.value())) {
            throw new Eip8347ArtifactVerificationException(
                "duplicate PBT key with conflicting values: " + Bytes.wrap(entry.key()));
          }
          continue;
        }
        final Leaf leaf = new Leaf(Bytes.wrap(entry.key()), Bytes32.wrap(entry.value()));
        pbt.insert(leaf.key(), leaf.value());
        snapshot.accept(leaf);
        leafCount++;
        previous = entry;
      }
      final Bytes32 pbtRoot = pbt.rootHash();
      snapshot.finish(pbtRoot);
      return new Result(pbtRoot, leafCount);
    }
  }

  private static StateBatch readState(
      final StateSource stateSource, final Eip8347PreimageFile.Batch batch) {
    final Address address = batch.account().address();
    StateSource.AccountView account = null;
    if (batch.first()) {
      account =
          stateSource
              .getAccount(address)
              .orElseThrow(
                  () ->
                      new Eip8347ArtifactVerificationException(
                          "preimage address " + address + " has no account in anchor state"));
    }
    final List<Bytes32> slotKeys = new ArrayList<>(batch.slots().size());
    final List<UInt256> values = new ArrayList<>(batch.slots().size());
    for (final Eip8347PreimageFile.Slot slot : batch.slots()) {
      final UInt256 value = stateSource.getStorage(address, slot.key());
      if (value.isZero()) {
        throw new Eip8347ArtifactVerificationException(
            "preimage slot "
                + slot.key().toHexString()
                + " for "
                + address
                + " has zero value in anchor state (MPT leaf must be absent)");
      }
      slotKeys.add(slot.key());
      values.add(value);
    }
    return new StateBatch(address, account, slotKeys, values);
  }

  /** PBT leaves of one batch (unsorted; the spill sorts them). CPU-only, thread-safe. */
  private static List<Leaf> deriveLeaves(final StateBatch batch) {
    final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(batch.address().getBytes());
    final Bytes32 addressKeyHash = TrieKeyDerivation.keyHash(address32);
    final List<Leaf> leaves = new ArrayList<>();
    if (batch.account() != null) {
      addHeaderAndCodeLeaves(leaves, address32, batch.account());
    }
    for (int i = 0; i < batch.slotKeys().size(); i++) {
      leaves.add(
          new Leaf(
              TrieKeyDerivation.getTreeKeyForStorageSlot(
                  address32, addressKeyHash, UInt256.fromBytes(batch.slotKeys().get(i))),
              Bytes32.leftPad(batch.values().get(i))));
    }
    return leaves;
  }

  private static void addHeaderAndCodeLeaves(
      final List<Leaf> leaves, final Bytes32 address32, final StateSource.AccountView account) {
    final Bytes code = account.code();
    final UInt256 balance = account.balance().toUInt256();
    if (CodeDelegationHelper.hasCodeDelegation(code)) {
      leaves.add(
          new Leaf(
              TrieKeyDerivation.getTreeKeyForBasicData(address32),
              BasicDataEncoder.encodeBasicData(
                  EmbeddingParameters.DELEGATION_CODE_SIZE, account.nonce(), balance)));
      leaves.add(
          new Leaf(
              TrieKeyDerivation.getTreeKeyForDelegation(address32),
              DelegationEncoder.encodeDelegation(
                  CodeDelegationHelper.getTargetAddress(code).getBytes())));
      return;
    }
    final Bytes32 codeHash = Bytes32.wrap(Hash.hash(code).getBytes());
    leaves.add(
        new Leaf(
            TrieKeyDerivation.getTreeKeyForBasicData(address32),
            BasicDataEncoder.encodeBasicData(code.size(), account.nonce(), balance)));
    leaves.add(new Leaf(TrieKeyDerivation.getTreeKeyForCodeHash(address32), codeHash));
    final List<Bytes32> chunks = CodeChunkifier.chunkifyCode(code);
    for (int i = 0; i < chunks.size(); i++) {
      if (!chunks.get(i).isZero()) {
        leaves.add(new Leaf(TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash, i), chunks.get(i)));
      }
    }
  }
}
