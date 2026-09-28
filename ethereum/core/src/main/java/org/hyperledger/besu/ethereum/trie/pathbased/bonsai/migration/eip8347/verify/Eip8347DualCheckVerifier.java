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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.BasicDataEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.CodeChunkifier;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.DelegationEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieConstants;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.AscendingCollapseBinaryTrie;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageRecord;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader;
import org.hyperledger.besu.ethereum.trie.patricia.AscendingCollapsePatriciaTrie;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EIP-8347 dual-check verifier: internal PBT consistency plus consensus anchoring against an MPT
 * {@code stateRoot}.
 *
 * <p>Phase-1 streams the snapshot once into {@link AscendingCollapseBinaryTrie} and a stem→offset
 * index (no full sort, no leaf spill). Phase-2 walks preimages in keccak order into {@link
 * AscendingCollapsePatriciaTrie} (account + storage). Both ascending builders parallelize hashing
 * of the root branch's children only.
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
    Objects.requireNonNull(snapshotPath, "snapshotPath");
    Objects.requireNonNull(preimagesPath, "preimagesPath");
    Objects.requireNonNull(expectedMptStateRoot, "expectedMptStateRoot");

    final AscendingCollapseBinaryTrie pbt = new AscendingCollapseBinaryTrie();
    try (final Eip8347SnapshotReader snapshot = new Eip8347SnapshotReader(snapshotPath);
        final Eip8347SnapshotLeafIndex leaves = new Eip8347SnapshotLeafIndex(snapshotPath)) {

      snapshot.forEach(
          leaf -> {
            try {
              pbt.insert(leaf.key(), leaf.value());
            } catch (final IllegalArgumentException e) {
              throw new Eip8347ArtifactVerificationException(e.getMessage(), e);
            }
            leaves.record(leaf.key(), snapshot.lastLeafOffset());
          });
      snapshot.ensureExhausted();

      if (pbt.insertCount() != snapshot.leafCount()) {
        throw new Eip8347ArtifactVerificationException(
            "PBT leaf count "
                + pbt.insertCount()
                + " disagrees with snapshot header "
                + snapshot.leafCount());
      }

      final Bytes32 computedPbtRoot =
          snapshot.leafCount() == 0 ? TrieConstants.EMPTY_TRIE_ROOT : pbt.rootHash();
      if (!computedPbtRoot.equals(snapshot.claimedRoot())) {
        throw new Eip8347ArtifactVerificationException(
            "PBT root mismatch: computed "
                + computedPbtRoot.toHexString()
                + " claimed "
                + snapshot.claimedRoot().toHexString());
      }
      LOG.info(
          "EIP-8347 internal PBT consistency OK (leaves={}, stems={}, root={})",
          snapshot.leafCount(),
          leaves.stemCount(),
          computedPbtRoot.toHexString());

      leaves.seal();
      anchorToMpt(preimagesPath, leaves, expectedMptStateRoot);
    }
  }

  /** Preimages in keccak order: stem-table lookup + seek + ascending Patricia insert. */
  private static void anchorToMpt(
      final Path preimagesPath,
      final Eip8347SnapshotLeafIndex leaves,
      final Bytes32 expectedMptStateRoot)
      throws IOException {
    final AscendingCollapsePatriciaTrie accountTrie = new AscendingCollapsePatriciaTrie();
    long accountCount = 0;
    try (final Eip8347PreimageReader preimages = new Eip8347PreimageReader(preimagesPath)) {
      final Iterator<Eip8347PreimageRecord> it = preimages.iterator();
      while (it.hasNext()) {
        final Eip8347PreimageRecord record = it.next();
        final Bytes accountRlp = buildAccountRlp(record, leaves);
        try {
          accountTrie.insert(record.addressHash().getBytes(), accountRlp);
        } catch (final IllegalArgumentException e) {
          throw new Eip8347ArtifactVerificationException(e.getMessage(), e);
        }
        accountCount++;
      }
      preimages.ensureExhausted();
    }

    final Bytes32 computedMptRoot = accountTrie.rootHash();
    if (!computedMptRoot.equals(expectedMptStateRoot)) {
      throw new Eip8347ArtifactVerificationException(
          "MPT stateRoot mismatch: computed "
              + computedMptRoot.toHexString()
              + " expected "
              + expectedMptStateRoot.toHexString());
    }

    leaves.ensureAllConsumed();
    LOG.info(
        "EIP-8347 consensus anchoring OK (accounts={}, stateRoot={})",
        accountCount,
        computedMptRoot.toHexString());
  }

  private static Bytes buildAccountRlp(
      final Eip8347PreimageRecord record, final Eip8347SnapshotLeafIndex leaves)
      throws IOException {
    final Address address = record.address();
    final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
    final Bytes accountStem =
        Eip8347SnapshotLeafIndex.stemOf(TrieKeyDerivation.getTreeKeyForBasicData(address32));
    final Map<Integer, Bytes32> stemLeaves = leaves.requireStem(accountStem);
    for (final Integer sub : stemLeaves.keySet()) {
      if (sub > EmbeddingParameters.DELEGATION_LEAF_KEY
          && sub < EmbeddingParameters.HEADER_STORAGE_OFFSET) {
        throw new Eip8347ArtifactVerificationException(
            "account " + address + " has reserved header sub-index " + sub);
      }
    }

    final Bytes32 basicDataValue =
        requireSub(stemLeaves, EmbeddingParameters.BASIC_DATA_LEAF_KEY, address);
    final BasicDataEncoder.BasicData basicData;
    try {
      basicData = BasicDataEncoder.decodeBasicData(basicDataValue);
    } catch (final IllegalArgumentException e) {
      throw new Eip8347ArtifactVerificationException(e.getMessage(), e);
    }

    final Bytes32 codeHashLeaf = stemLeaves.get(EmbeddingParameters.CODE_HASH_LEAF_KEY);
    final Bytes32 delegationLeaf = stemLeaves.get(EmbeddingParameters.DELEGATION_LEAF_KEY);

    final Hash codeHash;
    if (delegationLeaf != null) {
      if (codeHashLeaf != null) {
        throw new Eip8347ArtifactVerificationException(
            "account " + address + " has both code_hash and delegation leaves");
      }
      if (basicData.codeSize() != EmbeddingParameters.DELEGATION_CODE_SIZE) {
        throw new Eip8347ArtifactVerificationException(
            "delegated account " + address + " must have code_size 23");
      }
      codeHash = Hash.hash(decodeDelegationLeaf(address, delegationLeaf));
    } else if (codeHashLeaf != null) {
      codeHash = Hash.wrap(codeHashLeaf);
      if (basicData.codeSize() == 0) {
        if (!codeHash.equals(Hash.EMPTY)) {
          throw new Eip8347ArtifactVerificationException(
              "codeless account " + address + " must use empty code hash");
        }
      } else {
        verifyOneCode(leaves, Bytes32.wrap(codeHash.getBytes()), basicData.codeSize());
      }
    } else {
      throw new Eip8347ArtifactVerificationException(
          "account " + address + " missing code_hash and delegation leaves");
    }

    final Hash storageRoot = buildStorageRoot(address32, record, leaves, stemLeaves, accountStem);
    final PatriciaTrieAccountValue accountValue =
        new PatriciaTrieAccountValue(
            basicData.nonce(), Wei.wrap(basicData.balance()), storageRoot, codeHash);
    return RLP.encode(accountValue::writeTo);
  }

  private static Bytes32 requireSub(
      final Map<Integer, Bytes32> stemLeaves, final int sub, final Address address) {
    final Bytes32 value = stemLeaves.get(sub);
    if (value == null) {
      throw new Eip8347ArtifactVerificationException(
          "account " + address + " missing header sub-index " + sub);
    }
    return value;
  }

  private static Bytes decodeDelegationLeaf(final Address address, final Bytes32 delegationValue) {
    final Bytes designatorAndTarget =
        delegationValue.slice(0, CodeDelegationHelper.DELEGATED_CODE_SIZE);
    if (!CodeDelegationHelper.hasCodeDelegation(designatorAndTarget)) {
      throw new Eip8347ArtifactVerificationException(
          "delegation leaf for " + address + " missing 0xef0100 designator");
    }
    final Address target = CodeDelegationHelper.getTargetAddress(designatorAndTarget);
    final Bytes32 expected = DelegationEncoder.encodeDelegation(target.getBytes());
    if (!expected.equals(delegationValue)) {
      throw new Eip8347ArtifactVerificationException(
          "delegation leaf for " + address + " does not match DelegationEncoder layout");
    }
    return designatorAndTarget;
  }

  private static Hash buildStorageRoot(
      final Bytes32 address32,
      final Eip8347PreimageRecord record,
      final Eip8347SnapshotLeafIndex leaves,
      final Map<Integer, Bytes32> accountStemLeaves,
      final Bytes accountStem)
      throws IOException {
    final List<Bytes32> slotKeys = record.slotKeys();
    if (slotKeys.isEmpty()) {
      return Hash.EMPTY_TRIE_HASH;
    }
    final List<Hash> slotHashes = record.slotKeyHashes();
    final AscendingCollapsePatriciaTrie storageTrie = new AscendingCollapsePatriciaTrie();

    for (int i = 0; i < slotKeys.size(); i++) {
      final Bytes32 slotKey = slotKeys.get(i);
      final UInt256 slot = UInt256.fromBytes(slotKey);
      final Bytes treeKey = TrieKeyDerivation.getTreeKeyForStorageSlot(address32, slot);
      final Bytes32 value;
      if (Eip8347SnapshotLeafIndex.stemOf(treeKey).equals(accountStem)) {
        final int sub = treeKey.get(treeKey.size() - 1) & 0xFF;
        value = accountStemLeaves.get(sub);
        if (value == null) {
          throw new Eip8347ArtifactVerificationException(
              "preimage slot "
                  + slot.toHexString()
                  + " for "
                  + record.address()
                  + " has no snapshot leaf");
        }
      } else {
        value =
            leaves
                .get(treeKey)
                .orElseThrow(
                    () ->
                        new Eip8347ArtifactVerificationException(
                            "preimage slot "
                                + slot.toHexString()
                                + " for "
                                + record.address()
                                + " has no snapshot leaf"));
      }
      try {
        storageTrie.insert(slotHashes.get(i).getBytes(), encodeStorageValue(value));
      } catch (final IllegalArgumentException e) {
        throw new Eip8347ArtifactVerificationException(e.getMessage(), e);
      }
    }
    return Hash.wrap(storageTrie.rootHash());
  }

  private static Bytes encodeStorageValue(final Bytes32 value) {
    final Bytes trimmed = value.trimLeadingZeros();
    return RLP.encode(out -> out.writeBytes(trimmed.isEmpty() ? Bytes.EMPTY : trimmed));
  }

  private static void verifyOneCode(
      final Eip8347SnapshotLeafIndex leaves, final Bytes32 codeHash, final long codeSize)
      throws IOException {
    if (codeSize <= 0) {
      throw new Eip8347ArtifactVerificationException("code_size must be positive for " + codeHash);
    }
    final int chunkCount = (int) ((codeSize + 30) / 31);
    final MutableBytes reassembled = MutableBytes.create((int) codeSize);
    for (int i = 0; i < chunkCount; i++) {
      final Bytes chunkKey = TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash, i);
      final Bytes32 chunk = leaves.get(chunkKey).orElse(Bytes32.ZERO);
      final int copy = (int) Math.min(31, codeSize - (long) i * 31);
      reassembled.set(i * 31, chunk.slice(1, copy));
    }
    final Bytes code = reassembled.copy();
    if (!Bytes32.wrap(Hash.hash(code).getBytes()).equals(codeHash)) {
      throw new Eip8347ArtifactVerificationException(
          "reassembled code hash mismatch for claimed " + codeHash.toHexString());
    }
    final List<Bytes32> expectedChunks = CodeChunkifier.chunkifyCode(code);
    for (int i = 0; i < expectedChunks.size(); i++) {
      final Bytes32 expected = expectedChunks.get(i);
      final Bytes chunkKey = TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash, i);
      if (Bytes32.ZERO.equals(expected)) {
        if (leaves.get(chunkKey).isPresent()) {
          throw new Eip8347ArtifactVerificationException(
              "zero code chunk " + i + " must be absent for " + codeHash.toHexString());
        }
      } else {
        final Bytes32 actual = leaves.require(chunkKey);
        if (!actual.equals(expected)) {
          throw new Eip8347ArtifactVerificationException(
              "code chunk " + i + " mismatch for " + codeHash.toHexString());
        }
      }
    }
  }
}
