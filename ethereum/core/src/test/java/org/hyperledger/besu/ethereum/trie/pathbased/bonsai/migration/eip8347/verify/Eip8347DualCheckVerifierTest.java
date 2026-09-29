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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.StoredPartitionedBinaryTrie;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile.AccountPreimages;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotLeaf;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotWriter;
import org.hyperledger.besu.ethereum.trie.patricia.AscendingCollapsePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Eip8347DualCheckVerifierTest {

  private static final NodeLoader EMPTY_LOADER = (location, hash) -> Optional.empty();

  @TempDir Path tmp;

  @Test
  void emptyArtifactsMatchEmptyMptRoot() throws Exception {
    final Path snapshot = tmp.resolve("empty.snap");
    final Path preimages = tmp.resolve("empty.pre");
    Eip8347SnapshotWriter.write(snapshot, TrieConstants.EMPTY_TRIE_ROOT, List.of());
    Eip8347PreimageFile.write(preimages, List.of());

    Eip8347DualCheckVerifier.verify(
        snapshot, preimages, Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes()));
  }

  @Test
  void dualCheckAcceptsEoaWithStorageAndContract() throws Exception {
    final Fixture fixture =
        Fixture.builder()
            .eoa(
                Address.fromHexString("0x00000000000000000000000000000000000000aa"),
                1L,
                Wei.of(1000),
                Map.of(UInt256.ZERO, UInt256.valueOf(7), UInt256.valueOf(64), UInt256.valueOf(9)))
            .contract(
                Address.fromHexString("0x00000000000000000000000000000000000000bb"),
                0L,
                Wei.of(1),
                Bytes.fromHexString("0x6001600055"),
                Map.of())
            .build();

    final Path snapshot = tmp.resolve("ok.snap");
    final Path preimages = tmp.resolve("ok.pre");
    Eip8347SnapshotWriter.write(snapshot, fixture.pbtRoot(), fixture.leaves());
    Eip8347PreimageFile.write(preimages, fixture.preimages());

    Eip8347DualCheckVerifier.verify(snapshot, preimages, fixture.mptRoot());
  }

  @Test
  void dualCheckAcceptsDelegationAccount() throws Exception {
    final Address target = Address.fromHexString("0x00000000000000000000000000000000000000cc");
    final Bytes delegationCode =
        Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, target.getBytes());
    final Fixture fixture =
        Fixture.builder()
            .delegation(
                Address.fromHexString("0x00000000000000000000000000000000000000dd"),
                1L,
                Wei.of(42),
                delegationCode)
            .build();

    final Path snapshot = tmp.resolve("deleg.snap");
    final Path preimages = tmp.resolve("deleg.pre");
    Eip8347SnapshotWriter.write(snapshot, fixture.pbtRoot(), fixture.leaves());
    Eip8347PreimageFile.write(preimages, fixture.preimages());

    Eip8347DualCheckVerifier.verify(snapshot, preimages, fixture.mptRoot());
  }

  @Test
  void dualCheckAcceptsSharedBytecodeAcrossAccounts() throws Exception {
    final Bytes shared = Bytes.fromHexString("0x6001600155");
    final Fixture fixture =
        Fixture.builder()
            .contract(
                Address.fromHexString("0x00000000000000000000000000000000000000a1"),
                0L,
                Wei.of(1),
                shared,
                Map.of())
            .contract(
                Address.fromHexString("0x00000000000000000000000000000000000000a2"),
                1L,
                Wei.of(2),
                shared,
                Map.of())
            .build();

    final Path snapshot = tmp.resolve("shared.snap");
    final Path preimages = tmp.resolve("shared.pre");
    Eip8347SnapshotWriter.write(snapshot, fixture.pbtRoot(), fixture.leaves());
    Eip8347PreimageFile.write(preimages, fixture.preimages());

    Eip8347DualCheckVerifier.verify(snapshot, preimages, fixture.mptRoot());
  }

  @Test
  void dualCheckAcceptsMixedAccountsWithStorageCodeAndDelegation() throws Exception {
    final Bytes sharedCode = Bytes.fromHexString("0x6001600155");
    final Address target = Address.fromHexString("0x00000000000000000000000000000000000000ce");
    final Bytes delegationCode =
        Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, target.getBytes());
    final Fixture fixture =
        Fixture.builder()
            .eoa(
                Address.fromHexString("0x00000000000000000000000000000000000000b1"),
                2L,
                Wei.of(100),
                Map.of(UInt256.ZERO, UInt256.valueOf(1), UInt256.valueOf(100), UInt256.valueOf(2)))
            .contract(
                Address.fromHexString("0x00000000000000000000000000000000000000b2"),
                0L,
                Wei.of(3),
                sharedCode,
                Map.of(UInt256.valueOf(7), UInt256.valueOf(8)))
            .delegation(
                Address.fromHexString("0x00000000000000000000000000000000000000b3"),
                5L,
                Wei.of(9),
                delegationCode)
            .build();

    final Path snapshot = tmp.resolve("mixed.snap");
    final Path preimages = tmp.resolve("mixed.pre");
    Eip8347SnapshotWriter.write(snapshot, fixture.pbtRoot(), fixture.leaves());
    Eip8347PreimageFile.write(preimages, fixture.preimages());

    Eip8347DualCheckVerifier.verify(snapshot, preimages, fixture.mptRoot());
  }

  @Test
  void rejectsWrongClaimedPbtRoot() throws Exception {
    final Fixture fixture =
        Fixture.builder()
            .eoa(
                Address.fromHexString("0x00000000000000000000000000000000000000ee"),
                0L,
                Wei.ONE,
                Map.of())
            .build();
    final Path snapshot = tmp.resolve("badroot.snap");
    final Path preimages = tmp.resolve("badroot.pre");
    Eip8347SnapshotWriter.write(snapshot, Bytes32.ZERO, fixture.leaves());
    Eip8347PreimageFile.write(preimages, fixture.preimages());

    assertThatThrownBy(
            () -> Eip8347DualCheckVerifier.verify(snapshot, preimages, fixture.mptRoot()))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("PBT root mismatch");
  }

  @Test
  void rejectsWrongAnchorStateRoot() throws Exception {
    final Fixture fixture =
        Fixture.builder()
            .eoa(
                Address.fromHexString("0x00000000000000000000000000000000000000ff"),
                0L,
                Wei.ONE,
                Map.of())
            .build();
    final Path snapshot = tmp.resolve("badmpt.snap");
    final Path preimages = tmp.resolve("badmpt.pre");
    Eip8347SnapshotWriter.write(snapshot, fixture.pbtRoot(), fixture.leaves());
    Eip8347PreimageFile.write(preimages, fixture.preimages());

    assertThatThrownBy(
            () ->
                Eip8347DualCheckVerifier.verify(
                    snapshot, preimages, Bytes32.fromHexString("0x" + "11".repeat(32))))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("MPT stateRoot mismatch");
  }

  @Test
  void rejectsWrongHeaderCountInSnapshot() throws Exception {
    final Fixture fixture =
        Fixture.builder()
            .eoa(
                Address.fromHexString("0x0000000000000000000000000000000000000010"),
                0L,
                Wei.ONE,
                Map.of())
            .build();
    final Path snapshot = tmp.resolve("badcount.snap");
    Eip8347SnapshotWriter.write(snapshot, fixture.pbtRoot(), fixture.leaves());
    // Corrupt headerCount (bytes 32..40) to headerCount+1.
    final byte[] bytes = Files.readAllBytes(snapshot);
    final long headerCount = ByteBuffer.wrap(bytes, 32, 8).order(ByteOrder.BIG_ENDIAN).getLong();
    ByteBuffer.wrap(bytes, 32, 8).order(ByteOrder.BIG_ENDIAN).putLong(headerCount + 1L);
    Files.write(snapshot, bytes);
    final Path preimages = tmp.resolve("badcount.pre");
    Eip8347PreimageFile.write(preimages, fixture.preimages());

    assertThatThrownBy(
            () -> Eip8347DualCheckVerifier.verify(snapshot, preimages, fixture.mptRoot()))
        .isInstanceOf(Eip8347ArtifactVerificationException.class);
  }

  @Test
  void rejectsUnsortedHeaderRecords() throws Exception {
    final Address a = Address.fromHexString("0x0000000000000000000000000000000000000011");
    final Address b = Address.fromHexString("0x0000000000000000000000000000000000000022");
    final Fixture fixture =
        Fixture.builder().eoa(a, 0L, Wei.ONE, Map.of()).eoa(b, 0L, Wei.ONE, Map.of()).build();

    final Path snapshot = tmp.resolve("unsorted.snap");
    // Build two single-account snapshots and concatenate their header records in reverse
    // addressHash order so the typed section is not strictly ascending.
    final Fixture onlyA = Fixture.builder().eoa(a, 0L, Wei.ONE, Map.of()).build();
    final Fixture onlyB = Fixture.builder().eoa(b, 0L, Wei.ONE, Map.of()).build();
    final Path snapA = tmp.resolve("a.snap");
    final Path snapB = tmp.resolve("b.snap");
    Eip8347SnapshotWriter.write(snapA, onlyA.pbtRoot(), onlyA.leaves());
    Eip8347SnapshotWriter.write(snapB, onlyB.pbtRoot(), onlyB.leaves());
    final byte[] aBytes = Files.readAllBytes(snapA);
    final byte[] bBytes = Files.readAllBytes(snapB);
    // Extract header record payload (after pbtRoot+headerCount, before codeCount).
    final byte[] headerA = extractFirstHeaderRecord(aBytes);
    final byte[] headerB = extractFirstHeaderRecord(bBytes);
    // Order by addressHash descending if needed.
    final Bytes32 hashA =
        TrieKeyDerivation.keyHash(TrieKeyDerivation.address20ToAddress32(a.getBytes()));
    final Bytes32 hashB =
        TrieKeyDerivation.keyHash(TrieKeyDerivation.address20ToAddress32(b.getBytes()));
    final byte[] first = hashA.compareTo(hashB) < 0 ? headerB : headerA;
    final byte[] second = hashA.compareTo(hashB) < 0 ? headerA : headerB;
    try (final OutputStream out = Files.newOutputStream(snapshot)) {
      out.write(fixture.pbtRoot().toArray());
      out.write(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(2L).array());
      out.write(first);
      out.write(second);
      out.write(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(0L).array()); // code
      out.write(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(0L).array()); // storage
    }
    final Path preimages = tmp.resolve("unsorted.pre");
    Eip8347PreimageFile.write(preimages, fixture.preimages());

    assertThatThrownBy(
            () -> Eip8347DualCheckVerifier.verify(snapshot, preimages, fixture.mptRoot()))
        // Reader order check and anchoring join run concurrently; either may report first.
        .isInstanceOf(Eip8347ArtifactVerificationException.class);
  }

  @Test
  void rejectsSurplusHeaderSlotWithoutPreimage() throws Exception {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000031");
    // Snapshot carries header slots 0 and 1; the preimage and MPT only know slot 0.
    final Fixture withSurplus =
        Fixture.builder()
            .eoa(
                address,
                1L,
                Wei.ONE,
                Map.of(UInt256.ZERO, UInt256.valueOf(7), UInt256.ONE, UInt256.valueOf(5)))
            .build();
    final Fixture anchored =
        Fixture.builder()
            .eoa(address, 1L, Wei.ONE, Map.of(UInt256.ZERO, UInt256.valueOf(7)))
            .build();

    final Path snapshot = tmp.resolve("surplus-slot.snap");
    final Path preimages = tmp.resolve("surplus-slot.pre");
    Eip8347SnapshotWriter.write(snapshot, withSurplus.pbtRoot(), withSurplus.leaves());
    Eip8347PreimageFile.write(preimages, anchored.preimages());

    assertThatThrownBy(
            () -> Eip8347DualCheckVerifier.verify(snapshot, preimages, anchored.mptRoot()))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("not covered by consensus anchoring");
  }

  @Test
  void rejectsStorageRecordSplitAcrossTwoRecords() throws Exception {
    // Slots 64 and 1000 land in two storage groups of one account: one record, groupCount 2.
    final Fixture fixture =
        Fixture.builder()
            .eoa(
                Address.fromHexString("0x0000000000000000000000000000000000000032"),
                1L,
                Wei.ONE,
                Map.of(UInt256.valueOf(64), UInt256.valueOf(9), UInt256.valueOf(1000), UInt256.ONE))
            .build();
    final Path canonical = tmp.resolve("split-canonical.snap");
    Eip8347SnapshotWriter.write(canonical, fixture.pbtRoot(), fixture.leaves());
    final byte[] bytes = Files.readAllBytes(canonical);

    // Tail: storageCount[8]=1 | addressHash[32] | groupCount=0x01 0x02 | group(36) | group(36)
    final int groupSize = 36;
    final int recordStart = bytes.length - (32 + 2 + 2 * groupSize);
    final int countStart = recordStart - 8;
    final byte[] addressHash = java.util.Arrays.copyOfRange(bytes, recordStart, recordStart + 32);
    final int groupsStart = recordStart + 34;
    final Path snapshot = tmp.resolve("split.snap");
    try (final OutputStream out = Files.newOutputStream(snapshot)) {
      out.write(bytes, 0, countStart);
      out.write(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(2L).array());
      for (int g = 0; g < 2; g++) {
        out.write(addressHash);
        out.write(new byte[] {1, 1});
        out.write(bytes, groupsStart + g * groupSize, groupSize);
      }
    }
    final Path preimages = tmp.resolve("split.pre");
    Eip8347PreimageFile.write(preimages, fixture.preimages());

    assertThatThrownBy(
            () -> Eip8347DualCheckVerifier.verify(snapshot, preimages, fixture.mptRoot()))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("storage records");
  }

  @Test
  void dualCheckWithTinySortBufferUsesMultiPassMerges() throws Exception {
    final Map<UInt256, UInt256> storage = new HashMap<>();
    for (int slot = 0; slot < 150; slot++) {
      storage.put(UInt256.valueOf(slot * 7L), UInt256.valueOf(slot + 1L));
    }
    // 9000 bytes of code: 291 chunks, i.e. two CODE_ZONE stem groups.
    final Bytes code = Bytes.repeat((byte) 0x5b, 9000);
    final Fixture fixture =
        Fixture.builder()
            .contract(
                Address.fromHexString("0x0000000000000000000000000000000000000033"),
                3L,
                Wei.of(10),
                code,
                storage)
            .contract(
                Address.fromHexString("0x0000000000000000000000000000000000000034"),
                0L,
                Wei.ONE,
                code,
                Map.of())
            .build();
    final Path snapshot = tmp.resolve("multipass.snap");
    final Path preimages = tmp.resolve("multipass.pre");
    Eip8347SnapshotWriter.write(snapshot, fixture.pbtRoot(), fixture.leaves());
    Eip8347PreimageFile.write(preimages, fixture.preimages());

    // One record per run: every sort goes through more than MAX_FAN_IN runs.
    Eip8347DualCheckVerifier.verify(snapshot, preimages, fixture.mptRoot(), 1L);
    try (final var left = Files.list(tmp)) {
      assertThat(left.map(p -> p.getFileName().toString()))
          .noneMatch(n -> n.startsWith("eip8347-"));
    }
  }

  /** Extracts the single header record from a one-account typed snapshot. */
  private static byte[] extractFirstHeaderRecord(final byte[] snapshot) {
    // pbtRoot[32] | headerCount[8]=1 | headerRecord | codeCount[8]=0 | storageCount[8]=0
    final int start = 40;
    final int end = snapshot.length - 16;
    final byte[] record = new byte[end - start];
    System.arraycopy(snapshot, start, record, 0, record.length);
    return record;
  }

  @Test
  void rejectsZeroValuedLeaf() {
    final Bytes key =
        TrieKeyDerivation.getTreeKeyForBasicData(
            TrieKeyDerivation.address20ToAddress32(
                Address.fromHexString("0x0000000000000000000000000000000000000033").getBytes()));
    assertThatThrownBy(() -> new Eip8347SnapshotLeaf(key, Bytes32.ZERO))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("zero-valued");
  }

  @Test
  void rejectsInvalidZoneKey() {
    final Bytes badKey = Bytes.fromHexString("0x02" + "00".repeat(33));
    assertThatThrownBy(() -> new Eip8347SnapshotLeaf(badKey, Bytes32.leftPad(Bytes.of(1))))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("reserved zone");
  }

  @Test
  void rejectsWrongZoneKeyLength() {
    final Bytes shortAccountKey =
        Bytes.concatenate(Bytes.of(EmbeddingParameters.ACCOUNT_ZONE), Bytes.repeat((byte) 0, 10));
    assertThatThrownBy(() -> new Eip8347SnapshotLeaf(shortAccountKey, Bytes32.leftPad(Bytes.of(1))))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("key length");
  }

  @Test
  void rejectsUnsortedPreimageRecords() throws Exception {
    final Address a = Address.fromHexString("0x0000000000000000000000000000000000000061");
    final Address b = Address.fromHexString("0x0000000000000000000000000000000000000062");
    final Fixture fixture =
        Fixture.builder().eoa(a, 0L, Wei.ONE, Map.of()).eoa(b, 0L, Wei.ONE, Map.of()).build();
    final List<AccountPreimages> reversed = new ArrayList<>(fixture.preimages());
    reversed.sort(Comparator.comparing(AccountPreimages::addressHash).reversed());

    final Path snapshot = tmp.resolve("unsorted-pre.snap");
    final Path preimages = tmp.resolve("unsorted-pre.pre");
    Eip8347SnapshotWriter.write(snapshot, fixture.pbtRoot(), fixture.leaves());
    writePreimagesInGivenOrder(preimages, reversed);

    assertThatThrownBy(
            () -> Eip8347DualCheckVerifier.verify(snapshot, preimages, fixture.mptRoot()))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("ascending by keccak256(address)");
  }

  @Test
  void rejectsUnsortedPreimageSlots() throws Exception {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000063");
    final Bytes32 slotLow = Bytes32.leftPad(Bytes.of(1));
    final Bytes32 slotHigh = Bytes32.leftPad(Bytes.of(2));
    final List<Bytes32> slots = new ArrayList<>(List.of(slotLow, slotHigh));
    slots.sort(Comparator.comparing(Hash::hash).reversed());
    final Path preimages = tmp.resolve("unsorted-slots.pre");
    writePreimagesInGivenOrder(preimages, List.of(new AccountPreimages(address, slots)));

    assertThatThrownBy(
            () -> {
              try (final Eip8347PreimageFile reader = new Eip8347PreimageFile(preimages)) {
                reader.forEach(r -> {});
              }
            })
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("ascending by keccak256(slotKey)");
  }

  @Test
  void rejectsSurplusPreimageAccount() throws Exception {
    final Address present = Address.fromHexString("0x0000000000000000000000000000000000000071");
    final Address surplus = Address.fromHexString("0x0000000000000000000000000000000000000072");
    final Fixture fixture = Fixture.builder().eoa(present, 0L, Wei.ONE, Map.of()).build();
    final List<AccountPreimages> withSurplus = new ArrayList<>(fixture.preimages());
    withSurplus.add(new AccountPreimages(surplus, List.of()));

    final Path snapshot = tmp.resolve("surplus.snap");
    final Path preimages = tmp.resolve("surplus.pre");
    Eip8347SnapshotWriter.write(snapshot, fixture.pbtRoot(), fixture.leaves());
    Eip8347PreimageFile.write(preimages, withSurplus);

    assertThatThrownBy(
            () -> Eip8347DualCheckVerifier.verify(snapshot, preimages, fixture.mptRoot()))
        .isInstanceOf(Eip8347ArtifactVerificationException.class);
  }

  @Test
  void rejectsMissingPreimageForSnapshotAccount() throws Exception {
    final Fixture fixture =
        Fixture.builder()
            .eoa(
                Address.fromHexString("0x0000000000000000000000000000000000000073"),
                0L,
                Wei.ONE,
                Map.of())
            .build();
    final Path snapshot = tmp.resolve("missing-pre.snap");
    final Path preimages = tmp.resolve("missing-pre.pre");
    Eip8347SnapshotWriter.write(snapshot, fixture.pbtRoot(), fixture.leaves());
    Eip8347PreimageFile.write(preimages, List.of());

    // Empty preimages rebuilds the empty MPT root; uncovered snapshot leaves are then rejected.
    assertThatThrownBy(
            () ->
                Eip8347DualCheckVerifier.verify(
                    snapshot, preimages, Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes())))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("not covered");
  }

  @Test
  void rejectsPreimageSlotWithoutSnapshotLeaf() throws Exception {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000074");
    final Fixture fixture = Fixture.builder().eoa(address, 0L, Wei.ONE, Map.of()).build();
    final List<AccountPreimages> mismatched =
        List.of(new AccountPreimages(address, List.of(Bytes32.leftPad(Bytes.of(9)))));

    final Path snapshot = tmp.resolve("slot-miss.snap");
    final Path preimages = tmp.resolve("slot-miss.pre");
    Eip8347SnapshotWriter.write(snapshot, fixture.pbtRoot(), fixture.leaves());
    Eip8347PreimageFile.write(preimages, mismatched);

    assertThatThrownBy(
            () -> Eip8347DualCheckVerifier.verify(snapshot, preimages, fixture.mptRoot()))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("has no snapshot leaf");
  }

  @Test
  void rejectsWrongCodeChunk() throws Exception {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000081");
    final Bytes code = Bytes.fromHexString("0x6001600055");
    final Fixture fixture =
        Fixture.builder().contract(address, 0L, Wei.ONE, code, Map.of()).build();
    final List<Eip8347SnapshotLeaf> tampered = new ArrayList<>();
    final Bytes32 codeHash = Bytes32.wrap(Hash.hash(code).getBytes());
    final Bytes chunk0Key = TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash, 0);
    for (final Eip8347SnapshotLeaf leaf : fixture.leaves()) {
      if (leaf.key().equals(chunk0Key)) {
        tampered.add(new Eip8347SnapshotLeaf(leaf.key(), Bytes32.leftPad(Bytes.of(0xaa))));
      } else {
        tampered.add(leaf);
      }
    }
    final Bytes32 tamperedRoot = recomputePbtRoot(tampered);

    final Path snapshot = tmp.resolve("badchunk.snap");
    final Path preimages = tmp.resolve("badchunk.pre");
    Eip8347SnapshotWriter.write(snapshot, tamperedRoot, tampered);
    Eip8347PreimageFile.write(preimages, fixture.preimages());

    assertThatThrownBy(
            () -> Eip8347DualCheckVerifier.verify(snapshot, preimages, fixture.mptRoot()))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("code");
  }

  @Test
  void rejectsWrongClaimedCodeSize() throws Exception {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000082");
    final Bytes code = Bytes.fromHexString("0x6001600055");
    final Fixture fixture =
        Fixture.builder().contract(address, 0L, Wei.ONE, code, Map.of()).build();
    final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
    final Bytes basicKey = TrieKeyDerivation.getTreeKeyForBasicData(address32);
    final List<Eip8347SnapshotLeaf> tampered = new ArrayList<>();
    for (final Eip8347SnapshotLeaf leaf : fixture.leaves()) {
      if (leaf.key().equals(basicKey)) {
        tampered.add(
            new Eip8347SnapshotLeaf(
                leaf.key(),
                BasicDataEncoder.encodeBasicData(code.size() + 1L, 0L, Wei.ONE.toUInt256())));
      } else {
        tampered.add(leaf);
      }
    }
    final Bytes32 tamperedRoot = recomputePbtRoot(tampered);

    final Path snapshot = tmp.resolve("badsize.snap");
    final Path preimages = tmp.resolve("badsize.pre");
    Eip8347SnapshotWriter.write(snapshot, tamperedRoot, tampered);
    Eip8347PreimageFile.write(preimages, fixture.preimages());

    assertThatThrownBy(
            () -> Eip8347DualCheckVerifier.verify(snapshot, preimages, fixture.mptRoot()))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("code");
  }

  @Test
  void rejectsBadBasicDataVersionAtPackTime() throws Exception {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000083");
    final Fixture fixture = Fixture.builder().eoa(address, 1L, Wei.of(7), Map.of()).build();
    final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
    final Bytes basicKey = TrieKeyDerivation.getTreeKeyForBasicData(address32);
    final List<Eip8347SnapshotLeaf> tampered = new ArrayList<>();
    for (final Eip8347SnapshotLeaf leaf : fixture.leaves()) {
      if (leaf.key().equals(basicKey)) {
        final byte[] raw = leaf.value().toArray();
        raw[0] = (byte) (EmbeddingParameters.BASIC_DATA_VERSION + 1);
        tampered.add(new Eip8347SnapshotLeaf(leaf.key(), Bytes32.wrap(raw)));
      } else {
        tampered.add(leaf);
      }
    }
    final Path snapshot = tmp.resolve("badver.snap");
    // Typed packing re-derives basic-data from nonce/balance/codeSize; non-canonical leaves are
    // rejected when packing the header record.
    assertThatThrownBy(() -> Eip8347SnapshotWriter.write(snapshot, fixture.pbtRoot(), tampered))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("basic-data");
  }

  @Test
  void rejectsBadBasicDataReservedBytesAtPackTime() throws Exception {
    final Address address = Address.fromHexString("0x0000000000000000000000000000000000000084");
    final Fixture fixture = Fixture.builder().eoa(address, 0L, Wei.ONE, Map.of()).build();
    final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
    final Bytes basicKey = TrieKeyDerivation.getTreeKeyForBasicData(address32);
    final List<Eip8347SnapshotLeaf> tampered = new ArrayList<>();
    for (final Eip8347SnapshotLeaf leaf : fixture.leaves()) {
      if (leaf.key().equals(basicKey)) {
        final byte[] raw = leaf.value().toArray();
        raw[2] = 0x01;
        tampered.add(new Eip8347SnapshotLeaf(leaf.key(), Bytes32.wrap(raw)));
      } else {
        tampered.add(leaf);
      }
    }
    final Path snapshot = tmp.resolve("badres.snap");
    assertThatThrownBy(() -> Eip8347SnapshotWriter.write(snapshot, fixture.pbtRoot(), tampered))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("basic-data");
  }

  @Test
  void rejectsTruncatedPreimage() throws Exception {
    final Path preimages = tmp.resolve("trunc.pre");
    Files.write(preimages, new byte[] {1, 2, 3});
    assertThatThrownBy(
            () -> {
              try (final Eip8347PreimageFile reader = new Eip8347PreimageFile(preimages)) {
                reader.forEach(r -> {});
              }
            })
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("truncated");
  }

  @Test
  void ascendingPatriciaTrieMatchesSimpleMpt() {
    final MerkleTrie<Bytes, Bytes> simple = new SimpleMerklePatriciaTrie<>(Function.identity());
    final AscendingCollapsePatriciaTrie ascending = new AscendingCollapsePatriciaTrie();
    final List<Bytes> keys = new ArrayList<>();
    for (int i = 0; i < 64; i++) {
      keys.add(Hash.hash(Bytes.ofUnsignedInt(i)).getBytes());
    }
    keys.sort(Comparator.naturalOrder());
    for (int i = 0; i < keys.size(); i++) {
      final int idx = i;
      final Bytes value = RLP.encode(out -> out.writeBytes(Bytes.ofUnsignedInt(idx + 1)));
      simple.put(keys.get(i), value);
      ascending.insert(keys.get(i), value);
    }
    assertThat(ascending.rootHash()).isEqualTo(simple.getRootHash());
    assertThat(new AscendingCollapsePatriciaTrie().rootHash())
        .isEqualTo(new SimpleMerklePatriciaTrie<>(Function.identity()).getRootHash());
  }

  @Test
  void ascendingPatriciaTrieRejectsDescendingKeys() {
    final AscendingCollapsePatriciaTrie ascending = new AscendingCollapsePatriciaTrie();
    final Bytes high = Bytes.fromHexString("0x" + "ff".repeat(32));
    final Bytes low = Bytes.fromHexString("0x" + "00".repeat(32));
    final Bytes value = RLP.encode(out -> out.writeInt(1));
    ascending.insert(high, value);
    assertThatThrownBy(() -> ascending.insert(low, value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ascending");
  }

  @Test
  void snapshotReaderRoundTrip() throws Exception {
    final Fixture fixture =
        Fixture.builder()
            .eoa(
                Address.fromHexString("0x0000000000000000000000000000000000000044"),
                2L,
                Wei.of(5),
                Map.of())
            .build();
    final Path snapshot = tmp.resolve("round.snap");
    Eip8347SnapshotWriter.write(snapshot, fixture.pbtRoot(), fixture.leaves());

    try (final Eip8347SnapshotReader reader = new Eip8347SnapshotReader(snapshot)) {
      assertThat(reader.claimedRoot()).isEqualTo(fixture.pbtRoot());
      final List<Eip8347SnapshotLeaf> read = new ArrayList<>();
      reader.forEach(read::add);
      reader.ensureExhausted();
      assertThat(reader.leafCount()).isEqualTo(fixture.leaves().size());
      assertThat(read).hasSize(fixture.leaves().size());
      assertThat(read.getFirst().key()).isEqualTo(fixture.leaves().getFirst().key());
    }
  }

  @Test
  void ascendingBinaryTrieMatchesStoredTrieRoot() throws Exception {
    final Fixture fixture =
        Fixture.builder()
            .contract(
                Address.fromHexString("0x0000000000000000000000000000000000000055"),
                0L,
                Wei.ZERO,
                Bytes.fromHexString("0x6000"),
                Map.of(UInt256.valueOf(3), UInt256.valueOf(99)))
            .build();
    assertThat(fixture.pbtRoot()).isNotEqualTo(TrieConstants.EMPTY_TRIE_ROOT);
    final AscendingCollapseBinaryTrie ascending = new AscendingCollapseBinaryTrie();
    for (final Eip8347SnapshotLeaf leaf : fixture.leaves()) {
      ascending.insert(leaf.key(), leaf.value());
    }
    assertThat(ascending.rootHash()).isEqualTo(fixture.pbtRoot());
  }

  private static Bytes32 recomputePbtRoot(final List<Eip8347SnapshotLeaf> leaves) {
    final List<Eip8347SnapshotLeaf> ordered = new ArrayList<>(leaves);
    ordered.sort(Comparator.comparing(Eip8347SnapshotLeaf::key));
    final AscendingCollapseBinaryTrie pbt = new AscendingCollapseBinaryTrie();
    for (final Eip8347SnapshotLeaf leaf : ordered) {
      pbt.insert(leaf.key(), leaf.value());
    }
    return pbt.rootHash();
  }

  /** Writes preimage records in the given order without sorting (for negative tests). */
  private static void writePreimagesInGivenOrder(
      final Path path, final List<AccountPreimages> records) throws Exception {
    try (final OutputStream out = Files.newOutputStream(path)) {
      for (final AccountPreimages record : records) {
        out.write(record.address().getBytes().toArrayUnsafe());
        out.write(
            ByteBuffer.allocate(4)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(record.slotKeys().size())
                .array());
        for (final Bytes32 slot : record.slotKeys()) {
          out.write(slot.toArray());
        }
      }
    }
  }

  /** Builds small EIP-8347 fixtures from Besu PBT/MPT APIs. */
  private static final class Fixture {
    private final List<Eip8347SnapshotLeaf> leaves;
    private final List<AccountPreimages> preimages;
    private final Bytes32 pbtRoot;
    private final Bytes32 mptRoot;

    private Fixture(
        final List<Eip8347SnapshotLeaf> leaves,
        final List<AccountPreimages> preimages,
        final Bytes32 pbtRoot,
        final Bytes32 mptRoot) {
      this.leaves = leaves;
      this.preimages = preimages;
      this.pbtRoot = pbtRoot;
      this.mptRoot = mptRoot;
    }

    List<Eip8347SnapshotLeaf> leaves() {
      return leaves;
    }

    List<AccountPreimages> preimages() {
      return preimages;
    }

    Bytes32 pbtRoot() {
      return pbtRoot;
    }

    Bytes32 mptRoot() {
      return mptRoot;
    }

    static Builder builder() {
      return new Builder();
    }

    static final class Builder {
      private final Map<Bytes, Bytes32> leafMap = new TreeMap<>();
      private final List<AccountPreimages> preimages = new ArrayList<>();
      private final Map<Address, AccountState> accounts = new HashMap<>();

      Builder eoa(
          final Address address,
          final long nonce,
          final Wei balance,
          final Map<UInt256, UInt256> storage) {
        accounts.put(address, new AccountState(nonce, balance, Bytes.EMPTY, storage, false));
        return this;
      }

      Builder contract(
          final Address address,
          final long nonce,
          final Wei balance,
          final Bytes code,
          final Map<UInt256, UInt256> storage) {
        accounts.put(address, new AccountState(nonce, balance, code, storage, false));
        return this;
      }

      Builder delegation(
          final Address address, final long nonce, final Wei balance, final Bytes delegationCode) {
        accounts.put(address, new AccountState(nonce, balance, delegationCode, Map.of(), true));
        return this;
      }

      Fixture build() throws Exception {
        final Map<Bytes32, Bytes> codeByHash = new HashMap<>();
        for (final var entry : accounts.entrySet()) {
          final Address address = entry.getKey();
          final AccountState state = entry.getValue();
          final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
          final List<Bytes32> slotKeys = new ArrayList<>();
          for (final var slot : state.storage.entrySet()) {
            if (UInt256.ZERO.equals(slot.getValue())) {
              continue;
            }
            slotKeys.add(Bytes32.leftPad(slot.getKey()));
            putLeaf(
                TrieKeyDerivation.getTreeKeyForStorageSlot(address32, slot.getKey()),
                Bytes32.leftPad(slot.getValue()));
          }
          preimages.add(new AccountPreimages(address, slotKeys));

          if (state.delegation) {
            final long codeSize = EmbeddingParameters.DELEGATION_CODE_SIZE;
            putLeaf(
                TrieKeyDerivation.getTreeKeyForBasicData(address32),
                BasicDataEncoder.encodeBasicData(codeSize, state.nonce, state.balance.toUInt256()));
            putLeaf(
                TrieKeyDerivation.getTreeKeyForDelegation(address32),
                DelegationEncoder.encodeDelegation(
                    CodeDelegationHelper.getTargetAddress(state.code).getBytes()));
          } else {
            final Hash codeHash = state.code.isEmpty() ? Hash.EMPTY : Hash.hash(state.code);
            putLeaf(
                TrieKeyDerivation.getTreeKeyForBasicData(address32),
                BasicDataEncoder.encodeBasicData(
                    state.code.size(), state.nonce, state.balance.toUInt256()));
            putLeaf(
                TrieKeyDerivation.getTreeKeyForCodeHash(address32),
                Bytes32.wrap(codeHash.getBytes()));
            if (!state.code.isEmpty()) {
              codeByHash.putIfAbsent(Bytes32.wrap(codeHash.getBytes()), state.code);
            }
          }
        }
        for (final var codeEntry : codeByHash.entrySet()) {
          final Bytes32 codeHash = codeEntry.getKey();
          final List<Bytes32> chunks = CodeChunkifier.chunkifyCode(codeEntry.getValue());
          for (int i = 0; i < chunks.size(); i++) {
            if (!Bytes32.ZERO.equals(chunks.get(i))) {
              putLeaf(TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash, i), chunks.get(i));
            }
          }
        }

        final List<Eip8347SnapshotLeaf> leaves = new ArrayList<>();
        for (final var e : leafMap.entrySet()) {
          leaves.add(new Eip8347SnapshotLeaf(e.getKey(), e.getValue()));
        }
        leaves.sort(Comparator.comparing(Eip8347SnapshotLeaf::key));

        final StoredPartitionedBinaryTrie pbt =
            new StoredPartitionedBinaryTrie(EMPTY_LOADER, Bytes32.ZERO);
        for (final Eip8347SnapshotLeaf leaf : leaves) {
          pbt.put(leaf.key(), leaf.value());
        }
        final Bytes32 pbtRoot = pbt.getRootHash();

        final Bytes32 mptRoot = computeMptRoot(accounts);
        return new Fixture(leaves, preimages, pbtRoot, mptRoot);
      }

      private void putLeaf(final Bytes key, final Bytes32 value) {
        if (Bytes32.ZERO.equals(value)) {
          return;
        }
        leafMap.put(key, value);
      }

      private static Bytes32 computeMptRoot(final Map<Address, AccountState> accounts) {
        final MerkleTrie<Bytes, Bytes> accountTrie =
            new SimpleMerklePatriciaTrie<>(Function.identity());
        final List<Address> ordered = new ArrayList<>(accounts.keySet());
        ordered.sort(Comparator.comparing(Address::addressHash));
        for (final Address address : ordered) {
          final AccountState state = accounts.get(address);
          final Hash storageRoot = storageRoot(state.storage);
          final Hash codeHash;
          if (state.delegation) {
            codeHash = Hash.hash(state.code);
          } else {
            codeHash = state.code.isEmpty() ? Hash.EMPTY : Hash.hash(state.code);
          }
          final PatriciaTrieAccountValue account =
              new PatriciaTrieAccountValue(state.nonce, state.balance, storageRoot, codeHash);
          accountTrie.put(address.addressHash().getBytes(), RLP.encode(account::writeTo));
        }
        return accountTrie.getRootHash();
      }

      private static Hash storageRoot(final Map<UInt256, UInt256> storage) {
        if (storage.isEmpty()) {
          return Hash.EMPTY_TRIE_HASH;
        }
        final MerkleTrie<Bytes, Bytes> storageTrie =
            new SimpleMerklePatriciaTrie<>(Function.identity());
        final List<UInt256> keys = new ArrayList<>(storage.keySet());
        keys.sort(Comparator.comparing(k -> Hash.hash(Bytes32.leftPad(k))));
        boolean any = false;
        for (final UInt256 key : keys) {
          final UInt256 value = storage.get(key);
          if (UInt256.ZERO.equals(value)) {
            continue;
          }
          any = true;
          final Bytes encoded =
              RLP.encode(out -> out.writeBytes(Bytes32.leftPad(value).trimLeadingZeros()));
          storageTrie.put(Hash.hash(Bytes32.leftPad(key)).getBytes(), encoded);
        }
        return any ? Hash.wrap(storageTrie.getRootHash()) : Hash.EMPTY_TRIE_HASH;
      }
    }

    private record AccountState(
        long nonce, Wei balance, Bytes code, Map<UInt256, UInt256> storage, boolean delegation) {}
  }
}
