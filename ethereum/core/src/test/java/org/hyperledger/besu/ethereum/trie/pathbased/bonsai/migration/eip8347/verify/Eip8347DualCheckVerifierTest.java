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
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.CodeRefCountEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.TrieNodeCodec;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieConstants;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.StoredPartitionedBinaryTrie;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.Eip8347Fixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.Eip8347Fixture.Artifacts;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile.AccountPreimages;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotWriter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.Leaf;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Eip8347DualCheckVerifierTest {

  private static final Bytes CODE = Bytes.fromHexString("0x6001600055");

  @TempDir Path tmp;

  // ---- Accept ----

  @Test
  void acceptsEmptyArtifactsAgainstTheEmptyMptRoot() throws Exception {
    final Path snapshot = tmp.resolve("empty.snap");
    final Path preimages = tmp.resolve("empty.pre");
    Eip8347SnapshotWriter.write(snapshot, TrieConstants.EMPTY_TRIE_ROOT, List.of());
    Eip8347PreimageFile.write(preimages, List.of());

    Eip8347DualCheckVerifier.verify(snapshot, preimages, emptyMptRoot(), tmp);
  }

  @Test
  void acceptsEoaContractAndDelegationWithHeaderAndStorageSlots() throws Exception {
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder()
            .eoa(
                address(0xb1),
                2L,
                Wei.of(100),
                Map.of(UInt256.ZERO, UInt256.valueOf(1), UInt256.valueOf(100), UInt256.valueOf(2)))
            .contract(address(0xb2), 0L, Wei.of(3), CODE, Map.of(UInt256.valueOf(7), UInt256.ONE))
            .contract(address(0xb3), 5L, Wei.of(9), delegationTo(address(0xce)), Map.of())
            .build();
    final Artifacts artifacts = fixture.write(tmp, "mixed");

    verify(artifacts, fixture.mptRoot());
  }

  @Test
  void acceptsWithSortsSpillingOneRecordPerRunAndLeavesNoTempFile() throws Exception {
    final Map<UInt256, UInt256> storage = new HashMap<>();
    for (int slot = 0; slot < 600; slot++) {
      storage.put(UInt256.valueOf(slot * 7L), UInt256.valueOf(slot + 1L));
    }
    // 9000 bytes of code: 291 chunks, i.e. two CODE_ZONE groups, shared by two accounts.
    final Bytes code = Bytes.repeat((byte) 0x5b, 9000);
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder()
            .contract(address(0x33), 3L, Wei.of(10), code, storage)
            .contract(address(0x34), 0L, Wei.ONE, code, Map.of())
            .build();
    final Artifacts artifacts = fixture.write(tmp, "multipass");

    // One record per run: the 600+ preimage requests take more than one merge pass.
    Eip8347DualCheckVerifier.verify(
        artifacts.snapshot(), artifacts.preimages(), fixture.mptRoot(), tmp, 1L);
    try (final var left = Files.list(tmp)) {
      assertThat(left.map(p -> p.getFileName().toString()))
          .noneMatch(n -> n.startsWith("eip8347-"));
    }
  }

  // ---- Load ----

  @Test
  void verifyAndLoadWritesWhatAStoredTrieCommitsPlusCodeReferenceCounts() throws Exception {
    final Bytes shared = Bytes.fromHexString("0x" + "60016001".repeat(40));
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder()
            .contract(
                address(0xc1),
                1L,
                Wei.of(5),
                shared,
                Map.of(UInt256.valueOf(3), UInt256.valueOf(7), UInt256.valueOf(1000), UInt256.ONE))
            .contract(address(0xc2), 0L, Wei.of(9), shared, Map.of())
            .eoa(address(0xe0), 4L, Wei.of(100), Map.of())
            .build();
    final Artifacts artifacts = fixture.write(tmp, "load");

    final NodeStore loaded = new NodeStore();
    final Eip8347DualCheckVerifier.Loaded result =
        Eip8347DualCheckVerifier.verifyAndLoad(
            artifacts.snapshot(), artifacts.preimages(), fixture.mptRoot(), tmp, loaded, 1L);

    assertThat(result.pbtRoot()).isEqualTo(fixture.pbtRoot());
    assertThat(result.leafCount()).isEqualTo(fixture.leaves().size());
    assertThat(result.codeHashes()).isEqualTo(1);

    // Same nodes as a stored trie commit of the same leaves, plus one reference count.
    final NodeStore committed = new NodeStore();
    final StoredPartitionedBinaryTrie reference =
        new StoredPartitionedBinaryTrie((location, hash) -> Optional.empty(), Bytes32.ZERO);
    fixture.leaves().forEach(leaf -> reference.put(leaf.key(), leaf.value()));
    reference.commit(committed);
    committed.nodes.put(
        TrieNodeCodec.codeRefCountKey(Bytes32.wrap(Hash.hash(shared).getBytes())),
        CodeRefCountEncoder.encode(2, CodeChunkifier.chunkifyCode(shared).size()));
    assertThat(loaded.nodes).isEqualTo(committed.nodes);
  }

  @Test
  void verifyAndLoadRejectsAWrongAnchor() throws Exception {
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder().contract(address(0xc3), 1L, Wei.ONE, CODE, Map.of()).build();
    final Artifacts artifacts = fixture.write(tmp, "load-bad");

    final NodeStore loaded = new NodeStore();
    assertThatThrownBy(
            () ->
                Eip8347DualCheckVerifier.verifyAndLoad(
                    artifacts.snapshot(),
                    artifacts.preimages(),
                    Bytes32.repeat((byte) 1),
                    tmp,
                    loaded))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("MPT stateRoot mismatch");
  }

  // ---- Reject: roots ----

  @Test
  void rejectsWrongClaimedPbtRoot() throws Exception {
    final Eip8347Fixture fixture = oneEoa(0xee);
    final Artifacts artifacts = fixture.write(tmp, "bad-root", Bytes32.ZERO, fixture.leaves());

    assertRejected(artifacts, fixture.mptRoot(), "PBT root mismatch");
  }

  @Test
  void rejectsWrongAnchorStateRoot() throws Exception {
    final Artifacts artifacts = oneEoa(0xef).write(tmp, "bad-mpt");

    assertRejected(artifacts, Bytes32.repeat((byte) 0x11), "MPT stateRoot mismatch");
  }

  // ---- Reject: snapshot layout ----

  @Test
  void rejectsHeaderCountLargerThanTheRecords() throws Exception {
    final Eip8347Fixture fixture = oneEoa(0x10);
    final Artifacts artifacts = fixture.write(tmp, "bad-count");
    final byte[] bytes = Files.readAllBytes(artifacts.snapshot());
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.putLong(32, buffer.getLong(32) + 1); // headerCount
    Files.write(artifacts.snapshot(), bytes);

    assertThatThrownBy(() -> verify(artifacts, fixture.mptRoot()))
        .isInstanceOf(Eip8347ArtifactVerificationException.class);
  }

  @Test
  void rejectsHeaderRecordsOutOfOrder() throws Exception {
    final Address a = address(0x11);
    final Address b = address(0x22);
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder()
            .eoa(a, 0L, Wei.ONE, Map.of())
            .eoa(b, 0L, Wei.ONE, Map.of())
            .build();
    final byte[] headerA = singleHeaderRecord(oneEoa(a).write(tmp, "a"));
    final byte[] headerB = singleHeaderRecord(oneEoa(b).write(tmp, "b"));
    final boolean aFirst = Arrays.compareUnsigned(headerA, headerB) < 0;
    final Artifacts artifacts = fixture.write(tmp, "unsorted");
    try (final OutputStream out = Files.newOutputStream(artifacts.snapshot())) {
      out.write(fixture.pbtRoot().toArrayUnsafe());
      out.write(count(2));
      out.write(aFirst ? headerB : headerA);
      out.write(aFirst ? headerA : headerB);
      out.write(count(0)); // code
      out.write(count(0)); // storage
    }

    // The reader's order check and the join run concurrently; either may report first.
    assertThatThrownBy(() -> verify(artifacts, fixture.mptRoot()))
        .isInstanceOf(Eip8347ArtifactVerificationException.class);
  }

  @Test
  void rejectsOneAccountsStorageSplitAcrossTwoRecords() throws Exception {
    // Slots 64 and 1000 land in two storage groups of one account: one record, groupCount 2.
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder()
            .eoa(
                address(0x32),
                1L,
                Wei.ONE,
                Map.of(UInt256.valueOf(64), UInt256.valueOf(9), UInt256.valueOf(1000), UInt256.ONE))
            .build();
    final Artifacts artifacts = fixture.write(tmp, "split");
    final byte[] bytes = Files.readAllBytes(artifacts.snapshot());

    // Tail: storageCount[8]=1 | addressHash[32] | groupCount=0x01 0x02 | group(36) | group(36)
    final int groupSize = 36;
    final int recordStart = bytes.length - (32 + 2 + 2 * groupSize);
    final byte[] addressHash = Arrays.copyOfRange(bytes, recordStart, recordStart + 32);
    final int groupsStart = recordStart + 34;
    try (final OutputStream out = Files.newOutputStream(artifacts.snapshot())) {
      out.write(bytes, 0, recordStart - 8);
      out.write(count(2));
      for (int g = 0; g < 2; g++) {
        out.write(addressHash);
        out.write(new byte[] {1, 1});
        out.write(bytes, groupsStart + g * groupSize, groupSize);
      }
    }

    assertRejected(artifacts, fixture.mptRoot(), "storage records are not strictly ascending");
  }

  // ---- Reject: consensus anchoring (preimages ⋈ snapshot) ----

  @Test
  void rejectsHeaderSlotWithoutPreimage() throws Exception {
    final Address address = address(0x31);
    final Eip8347Fixture withSurplus =
        Eip8347Fixture.builder()
            .eoa(
                address,
                1L,
                Wei.ONE,
                Map.of(UInt256.ZERO, UInt256.valueOf(7), UInt256.ONE, UInt256.valueOf(5)))
            .build();
    final Eip8347Fixture anchored =
        Eip8347Fixture.builder()
            .eoa(address, 1L, Wei.ONE, Map.of(UInt256.ZERO, UInt256.valueOf(7)))
            .build();
    // Snapshot with header slots 0 and 1; preimages and MPT only know slot 0.
    final Artifacts artifacts =
        anchored.write(tmp, "surplus-slot", withSurplus.pbtRoot(), withSurplus.leaves());

    assertRejected(artifacts, anchored.mptRoot(), "not covered by consensus anchoring");
  }

  @Test
  void rejectsSnapshotAccountWithoutPreimage() throws Exception {
    final Eip8347Fixture fixture = oneEoa(0x73);
    final Artifacts artifacts = fixture.write(tmp, "missing-pre");
    Eip8347PreimageFile.write(artifacts.preimages(), List.of());

    assertRejected(artifacts, emptyMptRoot(), "not covered by consensus anchoring");
  }

  @Test
  void rejectsPreimageAccountWithoutSnapshotLeaf() throws Exception {
    final Address surplus = address(0x72);
    final Eip8347Fixture fixture = oneEoa(0x71);
    final Artifacts artifacts = fixture.write(tmp, "surplus-pre");
    final List<AccountPreimages> withSurplus = new ArrayList<>(fixture.preimages());
    withSurplus.add(new AccountPreimages(surplus, List.of()));
    Eip8347PreimageFile.write(artifacts.preimages(), withSurplus);

    assertRejected(
        artifacts, fixture.mptRoot(), "preimage account " + surplus + " has no snapshot leaf");
  }

  @Test
  void rejectsPreimageSlotWithoutSnapshotLeaf() throws Exception {
    final Address address = address(0x74);
    final Bytes32 slot = Bytes32.leftPad(Bytes.of(9));
    final Eip8347Fixture fixture = oneEoa(address);
    final Artifacts artifacts = fixture.write(tmp, "slot-miss");
    Eip8347PreimageFile.write(
        artifacts.preimages(), List.of(new AccountPreimages(address, List.of(slot))));

    assertRejected(
        artifacts,
        fixture.mptRoot(),
        "preimage slot " + slot.toHexString() + " of " + address + " has no snapshot leaf");
  }

  @Test
  void rejectsPreimageRecordsOutOfOrder() throws Exception {
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder()
            .eoa(address(0x61), 0L, Wei.ONE, Map.of())
            .eoa(address(0x62), 0L, Wei.ONE, Map.of())
            .build();
    final Artifacts artifacts = fixture.write(tmp, "unsorted-pre");
    final List<AccountPreimages> reversed = new ArrayList<>(fixture.preimages());
    reversed.sort(Comparator.comparing(AccountPreimages::addressHash).reversed());
    Eip8347Fixture.writePreimagesInGivenOrder(artifacts.preimages(), reversed);

    assertRejected(artifacts, fixture.mptRoot(), "ascending by keccak256(address)");
  }

  // ---- Reject: code ----

  @Test
  void rejectsTamperedCodeChunk() throws Exception {
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder().contract(address(0x81), 0L, Wei.ONE, CODE, Map.of()).build();
    final List<Leaf> tampered =
        fixture.withLeaf(
            TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash(CODE), 0),
            Bytes32.leftPad(Bytes.of(0xaa)));
    final Artifacts artifacts =
        fixture.write(tmp, "bad-chunk", Eip8347Fixture.pbtRootOf(tampered), tampered);

    assertRejected(artifacts, fixture.mptRoot(), "reassembled code hash mismatch");
  }

  @Test
  void rejectsWrongCodeSize() throws Exception {
    final Address address = address(0x82);
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder().contract(address, 0L, Wei.ONE, CODE, Map.of()).build();
    final List<Leaf> tampered =
        fixture.withLeaf(
            basicDataKey(address),
            BasicDataEncoder.encodeBasicData(CODE.size() + 1L, 0L, UInt256.ONE));
    final Artifacts artifacts =
        fixture.write(tmp, "bad-size", Eip8347Fixture.pbtRootOf(tampered), tampered);

    assertRejected(artifacts, fixture.mptRoot(), "reassembled code hash mismatch");
  }

  @Test
  void rejectsSharedCodeWithTwoSizes() throws Exception {
    final Address second = address(0x84);
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder()
            .contract(address(0x83), 0L, Wei.ONE, CODE, Map.of())
            .contract(second, 0L, Wei.ONE, CODE, Map.of())
            .build();
    final List<Leaf> tampered =
        fixture.withLeaf(
            basicDataKey(second),
            BasicDataEncoder.encodeBasicData(CODE.size() + 1L, 0L, UInt256.ONE));
    final Artifacts artifacts =
        fixture.write(tmp, "two-sizes", Eip8347Fixture.pbtRootOf(tampered), tampered);

    assertRejected(artifacts, fixture.mptRoot(), "disagree on codeSize");
  }

  @Test
  void rejectsCodeGroupNoHeaderReferences() throws Exception {
    final Eip8347Fixture fixture = oneEoa(0x85);
    final List<Leaf> withOrphan = new ArrayList<>(fixture.leaves());
    withOrphan.add(
        new Leaf(
            TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash(CODE), 0),
            CodeChunkifier.chunkifyCode(CODE).getFirst()));
    final Artifacts artifacts =
        fixture.write(tmp, "orphan-code", Eip8347Fixture.pbtRootOf(withOrphan), withOrphan);

    assertRejected(artifacts, fixture.mptRoot(), "is not referenced by any header record");
  }

  // ---- Helpers ----

  /** Stored nodes by location, as the binary column holds them. */
  private static final class NodeStore implements NodeUpdater {
    final Map<Bytes, Bytes> nodes = new HashMap<>();

    @Override
    public void store(final Bytes location, final Bytes32 hash, final Bytes value) {
      if (value == null) {
        nodes.remove(location);
      } else {
        nodes.put(location, value);
      }
    }
  }

  private void verify(final Artifacts artifacts, final Bytes32 mptRoot) throws Exception {
    Eip8347DualCheckVerifier.verify(artifacts.snapshot(), artifacts.preimages(), mptRoot, tmp);
  }

  private void assertRejected(final Artifacts artifacts, final Bytes32 mptRoot, final String why) {
    assertThatThrownBy(() -> verify(artifacts, mptRoot))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining(why);
  }

  private static Eip8347Fixture oneEoa(final int addressByte) {
    return oneEoa(address(addressByte));
  }

  private static Eip8347Fixture oneEoa(final Address address) {
    return Eip8347Fixture.builder().eoa(address, 0L, Wei.ONE, Map.of()).build();
  }

  private static Address address(final int lastByte) {
    return Address.fromHexString(String.format("0x%040x", lastByte));
  }

  private static Bytes delegationTo(final Address target) {
    return Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, target.getBytes());
  }

  private static Bytes32 codeHash(final Bytes code) {
    return Bytes32.wrap(Hash.hash(code).getBytes());
  }

  private static Bytes basicDataKey(final Address address) {
    return TrieKeyDerivation.getTreeKeyForBasicData(
        TrieKeyDerivation.address20ToAddress32(address.getBytes()));
  }

  private static Bytes32 emptyMptRoot() {
    return Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes());
  }

  private static byte[] count(final long count) {
    return ByteBuffer.allocate(8).putLong(count).array();
  }

  /** The header record of a one-EOA snapshot: {@code pbtRoot | 1 | record | 0 | 0}. */
  private static byte[] singleHeaderRecord(final Artifacts artifacts) throws Exception {
    final byte[] snapshot = Files.readAllBytes(artifacts.snapshot());
    return Arrays.copyOfRange(snapshot, 40, snapshot.length - 16);
  }
}
