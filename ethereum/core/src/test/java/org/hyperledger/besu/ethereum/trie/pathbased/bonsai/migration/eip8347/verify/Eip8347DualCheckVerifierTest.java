/*
 * Copyright contributors to Besu.
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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347Pipelines;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;

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

  @Test
  void acceptsAccountsWithStorageCutIntoRanges() throws Exception {
    // Slots past UNIT_SLOTS are cut into nibble-prefix ranges, attached between other accounts.
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder()
            .eoa(address(0xa1), 1L, Wei.of(7), randomStorage(5, 30))
            .contract(
                address(0xa2),
                2L,
                Wei.of(9),
                CODE,
                randomStorage(1, Eip8347MptRebuild.UNIT_SLOTS * 2 + 7))
            .eoa(address(0xa3), 3L, Wei.ONE, randomStorage(2, Eip8347MptRebuild.UNIT_SLOTS))
            .eoa(address(0xa4), 4L, Wei.ONE, randomStorage(3, Eip8347MptRebuild.UNIT_SLOTS + 1))
            .build();

    verify(fixture.write(tmp, "storage-ranges"), fixture.mptRoot());
  }

  @Test
  void acceptsStorageWhoseKeysShareAPrefixPastTheRangeCap() throws Exception {
    // Keys ground on the first nibble of their keccak: one range overflows and is inserted slot by
    // slot.
    final Map<UInt256, UInt256> storage = new HashMap<>();
    for (long slot = 0; storage.size() < Eip8347MptRebuild.MAX_RANGE_KEYS + 500; slot++) {
      final UInt256 key = UInt256.valueOf(slot);
      if ((Hash.hash(key).getBytes().get(0) & 0xf0) == 0) {
        storage.put(key, UInt256.valueOf(slot + 1));
      }
    }
    storage.put(UInt256.MAX_VALUE, UInt256.ONE);
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder()
            .contract(address(0xa5), 1L, Wei.ONE, CODE, storage)
            .eoa(address(0xa6), 1L, Wei.ONE, Map.of())
            .build();

    verify(fixture.write(tmp, "ground"), fixture.mptRoot());
  }

  @Test
  void acceptsAccountsCutIntoRanges() throws Exception {
    // 5,000 accounts: the account trie is built in 16 ranges, on the first nibble of their path.
    final Eip8347Fixture.Builder builder = Eip8347Fixture.builder();
    for (int i = 0; i < 5_000; i++) {
      builder.eoa(address(0x1000 + i), i, Wei.of(i + 1L), Map.of());
    }
    final Eip8347Fixture fixture =
        builder
            .contract(
                address(0xb1),
                1L,
                Wei.ONE,
                CODE,
                randomStorage(9, Eip8347MptRebuild.UNIT_SLOTS + 3))
            .build();

    verify(fixture.write(tmp, "account-ranges"), fixture.mptRoot());
  }

  @Test
  void acceptsAccountsGroundToShareAPrefixPastTheRangeCap() throws Exception {
    // Addresses ground on the first nibble of their keccak: one range overflows and its accounts
    // are inserted one by one.
    final Eip8347Fixture.Builder builder = Eip8347Fixture.builder();
    int ground = 0;
    for (int i = 0; ground < Eip8347MptRebuild.MAX_RANGE_KEYS + 300; i++) {
      final Address address = address(0x100000 + i);
      if ((address.addressHash().getBytes().get(0) & 0xf0) == 0) {
        builder.eoa(address, 1L, Wei.ONE, Map.of());
        ground++;
      }
    }
    final Eip8347Fixture fixture = builder.eoa(address(0xc0), 2L, Wei.ONE, Map.of()).build();

    verify(fixture.write(tmp, "ground-accounts"), fixture.mptRoot());
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
            artifacts.snapshot(),
            artifacts.preimages(),
            fixture.mptRoot(),
            tmp,
            loaded,
            1L,
            ForkJoinPool.commonPool());

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
  void verifyAndLoadReportsAnErrorInTheCodeCheck() throws Exception {
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder().contract(address(0xc4), 1L, Wei.ONE, CODE, Map.of()).build();
    final Artifacts artifacts = fixture.write(tmp, "load-error");
    final Bytes referenceCount =
        TrieNodeCodec.codeRefCountKey(Bytes32.wrap(Hash.hash(CODE).getBytes()));
    // An Error, not an exception, on the code check's thread.
    final NodeUpdater failing =
        (location, hash, value) -> {
          if (location.equals(referenceCount)) {
            throw new LinkageError("reference count not stored");
          }
        };

    assertThatThrownBy(
            () ->
                Eip8347DualCheckVerifier.verifyAndLoad(
                    artifacts.snapshot(), artifacts.preimages(), fixture.mptRoot(), tmp, failing))
        .isInstanceOf(LinkageError.class)
        .hasMessage("reference count not stored");
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
    reversed.sort(
        Comparator.comparing((AccountPreimages a) -> a.addressHash().getBytes()).reversed());
    Eip8347Fixture.writePreimagesInGivenOrder(artifacts.preimages(), reversed);

    assertRejected(artifacts, fixture.mptRoot(), "ascending by keccak256(address)");
  }

  // ---- Reject: check 2 (MPT rebuild) ----

  @Test
  void rejectsASlotValueChangedWithThePbtRootRecomputed() throws Exception {
    final Address address = address(0x91);
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder()
            .eoa(address, 1L, Wei.ONE, Map.of(UInt256.valueOf(1000), UInt256.valueOf(5)))
            .build();

    assertTamperedSlotRejected(fixture, address, UInt256.valueOf(1000), "slot-changed");
  }

  @Test
  void rejectsASlotOfALargeStorageChangedWithThePbtRootRecomputed() throws Exception {
    final Address address = address(0x92);
    final Map<UInt256, UInt256> storage = randomStorage(4, Eip8347MptRebuild.UNIT_SLOTS + 100);
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder().eoa(address, 1L, Wei.ONE, storage).build();

    assertTamperedSlotRejected(
        fixture, address, storage.keySet().iterator().next(), "large-storage-slot-changed");
  }

  @Test
  void rejectsANonceChangedWithThePbtRootRecomputed() throws Exception {
    final Address address = address(0x93);
    final Eip8347Fixture fixture = oneEoa(address);
    final List<Leaf> tampered =
        fixture.withLeaf(
            basicDataKey(address), BasicDataEncoder.encodeBasicData(0L, 1L, UInt256.ONE));
    final Artifacts artifacts =
        fixture.write(tmp, "nonce-changed", Eip8347Fixture.pbtRootOf(tampered), tampered);

    assertRejected(artifacts, fixture.mptRoot(), "MPT stateRoot mismatch");
  }

  @Test
  void rejectsAFakeAccountInBothFiles() throws Exception {
    final Eip8347Fixture anchored = oneEoa(0x94);
    final Eip8347Fixture withFake =
        Eip8347Fixture.builder()
            .eoa(address(0x94), 0L, Wei.ONE, Map.of())
            .eoa(address(0x95), 0L, Wei.ONE, Map.of())
            .build();

    assertRejected(withFake.write(tmp, "fake"), anchored.mptRoot(), "MPT stateRoot mismatch");
  }

  @Test
  void rejectsPreimageSlotsOutOfOrder() throws Exception {
    assertSlotsOutOfOrderRejected(randomStorage(6, 2), 0, "unsorted-slots");
  }

  @Test
  void rejectsPreimageSlotsOfALargeStorageOutOfOrder() throws Exception {
    assertSlotsOutOfOrderRejected(
        randomStorage(7, Eip8347MptRebuild.UNIT_SLOTS + 10), 2000, "unsorted-large-storage-slots");
  }

  @Test
  void rejectsPreimageSlotsOfALargeStorageOutOfOrderAcrossChunks() throws Exception {
    // The two slots swapped are read in two chunks: the check runs between chunks.
    assertSlotsOutOfOrderRejected(
        randomStorage(8, Eip8347MptRebuild.UNIT_SLOTS + 10),
        Eip8347Pipelines.LIST_WEIGHT - 1,
        "unsorted-large-storage-chunks");
  }

  @Test
  void rejectsAccountsOutOfOrderAcrossRanges() throws Exception {
    // The first and last accounts swapped: each range is sorted, but they are not in order.
    final Eip8347Fixture fixture = manyAccounts();
    final List<AccountPreimages> records = sortedRecords(fixture);
    Collections.swap(records, 0, records.size() - 1);

    assertAccountsOutOfOrderRejected(fixture, records, "accounts-across");
  }

  @Test
  void rejectsAccountsOutOfOrderInsideARange() throws Exception {
    final Eip8347Fixture fixture = manyAccounts();
    final List<AccountPreimages> records = sortedRecords(fixture);
    int i = 100;
    while (records.get(i).addressHash().getBytes().get(0) >>> 4
        != records.get(i + 1).addressHash().getBytes().get(0) >>> 4) {
      i++;
    }
    Collections.swap(records, i, i + 1);

    assertAccountsOutOfOrderRejected(fixture, records, "accounts-inside");
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

  /** Rejects the fixture with {@code slot} changed and its PBT root recomputed. */
  private void assertTamperedSlotRejected(
      final Eip8347Fixture fixture, final Address address, final UInt256 slot, final String name)
      throws Exception {
    final Bytes key =
        TrieKeyDerivation.getTreeKeyForStorageSlot(
            TrieKeyDerivation.address20ToAddress32(address.getBytes()), slot);
    final List<Leaf> tampered = fixture.withLeaf(key, Bytes32.leftPad(Bytes.of(0x77)));
    final Artifacts artifacts =
        fixture.write(tmp, name, Eip8347Fixture.pbtRootOf(tampered), tampered);

    assertRejected(artifacts, fixture.mptRoot(), "MPT stateRoot mismatch");
  }

  /** Rejects preimages whose slots at {@code index} and {@code index + 1} are swapped. */
  private void assertSlotsOutOfOrderRejected(
      final Map<UInt256, UInt256> storage, final int index, final String name) throws Exception {
    final Address address = address(0x96);
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder().eoa(address, 1L, Wei.ONE, storage).build();
    final Artifacts artifacts = fixture.write(tmp, name);
    final List<Bytes32> slots =
        new ArrayList<>(
            fixture.preimages().getFirst().slotKeys().stream()
                .sorted(Comparator.comparing(slot -> Hash.hash(slot).getBytes()))
                .toList());
    Collections.swap(slots, index, index + 1);
    Eip8347Fixture.writePreimagesInGivenOrder(
        artifacts.preimages(), List.of(new AccountPreimages(address, slots)));

    assertRejected(artifacts, fixture.mptRoot(), "ascending by keccak256(slotKey)");
  }

  /** 5,000 accounts, enough for the account trie to be cut into ranges. */
  private static Eip8347Fixture manyAccounts() {
    final Eip8347Fixture.Builder builder = Eip8347Fixture.builder();
    for (int i = 0; i < 5_000; i++) {
      builder.eoa(address(0x2000 + i), 1L, Wei.ONE, Map.of());
    }
    return builder.build();
  }

  private static List<AccountPreimages> sortedRecords(final Eip8347Fixture fixture) {
    final List<AccountPreimages> records = new ArrayList<>(fixture.preimages());
    records.sort(Comparator.comparing(a -> a.addressHash().getBytes()));
    return records;
  }

  private void assertAccountsOutOfOrderRejected(
      final Eip8347Fixture fixture, final List<AccountPreimages> records, final String name)
      throws Exception {
    final Artifacts artifacts = fixture.write(tmp, name);
    Eip8347Fixture.writePreimagesInGivenOrder(artifacts.preimages(), records);

    assertRejected(artifacts, fixture.mptRoot(), "ascending by keccak256(address)");
  }

  /** {@code count} non-zero slots at random keys. */
  private static Map<UInt256, UInt256> randomStorage(final long seed, final int count) {
    final Random random = new Random(seed);
    final Map<UInt256, UInt256> storage = new HashMap<>();
    while (storage.size() < count) {
      storage.put(
          UInt256.fromBytes(Bytes32.random(random)), UInt256.valueOf(1 + random.nextInt(1 << 20)));
    }
    return storage;
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
}
