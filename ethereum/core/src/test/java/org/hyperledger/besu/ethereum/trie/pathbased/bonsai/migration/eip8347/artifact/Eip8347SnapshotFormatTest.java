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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.Eip8347Fixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.Leaf;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Snapshot leaves, typed records and their byte-canonical encoding. */
class Eip8347SnapshotFormatTest {

  private static final Address ADDRESS =
      Address.fromHexString("0x0000000000000000000000000000000000000044");

  @TempDir Path tmp;

  @Test
  void readerReturnsTheLeavesTheWriterPacked() throws Exception {
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder()
            .contract(
                ADDRESS,
                2L,
                Wei.of(5),
                Bytes.fromHexString("0x6001600055"),
                Map.of(UInt256.ONE, UInt256.valueOf(3), UInt256.valueOf(1000), UInt256.ONE))
            .build();
    final Path snapshot = fixture.write(tmp, "round").snapshot();

    try (final Eip8347SnapshotReader reader = new Eip8347SnapshotReader(snapshot)) {
      final List<Leaf> read = new ArrayList<>();
      reader.units().forEachRemaining(unit -> read.addAll(unit.leaves()));
      reader.ensureExhausted();
      assertThat(reader.claimedRoot()).isEqualTo(fixture.pbtRoot());
      assertThat(reader.leafCount()).isEqualTo(fixture.leaves().size());
      assertThat(read).isEqualTo(fixture.leaves());
    }
  }

  @Test
  void writerRejectsNonZeroBasicDataVersion() {
    assertPackRejected(0, (byte) (EmbeddingParameters.BASIC_DATA_VERSION + 1));
  }

  @Test
  void writerRejectsNonZeroBasicDataReservedByte() {
    assertPackRejected(2, (byte) 1);
  }

  @Test
  void leafRejectsZeroValue() {
    assertThatThrownBy(() -> new Leaf(basicDataKey(), Bytes32.ZERO))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("zero-valued");
  }

  @Test
  void leafRejectsReservedZone() {
    assertThatThrownBy(
            () ->
                new Leaf(
                    Bytes.fromHexString("0x02" + "00".repeat(33)), Bytes32.leftPad(Bytes.of(1))))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("reserved zone");
  }

  @Test
  void leafRejectsKeyLengthOfAnotherZone() {
    final Bytes shortAccountKey =
        Bytes.concatenate(Bytes.of(EmbeddingParameters.ACCOUNT_ZONE), Bytes.repeat((byte) 0, 10));
    assertThatThrownBy(() -> new Leaf(shortAccountKey, Bytes32.leftPad(Bytes.of(1))))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("key length");
  }

  // ---- Byte layout: record * | end | pbtRoot ----

  @Test
  void rejectsASnapshotWithoutTheEndTag() throws Exception {
    final byte[] bytes = oneEoaWithTwoStorageGroups();
    assertRejected(Arrays.copyOf(bytes, bytes.length - TRAILER), "unexpected EOF");
  }

  @Test
  void rejectsTrailingBytes() throws Exception {
    final byte[] bytes = oneEoaWithTwoStorageGroups();
    assertRejected(Arrays.copyOf(bytes, bytes.length + 1), "trailing bytes");
  }

  @Test
  void rejectsAnUnknownTag() throws Exception {
    final byte[] bytes = oneEoaWithTwoStorageGroups();
    bytes[bytes.length - TRAILER] = 0x08;
    assertRejected(bytes, "invalid snapshot record tag");
  }

  @Test
  void rejectsHeaderRecordsOutOfOrder() throws Exception {
    final byte[] first = headerRecordOf(Address.fromHexString("0x" + "00".repeat(19) + "11"));
    final byte[] second = headerRecordOf(Address.fromHexString("0x" + "00".repeat(19) + "22"));
    final boolean ordered = Arrays.compareUnsigned(first, second) < 0;
    assertRejected(
        concat(ordered ? second : first, ordered ? first : second, endTrailer()),
        "not strictly ascending in PBT key order");
  }

  @Test
  void rejectsOneAccountsStorageSplitAcrossTwoStorageAccounts() throws Exception {
    final byte[] bytes = oneEoaWithTwoStorageGroups();
    final int account = bytes.length - TRAILER - 2 * SINGLE_GROUP - STORAGE_ACCOUNT;
    final byte[] storageAccount = Arrays.copyOfRange(bytes, account, account + STORAGE_ACCOUNT);
    final int secondGroup = account + STORAGE_ACCOUNT + SINGLE_GROUP;
    assertRejected(
        concat(
            Arrays.copyOf(bytes, secondGroup),
            storageAccount,
            Arrays.copyOfRange(bytes, secondGroup, bytes.length)),
        "storage records are not strictly ascending");
  }

  @Test
  void rejectsAStorageAccountWithoutGroups() throws Exception {
    final byte[] bytes = oneEoaWithTwoStorageGroups();
    final int groups = bytes.length - TRAILER - 2 * SINGLE_GROUP;
    assertRejected(
        concat(
            Arrays.copyOf(bytes, groups),
            Arrays.copyOfRange(bytes, bytes.length - TRAILER, bytes.length)),
        "is not followed by a storage group");
  }

  @Test
  void rejectsAOneLeafStorageGroupInTheMultiLeafEncoding() throws Exception {
    final byte[] bytes = oneEoaWithTwoStorageGroups();
    final int group = bytes.length - TRAILER - SINGLE_GROUP;
    // 0x05 | stemHash | subIndex | value  →  0x06 | stemHash | n = 0 | subIndex | value
    final byte[] multi =
        concat(
            new byte[] {(byte) Eip8347TypedSnapshotCodec.TAG_STORAGE_GROUP},
            Arrays.copyOfRange(bytes, group + 1, group + 1 + Bytes32.SIZE),
            new byte[] {0},
            Arrays.copyOfRange(bytes, group + 1 + Bytes32.SIZE, group + SINGLE_GROUP));
    assertRejected(
        concat(
            Arrays.copyOf(bytes, group),
            multi,
            Arrays.copyOfRange(bytes, bytes.length - TRAILER, bytes.length)),
        "single-group encoding");
  }

  /** {@code end[1] | pbtRoot[32]}. */
  private static final int TRAILER = 1 + Bytes32.SIZE;

  /** {@code 0x04 | addressHash[32]}. */
  private static final int STORAGE_ACCOUNT = 1 + Bytes32.SIZE;

  /** {@code 0x05 | stemHash[32] | subIndex[1] | value[≤32]} with a one-byte value. */
  private static final int SINGLE_GROUP = 1 + Bytes32.SIZE + 1 + 2;

  /**
   * An EOA whose slots 64 and 1000 sit in two one-leaf storage groups: {@code header | 0x04 account
   * | 0x05 group | 0x05 group | 0x07 | root}.
   */
  private byte[] oneEoaWithTwoStorageGroups() throws Exception {
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder()
            .eoa(
                ADDRESS,
                1L,
                Wei.ONE,
                Map.of(UInt256.valueOf(64), UInt256.valueOf(9), UInt256.valueOf(1000), UInt256.ONE))
            .build();
    return Files.readAllBytes(fixture.write(tmp, "two-groups").snapshot());
  }

  /** The header record of a snapshot holding one storage-less EOA. */
  private byte[] headerRecordOf(final Address address) throws Exception {
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder().eoa(address, 0L, Wei.ONE, Map.of()).build();
    final byte[] bytes = Files.readAllBytes(fixture.write(tmp, address.toHexString()).snapshot());
    return Arrays.copyOf(bytes, bytes.length - TRAILER);
  }

  private static byte[] endTrailer() {
    return concat(new byte[] {(byte) Eip8347TypedSnapshotCodec.TAG_END}, new byte[Bytes32.SIZE]);
  }

  private static byte[] concat(final byte[]... parts) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (final byte[] part : parts) {
      out.writeBytes(part);
    }
    return out.toByteArray();
  }

  /** Reading {@code bytes} as a snapshot, to its end, rejects with {@code why}. */
  private void assertRejected(final byte[] bytes, final String why) throws Exception {
    final Path snapshot = tmp.resolve("tampered.snap");
    Files.write(snapshot, bytes);
    assertThatThrownBy(
            () -> {
              try (final Eip8347SnapshotReader reader = new Eip8347SnapshotReader(snapshot)) {
                reader.units().forEachRemaining(unit -> {});
                reader.ensureExhausted();
              }
            })
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining(why);
  }

  /** A basic-data leaf with byte {@code index} set to {@code value} does not pack into a header. */
  private void assertPackRejected(final int index, final byte value) {
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder().eoa(ADDRESS, 1L, Wei.of(7), Map.of()).build();
    final Bytes basicData =
        fixture.leaves().stream()
            .filter(leaf -> leaf.key().equals(basicDataKey()))
            .findFirst()
            .orElseThrow()
            .value();
    final byte[] raw = basicData.toArray();
    raw[index] = value;
    final List<Leaf> tampered = fixture.withLeaf(basicDataKey(), Bytes32.wrap(raw));

    assertThatThrownBy(
            () -> Eip8347SnapshotWriter.write(tmp.resolve("bad.snap"), fixture.pbtRoot(), tampered))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("basic-data");
  }

  private static Bytes basicDataKey() {
    return TrieKeyDerivation.getTreeKeyForBasicData(
        TrieKeyDerivation.address20ToAddress32(ADDRESS.getBytes()));
  }
}
