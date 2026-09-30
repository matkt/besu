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

import java.nio.file.Path;
import java.util.ArrayList;
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
