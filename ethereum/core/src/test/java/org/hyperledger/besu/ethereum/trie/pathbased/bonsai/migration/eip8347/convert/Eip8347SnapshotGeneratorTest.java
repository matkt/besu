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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieConstants;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.Eip8347Fixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.Eip8347Fixture.Artifacts;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile.AccountPreimages;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.verify.Eip8347DualCheckVerifier;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Eip8347SnapshotGeneratorTest {

  private static final Address EOA =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");

  @TempDir Path tmp;

  @Test
  void generatesAnEmptySnapshotFromEmptyPreimages() throws Exception {
    final Path preimages = tmp.resolve("empty.pre");
    final Path snapshot = tmp.resolve("empty.snap");
    Eip8347PreimageFile.write(preimages, List.of());

    final Eip8347SnapshotGenerator.Result result =
        Eip8347SnapshotGenerator.generate(preimages, address -> Optional.empty(), snapshot, tmp);

    assertThat(result.leafCount()).isZero();
    assertThat(result.pbtRoot()).isEqualTo(TrieConstants.EMPTY_TRIE_ROOT);
    Eip8347DualCheckVerifier.verify(
        snapshot, preimages, Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes()), tmp);
  }

  @Test
  void generatesTheCanonicalSnapshotWhateverTheSortBuffer() throws Exception {
    final Bytes shared = Bytes.fromHexString("0x6001600055");
    final Bytes delegation =
        Bytes.concatenate(
            CodeDelegationHelper.CODE_DELEGATION_PREFIX,
            Address.fromHexString("0x00000000000000000000000000000000000000dd").getBytes());
    final Eip8347Fixture fixture =
        Eip8347Fixture.builder()
            .eoa(
                EOA,
                1L,
                Wei.of(1000),
                Map.of(UInt256.ZERO, UInt256.valueOf(7), UInt256.valueOf(64), UInt256.valueOf(9)))
            .contract(
                Address.fromHexString("0x00000000000000000000000000000000000000b1"),
                0L,
                Wei.ONE,
                shared,
                Map.of())
            .contract(
                Address.fromHexString("0x00000000000000000000000000000000000000b2"),
                3L,
                Wei.of(2),
                shared,
                Map.of())
            .contract(
                Address.fromHexString("0x00000000000000000000000000000000000000cc"),
                2L,
                Wei.of(5),
                delegation,
                Map.of())
            .build();
    final Artifacts expected = fixture.write(tmp, "expected");

    // Default buffer (one in-memory sort) and one record per run (multi-pass merge).
    for (final long sortBuffer : new long[] {64L << 20, 1L}) {
      final Path snapshot = tmp.resolve("generated-" + sortBuffer + ".snap");
      final Eip8347SnapshotGenerator.Result result =
          Eip8347SnapshotGenerator.generate(
              expected.preimages(), fixture.stateSource(), snapshot, tmp, sortBuffer);

      assertThat(result.pbtRoot()).isEqualTo(fixture.pbtRoot());
      assertThat(result.leafCount()).isEqualTo(fixture.leaves().size());
      assertThat(Files.readAllBytes(snapshot)).isEqualTo(Files.readAllBytes(expected.snapshot()));
    }
    Eip8347DualCheckVerifier.verify(
        expected.snapshot(), expected.preimages(), fixture.mptRoot(), tmp);
  }

  @Test
  void rejectsPreimageOfAnAbsentAccount() throws Exception {
    final Path preimages = tmp.resolve("absent.pre");
    Eip8347PreimageFile.write(preimages, List.of(new AccountPreimages(EOA, List.of())));

    assertThatThrownBy(
            () ->
                Eip8347SnapshotGenerator.generate(
                    preimages, address -> Optional.empty(), tmp.resolve("absent.snap"), tmp))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("has no account");
  }

  @Test
  void rejectsPreimageOfAZeroSlot() throws Exception {
    final Eip8347Fixture fixture = Eip8347Fixture.builder().eoa(EOA, 0L, Wei.ONE, Map.of()).build();
    final Path preimages = tmp.resolve("zero.pre");
    Eip8347PreimageFile.write(
        preimages, List.of(new AccountPreimages(EOA, List.of(Bytes32.leftPad(UInt256.ZERO)))));

    assertThatThrownBy(
            () ->
                Eip8347SnapshotGenerator.generate(
                    preimages, fixture.stateSource(), tmp.resolve("zero.snap"), tmp))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("zero value");
  }
}
