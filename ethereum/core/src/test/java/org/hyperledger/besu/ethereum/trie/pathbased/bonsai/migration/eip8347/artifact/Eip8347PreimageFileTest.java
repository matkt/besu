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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.config.GenesisAccount;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.Eip8347Fixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile.AccountPreimages;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Eip8347PreimageFileTest {

  private static final Address A =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");
  private static final Address B =
      Address.fromHexString("0x00000000000000000000000000000000000000bb");

  @TempDir Path tmp;

  @Test
  void writeGenesisSortsByKeccakAndSkipsZeroSlots() throws Exception {
    final GenesisAccount withSlots =
        new GenesisAccount(
            A,
            1L,
            Wei.of(1),
            Bytes.EMPTY,
            Map.of(UInt256.valueOf(7), UInt256.valueOf(4), UInt256.ZERO, UInt256.ZERO),
            null);
    final GenesisAccount contract =
        new GenesisAccount(B, 0L, Wei.of(2), Bytes.fromHexString("0x6000"), Map.of(), null);
    final Path path = tmp.resolve("genesis.pre");

    assertThat(Eip8347PreimageFile.writeGenesis(path, Stream.of(withSlots, contract))).isEqualTo(2);

    final List<AccountPreimages> expected =
        new ArrayList<>(
            List.of(
                new AccountPreimages(A, List.of(Bytes32.leftPad(UInt256.valueOf(7)))),
                new AccountPreimages(B, List.of())));
    expected.sort(Comparator.comparing(a -> a.addressHash().getBytes()));
    assertThat(readAll(path)).isEqualTo(expected);
  }

  @Test
  void rejectsSlotsOutOfKeccakOrder() throws Exception {
    final List<Bytes32> slots =
        new ArrayList<>(List.of(Bytes32.leftPad(Bytes.of(1)), Bytes32.leftPad(Bytes.of(2))));
    slots.sort(Comparator.comparing((Bytes32 slot) -> Hash.hash(slot).getBytes()).reversed());
    final Path path = tmp.resolve("unsorted-slots.pre");
    Eip8347Fixture.writePreimagesInGivenOrder(path, List.of(new AccountPreimages(A, slots)));

    assertThatThrownBy(() -> readAll(path))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("ascending by keccak256(slotKey)");
  }

  @Test
  void countsTheBytesReadFromTheFile() throws Exception {
    final Path path = tmp.resolve("count.pre");
    Eip8347PreimageFile.write(
        path,
        List.of(
            new AccountPreimages(A, List.of(Bytes32.leftPad(Bytes.of(1)))),
            new AccountPreimages(B, List.of())));

    try (final Eip8347PreimageFile reader = new Eip8347PreimageFile(path)) {
      assertThat(reader.bytesRead()).isZero();
      reader.batches(1).forEachRemaining(batch -> {});
      reader.ensureExhausted();
      assertThat(reader.bytesRead()).isEqualTo(Files.size(path));
    }
  }

  @Test
  void aSectionReadsTheRecordsBetweenTwoOffsets() throws Exception {
    final Path path = tmp.resolve("section.pre");
    Eip8347PreimageFile.write(
        path,
        List.of(
            new AccountPreimages(
                A, List.of(Bytes32.leftPad(Bytes.of(1)), Bytes32.leftPad(Bytes.of(2)))),
            new AccountPreimages(B, List.of()),
            new AccountPreimages(
                Address.fromHexString("0x00000000000000000000000000000000000000cc"),
                List.of(Bytes32.leftPad(Bytes.of(3))))));
    final List<AccountPreimages> records = readAll(path);
    final long second = Address.SIZE + Integer.BYTES + 32L * records.getFirst().slotKeys().size();

    try (final Eip8347PreimageFile section = Eip8347PreimageFile.section(path, 0, second)) {
      assertThat(read(section)).isEqualTo(records.subList(0, 1));
    }
    try (final Eip8347PreimageFile section =
        Eip8347PreimageFile.section(path, second, Files.size(path))) {
      assertThat(section.position()).isEqualTo(second);
      assertThat(read(section)).isEqualTo(records.subList(1, 3));
      assertThat(section.position()).isEqualTo(Files.size(path));
    }
  }

  @Test
  void rejectsTruncatedRecord() throws Exception {
    final Path path = tmp.resolve("truncated.pre");
    Files.write(path, new byte[] {1, 2, 3});

    assertThatThrownBy(() -> readAll(path))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("truncated");
  }

  private static List<AccountPreimages> readAll(final Path path) throws Exception {
    try (final Eip8347PreimageFile reader = new Eip8347PreimageFile(path)) {
      final List<AccountPreimages> records = read(reader);
      reader.ensureExhausted();
      return records;
    }
  }

  private static List<AccountPreimages> read(final Eip8347PreimageFile reader) throws Exception {
    final List<AccountPreimages> records = new ArrayList<>();
    Eip8347PreimageFile.Account account;
    while ((account = reader.nextAccount()) != null) {
      final List<Bytes32> slots = new ArrayList<>();
      for (int i = 0; i < account.slotCount(); i++) {
        slots.add(reader.nextSlot().key());
      }
      records.add(new AccountPreimages(account.address(), slots));
    }
    return records;
  }
}
