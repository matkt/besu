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

import org.hyperledger.besu.config.GenesisAccount;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageReader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageRecord;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Eip8347PreimageFromGenesisTest {

  @TempDir Path tmp;

  @Test
  void writesRecordsInKeccakAddressOrderSkippingZeroSlots() throws Exception {
    final Address a = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final Address b = Address.fromHexString("0x00000000000000000000000000000000000000bb");
    final UInt256 slot7 = UInt256.valueOf(7);
    final UInt256 slot0 = UInt256.ZERO;

    final GenesisAccount acctA =
        new GenesisAccount(
            a,
            1L,
            Wei.of(1),
            Bytes.EMPTY,
            Map.of(slot7, UInt256.valueOf(4), slot0, UInt256.ZERO),
            null);
    final GenesisAccount acctB =
        new GenesisAccount(b, 0L, Wei.of(2), Bytes.fromHexString("0x6000"), Map.of(), null);

    final Path out = tmp.resolve("preimages.bin");
    final int n = Eip8347PreimageFromGenesis.write(out, Stream.of(acctA, acctB));
    assertThat(n).isEqualTo(2);

    try (final Eip8347PreimageReader reader = new Eip8347PreimageReader(out)) {
      final Iterator<Eip8347PreimageRecord> it = reader.iterator();
      final Eip8347PreimageRecord first = it.next();
      final Eip8347PreimageRecord second = it.next();
      assertThat(it.hasNext()).isFalse();
      // keccak order, not raw address order
      assertThat(first.addressHash().compareTo(second.addressHash())).isLessThan(0);
      final List<Address> addrs = List.of(first.address(), second.address());
      assertThat(addrs).containsExactlyInAnyOrder(a, b);
      final Eip8347PreimageRecord withSlots = first.address().equals(a) ? first : second;
      assertThat(withSlots.slotKeys()).containsExactly(Bytes32.leftPad(slot7));
    }
  }
}
