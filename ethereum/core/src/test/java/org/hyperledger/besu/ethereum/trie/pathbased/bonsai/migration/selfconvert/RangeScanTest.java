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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.selfconvert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.selfconvert.FlatState.RANGES;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile.AccountPreimages;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RangeScanTest {

  @TempDir Path tmp;

  @Test
  void eachRangeStartsAtItsFirstRecord() throws Exception {
    // Three strides, accounts with no slot to two.
    final Path file = tmp.resolve("preimages");
    Eip8347PreimageFile.write(
        file,
        IntStream.range(0, 3000)
            .mapToObj(
                i ->
                    new AccountPreimages(
                        Address.fromHexString(String.format("0x%040x", i)),
                        IntStream.range(0, i % 3)
                            .mapToObj(slot -> Bytes32.leftPad(Bytes.of(slot)))
                            .toList()))
            .toList());

    assertThat(RangeScan.starts(file)).containsExactly(startsReadRecordByRecord(file));
  }

  @Test
  void everyRangeOfAnEmptyFileIsEmpty() throws Exception {
    final Path file = tmp.resolve("empty");
    Eip8347PreimageFile.write(file, List.of());

    assertThat(RangeScan.starts(file)).containsOnly(0L).hasSize(RANGES + 1);
  }

  private static long[] startsReadRecordByRecord(final Path path) throws IOException {
    final long[] starts = new long[RANGES + 1];
    Arrays.fill(starts, Files.size(path));
    try (final Eip8347PreimageFile file = new Eip8347PreimageFile(path)) {
      int nextRange = 0;
      long offset = file.position();
      for (Eip8347PreimageFile.Account account = file.nextAccount();
          account != null;
          account = file.nextAccount()) {
        final int range = FlatState.rangeOf(account.addressHash().getBytes().toArrayUnsafe());
        while (nextRange <= range) {
          starts[nextRange++] = offset;
        }
        file.skipSlots();
        offset = file.position();
      }
    }
    return starts;
  }
}
