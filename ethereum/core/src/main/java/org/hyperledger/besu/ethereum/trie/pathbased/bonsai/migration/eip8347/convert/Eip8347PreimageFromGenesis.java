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

import org.hyperledger.besu.config.GenesisAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactWriter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageRecord;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Builds an EIP-8347 preimage file from a genesis {@code alloc}: one record per account, slot keys
 * from that account's storage map. Ordering matches the artifact layout (keccak of address / slot).
 *
 * <p>Used when the client has plain keys at genesis but no separate keccak preimage store (Bonsai).
 */
public final class Eip8347PreimageFromGenesis {

  private Eip8347PreimageFromGenesis() {}

  /**
   * Writes preimage records derived from {@code allocations}.
   *
   * @param path output path
   * @param allocations genesis accounts (typically {@code GenesisConfig#streamAllocations()})
   * @return number of records written
   */
  public static int write(final Path path, final Stream<GenesisAccount> allocations)
      throws IOException {
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(allocations, "allocations");
    final List<Eip8347PreimageRecord> records = new ArrayList<>();
    allocations.forEach(
        account -> {
          final List<Bytes32> slots = new ArrayList<>();
          for (final Map.Entry<UInt256, UInt256> e : account.storage().entrySet()) {
            if (UInt256.ZERO.equals(e.getValue())) {
              continue;
            }
            slots.add(Bytes32.leftPad(e.getKey()));
          }
          records.add(new Eip8347PreimageRecord(account.address(), slots));
        });
    Eip8347ArtifactWriter.writePreimages(path, records);
    return records.size();
  }
}
