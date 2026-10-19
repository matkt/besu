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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class PbtColumnOwnershipTest {

  private final BonsaiWorldStateKeyValueStorage storage =
      new BonsaiWorldStateKeyValueStorage(
          new InMemoryKeyValueStorageProvider(),
          new NoOpMetricsSystem(),
          DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  private final PbtColumnOwnership ownership = new PbtColumnOwnership(storage);

  @Test
  void migratorWritesUntilTheChainClaimsTheColumn() {
    final List<String> writes = new ArrayList<>();

    assertThat(ownership.writeAsMigrator(() -> writes.add("before claim"))).isTrue();
    ownership.claimForChain();
    assertThat(ownership.writeAsMigrator(() -> writes.add("after claim"))).isFalse();

    assertThat(writes).containsExactly("before claim");
    assertThat(ownership.isClaimedByChain()).isTrue();
    assertThat(storage.isPbtMigratorRetired()).isTrue();
  }
}
