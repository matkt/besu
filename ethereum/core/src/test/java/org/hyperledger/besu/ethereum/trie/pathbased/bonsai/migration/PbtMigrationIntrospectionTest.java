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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.BINARY_TRIE_BRANCH_STORAGE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

/** What debug_migrationProgress reports before the binary column has a cursor, and after. */
class PbtMigrationIntrospectionTest {

  private static final Optional<Long> PBT_TIME = Optional.of(Long.MAX_VALUE);

  private final BonsaiWorldStateKeyValueStorage storage =
      (BonsaiWorldStateKeyValueStorage)
          new InMemoryKeyValueStorageProvider()
              .createWorldStateStorage(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  private final BlockHeader head = new BlockHeaderTestFixture().number(5).buildHeader();
  private final Blockchain blockchain = mock(Blockchain.class);

  PbtMigrationIntrospectionTest() {
    when(blockchain.getChainHeadHeader()).thenReturn(head);
    when(blockchain.getFinalized()).thenReturn(Optional.empty());
    when(blockchain.getBlockHeader(head.getHash())).thenReturn(Optional.of(head));
  }

  @Test
  void aBootstrapWritingTheColumnIsImporting() {
    assertThat(progress().getPhase()).isEqualTo("running");

    put(BonsaiWorldStateKeyValueStorage.PBT_IMPORTING_KEY, new byte[] {1});

    assertThat(progress().getPhase()).isEqualTo("importing");
  }

  @Test
  void aRefusedBootstrapIsImportFailedWithItsReason() {
    put(BonsaiWorldStateKeyValueStorage.PBT_IMPORTING_KEY, new byte[] {1});
    put(
        BonsaiWorldStateKeyValueStorage.PBT_IMPORT_FAILED_KEY,
        "no preimage for flat key 0x01".getBytes(StandardCharsets.UTF_8));

    final MigrationProgressResult progress = progress();
    assertThat(progress.getPhase()).isEqualTo("importFailed");
    assertThat(progress.getBinary().getError()).isEqualTo("no preimage for flat key 0x01");
  }

  @Test
  void theCursorEndsTheImport() {
    put(BonsaiWorldStateKeyValueStorage.PBT_IMPORTING_KEY, new byte[] {1});
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    PbtMigrator.putCursor(updater, Bytes32.ZERO, head.getHash());
    updater.commit();

    assertThat(progress().getPhase()).isEqualTo("running");
    assertThat(progress().getBinary().getPhase()).isEqualTo("synced");
    assertThat(progress().getBinary().getCursorHash()).isEqualTo(head.getHash().toHexString());
  }

  private void put(final byte[] key, final byte[] value) {
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    updater.getWorldStateTransaction().put(BINARY_TRIE_BRANCH_STORAGE, key, value);
    updater.commit();
  }

  private MigrationProgressResult progress() {
    return PbtMigrationIntrospection.progress(storage, blockchain, PBT_TIME);
  }
}
