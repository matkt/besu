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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_LOG_STORAGE;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MigrationScopedWorldStateKeyValueStorageTest {

  private final KeyValueStorageTransaction trieLogTransaction =
      mock(KeyValueStorageTransaction.class);
  private MigrationScopedWorldStateKeyValueStorage storage;

  @BeforeEach
  void setUp() {
    final InMemoryKeyValueStorageProvider provider = spy(new InMemoryKeyValueStorageProvider());
    final KeyValueStorage trieLogStorage = mock(KeyValueStorage.class);
    when(trieLogStorage.startTransaction()).thenReturn(trieLogTransaction);
    doReturn(trieLogStorage).when(provider).getStorageBySegmentIdentifier(TRIE_LOG_STORAGE);
    storage =
        new MigrationScopedWorldStateKeyValueStorage(
            new BonsaiWorldStateKeyValueStorage(
                provider, new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG));
  }

  @Test
  void commitClosesTheUnusedTrieLogTransaction() {
    storage.updater().commit();

    verify(trieLogTransaction).close();
    verify(trieLogTransaction, never()).commit();
  }

  @Test
  void rollbackRollsBackTheUnusedTrieLogTransaction() {
    storage.updater().rollback();

    verify(trieLogTransaction).rollback();
  }
}
