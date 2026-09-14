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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateLayerStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BonsaiHeadLayerManagerTest {

  private static final long MEMORY_BUDGET_BYTES = 64L * 1024L * 1024L;

  private BonsaiWorldStateKeyValueStorage rootStorage;
  private BonsaiHeadLayerManager manager;
  private final BlockHeaderTestFixture headerFixture = new BlockHeaderTestFixture();

  @BeforeEach
  void setUp() throws Exception {
    rootStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    manager = new BonsaiHeadLayerManager(rootStorage, 32, MEMORY_BUDGET_BYTES);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (rootStorage != null) {
      rootStorage.close();
    }
  }

  @Test
  void registerAndPromoteLinearChainExtendsCanonicalWindow() throws Exception {
    final BlockHeader genesis = headerFixture.number(0).buildHeader();
    rootStorage.updater().commit();

    final BlockHeader block1 = childHeader(genesis, 1);
    registerLayerForHeader(block1);
    assertThat(manager.hasCandidate(block1.getBlockHash())).isTrue();

    assertThat(manager.promote(block1)).isPresent();
    assertThat(manager.getPromotionCount()).isEqualTo(1);
    assertThat(manager.getCanonicalWindowDepth()).isEqualTo(1);

    final BlockHeader block2 = childHeader(block1, 2);
    registerLayerForHeader(block2);
    assertThat(manager.promote(block2)).isPresent();
    assertThat(manager.getPromotionCount()).isEqualTo(2);
    assertThat(manager.getCanonicalWindowDepth()).isEqualTo(2);
  }

  @Test
  void siblingCandidatesRemainRegisteredUntilUnrelatedForkIsPruned() throws Exception {
    final BlockHeader genesis = headerFixture.number(0).buildHeader();
    rootStorage.updater().commit();

    final BlockHeader forkA =
        headerFixture.number(1).parentHash(genesis.getHash()).timestamp(1).buildHeader();
    final BlockHeader forkB =
        headerFixture.number(1).parentHash(genesis.getHash()).timestamp(2).buildHeader();

    registerLayerForHeader(forkA);
    registerLayerForHeader(forkB);
    assertThat(manager.hasCandidate(forkA.getBlockHash())).isTrue();
    assertThat(manager.hasCandidate(forkB.getBlockHash())).isTrue();

    assertThat(manager.promote(forkA)).isPresent();
    assertThat(manager.hasCandidate(forkB.getBlockHash())).isTrue();

    assertThat(manager.promote(forkB)).isPresent();
    assertThat(manager.getPromotionCount()).isEqualTo(2);
  }

  @Test
  void checkpointAtIntervalOneFlushesAndClearsWindow() throws Exception {
    manager = new BonsaiHeadLayerManager(rootStorage, 1, MEMORY_BUDGET_BYTES);

    final BlockHeader genesis = headerFixture.number(0).buildHeader();
    final var genesisUpdater = rootStorage.updater();
    genesisUpdater
        .getWorldStateTransaction()
        .put(
            org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
            org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage
                .WORLD_CHECKPOINT_NUMBER_KEY,
            Bytes.ofUnsignedLong(0).toArrayUnsafe());
    genesisUpdater
        .getWorldStateTransaction()
        .put(
            org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
            org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage
                .WORLD_CHECKPOINT_HASH_KEY,
            genesis.getBlockHash().getBytes().toArrayUnsafe());
    genesisUpdater.commit();

    final BlockHeader block1 = childHeader(genesis, 1);
    registerLayerForHeader(block1);
    assertThat(manager.promote(block1)).isPresent();

    assertThat(manager.getCheckpointCount()).isEqualTo(1);
    assertThat(manager.getCanonicalWindowDepth()).isZero();
    assertThat(manager.getEstimatedWindowBytes()).isZero();
    assertThat(manager.getCheckpointNumber()).isEqualTo(block1.getNumber());
    assertThat(manager.getCheckpointHash()).isEqualTo(block1.getBlockHash());
  }

  private BlockHeader childHeader(final BlockHeader parent, final long number) {
    return headerFixture
        .number(number)
        .parentHash(parent.getBlockHash())
        .stateRoot(Hash.EMPTY_TRIE_HASH)
        .buildHeader();
  }

  private void registerLayerForHeader(final BlockHeader blockHeader) throws Exception {
    try (BonsaiWorldStateLayerStorage writable = new BonsaiWorldStateLayerStorage(rootStorage)) {
      writable
          .updater()
          .putAccountInfoState(Hash.hash(Bytes.of((byte) blockHeader.getNumber())), Bytes.of(1, 2, 3))
          .commit();
      manager.registerCandidate(blockHeader, writable.clone());
    }
  }
}
