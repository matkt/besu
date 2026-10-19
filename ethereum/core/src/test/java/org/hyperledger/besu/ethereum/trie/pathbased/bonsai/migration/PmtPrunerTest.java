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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.FLAT_DB_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.FlatDbStrategyProvider.FLAT_DB_MODE;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class PmtPrunerTest {

  private static final long FORK_TIME = 1_000L;
  private static final Optional<Long> MILESTONE = Optional.of(FORK_TIME);

  private static final Bytes32 PMT_ROOT = Bytes32.repeat((byte) 0x11);
  private static final Bytes32 PBT_ROOT = Bytes32.repeat((byte) 0x22);
  private static final Bytes32 NODE_HASH = Bytes32.repeat((byte) 0x33);
  private static final Bytes NODE = Bytes.fromHexString("0xc0ffee");

  private final BlockHeader preFork =
      new BlockHeaderTestFixture().number(9).timestamp(FORK_TIME - 1).buildHeader();
  private final BlockHeader postFork =
      new BlockHeaderTestFixture()
          .number(10)
          .timestamp(FORK_TIME)
          .parentHash(preFork.getHash())
          .buildHeader();

  private final Blockchain blockchain = mock(Blockchain.class);
  private BonsaiWorldStateKeyValueStorage storage;
  private Map<Bytes, Bytes> patriciaMetadata;

  @BeforeEach
  void setUp() {
    storage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    when(blockchain.getBlockHeader(preFork.getHash())).thenReturn(Optional.of(preFork));
    when(blockchain.getBlockHeader(postFork.getHash())).thenReturn(Optional.of(postFork));
    when(blockchain.getChainHeadHeader()).thenReturn(postFork);
    when(blockchain.observeBlockAdded(any())).thenReturn(7L);

    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    // PMT at the last pre-fork block, with nodes whose keys share a prefix with its metadata keys.
    updater.saveWorldState(TrieBranchType.PATRICIA, preFork.getHash().getBytes(), PMT_ROOT, NODE);
    Stream.of(
            Bytes.of(0x01, 0x0f),
            Bytes.concatenate(Bytes32.repeat((byte) 0xff), Bytes.of(0x0a)),
            Bytes.concatenate(Bytes.wrap(FLAT_DB_MODE), Bytes.of(0x00)),
            Bytes.concatenate(Bytes.wrap(WORLD_BLOCK_NUMBER_KEY), Bytes.of(0x05)))
        .forEach(
            location -> updater.putTrieNode(TrieBranchType.PATRICIA, location, NODE_HASH, NODE));
    updater
        .getWorldStateTransaction()
        .put(
            TRIE_BRANCH_STORAGE,
            WORLD_BLOCK_NUMBER_KEY,
            Bytes.ofUnsignedLong(preFork.getNumber()).toArrayUnsafe());
    updater.putFlatDbBlockHash(postFork.getHash());
    // PBT at the first post-fork block, owned by the chain.
    updater.saveWorldState(TrieBranchType.BINARY, postFork.getHash().getBytes(), PBT_ROOT, NODE);
    updater.putTrieNode(TrieBranchType.BINARY, Bytes.of(0x01), NODE_HASH, NODE);
    updater.commit();
    storage.markPbtMigratorRetired();

    patriciaMetadata =
        Map.of(
            Bytes.wrap(FLAT_DB_MODE),
            Bytes.wrap(
                storage
                    .getComposedWorldStateStorage()
                    .get(TRIE_BRANCH_STORAGE, FLAT_DB_MODE)
                    .orElseThrow()),
            Bytes.wrap(WORLD_BLOCK_NUMBER_KEY),
            Bytes.ofUnsignedLong(preFork.getNumber()),
            Bytes.wrap(FLAT_DB_BLOCK_HASH_KEY),
            postFork.getHash().getBytes());
  }

  @Test
  void keepsThePmtWhileOnlyPreForkBlocksAreFinalized() {
    final Map<Bytes, Bytes> patriciaColumn = column(TRIE_BRANCH_STORAGE);
    finalizeBlock(preFork);
    final PmtPruner pruner = new PmtPruner(storage, blockchain, MILESTONE);

    assertThat(pruner.pruneIfForkFinalized()).isFalse();
    assertThat(pruner.isPmtPruned()).isFalse();
    assertThat(storage.isPmtPruned()).isFalse();
    assertThat(column(TRIE_BRANCH_STORAGE)).isEqualTo(patriciaColumn);
  }

  @Test
  void keepsThePmtUntilTheChainOwnsTheBinaryColumn() {
    storage.clearBinaryTrie();
    finalizeBlock(postFork);
    final PmtPruner pruner = new PmtPruner(storage, blockchain, MILESTONE);

    assertThat(pruner.pruneIfForkFinalized()).isFalse();
    assertThat(storage.getWorldStateRootHash(TrieBranchType.PATRICIA)).isPresent();
  }

  @Test
  void keepsThePmtWithoutPbtFork() {
    finalizeBlock(postFork);
    final PmtPruner pruner = new PmtPruner(storage, blockchain, Optional.empty());

    assertThat(pruner.pruneIfForkFinalized()).isFalse();
    assertThat(storage.getWorldStateRootHash(TrieBranchType.PATRICIA)).isPresent();
  }

  @Test
  void deletesThePmtAndKeepsTheFlatDbMetadataOnceAPostForkBlockIsFinalized() {
    final Map<Bytes, Bytes> binaryColumn = column(BINARY_TRIE_BRANCH_STORAGE);
    finalizeBlock(postFork);
    final PmtPruner pruner = new PmtPruner(storage, blockchain, MILESTONE);

    assertThat(pruner.pruneIfForkFinalized()).isTrue();

    assertThat(pruner.isPmtPruned()).isTrue();
    assertThat(storage.isPmtPruned()).isTrue();
    assertThat(column(TRIE_BRANCH_STORAGE)).isEqualTo(patriciaMetadata);
    assertThat(storage.getWorldStateRootHash(TrieBranchType.PATRICIA)).isEmpty();
    assertThat(storage.isWorldStateAvailable(TrieBranchType.PATRICIA, PMT_ROOT, preFork.getHash()))
        .isFalse();
    assertThat(storage.getWorldStateBlockNumber(TrieBranchType.PATRICIA))
        .contains(preFork.getNumber());
    // Only the marker is added to the binary column.
    assertThat(column(BINARY_TRIE_BRANCH_STORAGE))
        .containsAllEntriesOf(binaryColumn)
        .hasSize(binaryColumn.size() + 1);
  }

  @Test
  void deletesThePmtOnceAcrossChecksAndRestarts() {
    final BonsaiWorldStateKeyValueStorage spiedStorage = spy(storage);
    finalizeBlock(postFork);

    assertThat(new PmtPruner(spiedStorage, blockchain, MILESTONE).pruneIfForkFinalized()).isTrue();
    final PmtPruner afterRestart = new PmtPruner(spiedStorage, blockchain, MILESTONE);
    assertThat(afterRestart.isPmtPruned()).isTrue();
    assertThat(afterRestart.pruneIfForkFinalized()).isTrue();

    verify(spiedStorage, times(1)).deletePatriciaTrie();
    assertThat(column(TRIE_BRANCH_STORAGE)).isEqualTo(patriciaMetadata);
  }

  @Test
  void deletesAgainAfterACrashBeforeTheMarker() {
    finalizeBlock(postFork);
    // Crashed after the deletion, before the marker.
    storage.deletePatriciaTrie();

    final PmtPruner afterRestart = new PmtPruner(storage, blockchain, MILESTONE);
    assertThat(afterRestart.isPmtPruned()).isFalse();
    assertThat(afterRestart.pruneIfForkFinalized()).isTrue();

    assertThat(storage.isPmtPruned()).isTrue();
    assertThat(column(TRIE_BRANCH_STORAGE)).isEqualTo(patriciaMetadata);
  }

  @Test
  void checksAfterEachNewBlockUntilThePmtIsDeleted() {
    final BonsaiWorldStateKeyValueStorage spiedStorage = spy(storage);
    finalizeBlock(preFork);
    final PmtPruner pruner = new PmtPruner(spiedStorage, blockchain, MILESTONE);
    final ArgumentCaptor<BlockAddedObserver> observer =
        ArgumentCaptor.forClass(BlockAddedObserver.class);

    pruner.start(MoreExecutors.newDirectExecutorService());
    verify(blockchain).observeBlockAdded(observer.capture());
    observer.getValue().onBlockAdded(null);
    assertThat(pruner.isPmtPruned()).isFalse();

    finalizeBlock(postFork);
    observer.getValue().onBlockAdded(null);
    observer.getValue().onBlockAdded(null);

    assertThat(pruner.isPmtPruned()).isTrue();
    verify(spiedStorage, times(1)).deletePatriciaTrie();
    verify(blockchain).removeObserver(7L);
  }

  @Test
  void restartAfterTheDeletionDoesNotWatchBlocks() {
    finalizeBlock(postFork);
    new PmtPruner(storage, blockchain, MILESTONE).pruneIfForkFinalized();
    final ExecutorService executor = MoreExecutors.newDirectExecutorService();

    new PmtPruner(storage, blockchain, MILESTONE).start(executor);

    verify(blockchain, never()).observeBlockAdded(any());
    assertThat(executor.isShutdown()).isTrue();
  }

  @Test
  void providerRefusesPreForkStatesOnceThePmtIsDeleted() {
    finalizeBlock(postFork);
    new PmtPruner(storage, blockchain, MILESTONE).pruneIfForkFinalized();

    final BonsaiWorldStateProvider provider =
        new BonsaiWorldStateProvider(
            storage,
            blockchain,
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG.getExtraStorageConfiguration(),
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            new BonsaiCodeCache(),
            Optional.empty(),
            MILESTONE);

    assertThat(provider.getPmtPruner().isPmtPruned()).isTrue();
    assertThat(provider.getWorldState(withBlockHeaderAndNoUpdateNodeHead(preFork))).isEmpty();
    assertThat(provider.isWorldStateAvailable(preFork.getStateRoot(), preFork.getHash())).isFalse();
    assertThat(provider.isWorldStateAvailable(postFork.getStateRoot(), postFork.getHash()))
        .isTrue();
  }

  private void finalizeBlock(final BlockHeader header) {
    when(blockchain.getFinalized()).thenReturn(Optional.of(header.getHash()));
  }

  private Map<Bytes, Bytes> column(final SegmentIdentifier segment) {
    try (final Stream<Pair<byte[], byte[]>> entries =
        storage.getComposedWorldStateStorage().stream(segment)) {
      return entries.collect(
          Collectors.toMap(
              entry -> Bytes.wrap(entry.getKey()), entry -> Bytes.wrap(entry.getValue())));
    }
  }
}
