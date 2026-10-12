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
package org.hyperledger.besu.ethereum.trie.forest.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.TransactionDBRocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Forest storage on the RocksDB TransactionDB it uses in production, with pessimistic locks. */
class ForestWorldStateKeyValueStorageRocksDBTest {

  @TempDir private Path dataDir;

  @Test
  void openUpdatersWritingTheSameNodeDoNotBlockEachOther() throws Exception {
    try (final SegmentedKeyValueStorage database = openTransactionDb()) {
      final ForestWorldStateKeyValueStorage storage = forestStorage(database);
      final Bytes node = Bytes.of(1, 2, 3);
      final Bytes32 nodeHash = nodeHash(node);

      // two concurrent writers of the same content-addressed node, like the snap sync persist steps
      final ForestWorldStateKeyValueStorage.Updater first = storage.updater();
      first.putAccountStorageTrieNode(nodeHash, node);
      final ForestWorldStateKeyValueStorage.Updater second = storage.updater();

      final long start = System.nanoTime();
      assertThatCode(() -> second.putAccountStorageTrieNode(nodeHash, node))
          .doesNotThrowAnyException();
      assertThat(System.nanoTime() - start).isLessThan(500_000_000L);

      second.commit();
      first.commit();
      assertThat(storage.getAccountStorageTrieNode(nodeHash)).contains(node);
    }
  }

  @Test
  void commitsDoNotWaitForEachOther() throws Exception {
    try (final SegmentedKeyValueStorage database = openTransactionDb()) {
      final ForestWorldStateKeyValueStorage storage = forestStorage(database);
      final CountDownLatch firstCommitInProgress = new CountDownLatch(1);
      final CountDownLatch releaseFirstCommit = new CountDownLatch(1);
      final AtomicBoolean isFirstCommit = new AtomicBoolean(true);
      // holds the first commit open from inside it, once its writes are applied
      storage.addNodeAddedListener(
          nodes -> {
            if (isFirstCommit.getAndSet(false)) {
              firstCommitInProgress.countDown();
              try {
                releaseFirstCommit.await(10, TimeUnit.SECONDS);
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
          });
      final Bytes firstNode = Bytes.of(1);
      final Bytes secondNode = Bytes.of(2);
      final ForestWorldStateKeyValueStorage.Updater first = storage.updater();
      first.putAccountStorageTrieNode(nodeHash(firstNode), firstNode);
      final ForestWorldStateKeyValueStorage.Updater second = storage.updater();
      second.putAccountStorageTrieNode(nodeHash(secondNode), secondNode);

      final CompletableFuture<Void> firstCommit = CompletableFuture.runAsync(first::commit);
      try {
        assertThat(firstCommitInProgress.await(5, TimeUnit.SECONDS)).isTrue();
        CompletableFuture.runAsync(second::commit).get(5, TimeUnit.SECONDS);
      } finally {
        releaseFirstCommit.countDown();
      }
      firstCommit.get(5, TimeUnit.SECONDS);

      assertThat(storage.getAccountStorageTrieNode(nodeHash(firstNode))).contains(firstNode);
      assertThat(storage.getAccountStorageTrieNode(nodeHash(secondNode))).contains(secondNode);
    }
  }

  @Test
  void concurrentCommitsOfSharedNodesWrittenInOppositeOrderComplete() throws Exception {
    try (final SegmentedKeyValueStorage database = openTransactionDb()) {
      final ForestWorldStateKeyValueStorage storage = forestStorage(database);
      final List<Bytes> nodes = IntStream.range(0, 2_000).mapToObj(Bytes::ofUnsignedInt).toList();

      for (int round = 0; round < 10; round++) {
        final ForestWorldStateKeyValueStorage.Updater forward = storage.updater();
        nodes.forEach(node -> forward.putAccountStorageTrieNode(nodeHash(node), node));
        final ForestWorldStateKeyValueStorage.Updater backward = storage.updater();
        nodes.reversed().forEach(node -> backward.putAccountStorageTrieNode(nodeHash(node), node));

        CompletableFuture.allOf(
                CompletableFuture.runAsync(forward::commit),
                CompletableFuture.runAsync(backward::commit))
            .get(30, TimeUnit.SECONDS);
      }

      nodes.forEach(
          node -> assertThat(storage.getAccountStorageTrieNode(nodeHash(node))).contains(node));
    }
  }

  private SegmentedKeyValueStorage openTransactionDb() {
    return new TransactionDBRocksDBColumnarKeyValueStorage(
        new RocksDBConfigurationBuilder().databaseDir(dataDir).build(),
        List.of(KeyValueSegmentIdentifier.DEFAULT, KeyValueSegmentIdentifier.WORLD_STATE),
        List.of(),
        new NoOpMetricsSystem(),
        RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
  }

  private static ForestWorldStateKeyValueStorage forestStorage(
      final SegmentedKeyValueStorage database) {
    return new ForestWorldStateKeyValueStorage(
        new SegmentedKeyValueStorageAdapter(KeyValueSegmentIdentifier.WORLD_STATE, database));
  }

  private static Bytes32 nodeHash(final Bytes node) {
    return Bytes32.wrap(Hash.hash(node).getBytes());
  }
}
