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
package org.hyperledger.besu.ethereum.trie.patricia;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.ethereum.trie.KeyValueMerkleStorage;
import org.hyperledger.besu.ethereum.trie.MerkleStorage;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Checks that the load phase resolves the update paths with batched reads only. */
class ParallelStoredMerklePatriciaTrieLoadPhaseTest {

  private final ForkJoinPool pool = new ForkJoinPool(4);
  private final ExecutorService ioExecutor = Executors.newFixedThreadPool(4);

  private MerkleStorage storage;
  private Bytes32 root;

  /** Loader counting single-node and batched reads. */
  private final AtomicInteger singleReads = new AtomicInteger();

  private final AtomicInteger batchReads = new AtomicInteger();
  private final AtomicInteger batchedNodes = new AtomicInteger();

  private final NodeLoader countingLoader =
      new NodeLoader() {
        @Override
        public Optional<Bytes> getNode(final Bytes location, final Bytes32 hash) {
          singleReads.incrementAndGet();
          return storage.get(location, hash);
        }

        @Override
        public List<Optional<Bytes>> getNodes(
            final List<Bytes> locations, final List<Bytes32> hashes) {
          batchReads.incrementAndGet();
          batchedNodes.addAndGet(locations.size());
          final List<Optional<Bytes>> nodes = new ArrayList<>(locations.size());
          for (int i = 0; i < locations.size(); i++) {
            nodes.add(storage.get(locations.get(i), hashes.get(i)));
          }
          return nodes;
        }
      };

  @BeforeEach
  void setUp() {
    storage = new KeyValueMerkleStorage(new InMemoryKeyValueStorage());
    final MerkleTrie<Bytes, Bytes> base =
        new StoredMerklePatriciaTrie<>(storage::get, Function.identity(), Function.identity());
    for (int i = 0; i < 5_000; i++) {
      base.put(key(i), value(i, 1));
    }
    base.commit(storage::put);
    storage.commit();
    root = base.getRootHash();
  }

  @AfterEach
  void tearDown() {
    pool.shutdownNow();
    ioExecutor.shutdownNow();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void updatesAndInsertsOnlyUseBatchedReads(final boolean dedicatedIoExecutor) {
    final MerkleTrie<Bytes, Bytes> trie = parallelTrie(dedicatedIoExecutor);
    final MerkleTrie<Bytes, Bytes> reference = sequentialTrie();
    for (int i = 0; i < 5_000; i += 7) {
      trie.put(key(i), value(i, 2));
      reference.put(key(i), value(i, 2));
    }
    for (int i = 10_000; i < 10_200; i++) {
      trie.put(key(i), value(i, 2));
      reference.put(key(i), value(i, 2));
    }

    assertThat(trie.getRootHash()).isEqualTo(reference.getRootHash());
    // Only the root is read on its own; everything below it is loaded level by level.
    assertThat(singleReads.get()).isEqualTo(1);
    assertThat(batchReads.get()).isLessThan(batchedNodes.get() / 10);
  }

  @ParameterizedTest
  @ValueSource(ints = {3, 50})
  void removalsAlsoLoadCollapsingSiblings(final int step) {
    final MerkleTrie<Bytes, Bytes> trie = parallelTrie(true);
    final MerkleTrie<Bytes, Bytes> reference = sequentialTrie();
    for (int i = 0; i < 5_000; i += step) {
      trie.remove(key(i));
      reference.remove(key(i));
    }

    assertThat(trie.getRootHash()).isEqualTo(reference.getRootHash());
    assertThat(singleReads.get()).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void commitMatchesSequentialTrie(final boolean dedicatedIoExecutor) {
    final MerkleTrie<Bytes, Bytes> trie = parallelTrie(dedicatedIoExecutor);
    final MerkleTrie<Bytes, Bytes> reference = sequentialTrie();
    for (int i = 0; i < 5_000; i += 5) {
      if (i % 3 == 0) {
        trie.remove(key(i));
        reference.remove(key(i));
      } else {
        trie.put(key(i), value(i, 3));
        reference.put(key(i), value(i, 3));
      }
    }
    trie.commit((location, hash, value) -> {});
    reference.commit((location, hash, value) -> {});
    assertThat(trie.getRootHash()).isEqualTo(reference.getRootHash());
  }

  private MerkleTrie<Bytes, Bytes> parallelTrie(final boolean dedicatedIoExecutor) {
    return dedicatedIoExecutor
        ? new ParallelStoredMerklePatriciaTrie<>(
            countingLoader, root, Function.identity(), Function.identity(), pool, ioExecutor)
        : new ParallelStoredMerklePatriciaTrie<>(
            countingLoader, root, Function.identity(), Function.identity(), pool);
  }

  private MerkleTrie<Bytes, Bytes> sequentialTrie() {
    return new StoredMerklePatriciaTrie<>(
        storage::get, root, Function.identity(), Function.identity());
  }

  private static Bytes key(final int i) {
    return Hash.keccak256(Bytes.ofUnsignedInt(i));
  }

  private static Bytes value(final int i, final int version) {
    return Bytes.concatenate(Bytes.ofUnsignedInt(i), Bytes.ofUnsignedInt(version));
  }
}
