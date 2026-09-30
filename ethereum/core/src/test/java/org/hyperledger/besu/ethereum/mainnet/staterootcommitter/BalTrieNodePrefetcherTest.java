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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.Hash;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.BalTrieNodePrefetcher.KeyPath;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.BalTrieNodePrefetcher.Result;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.BalTrieNodePrefetcher.TrieWalk;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeStrategy.TrieNodeRequest;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BalTrieNodePrefetcherTest {

  private static final Bytes32 ACCOUNT_A = Bytes32.fromHexStringLenient("0xaa");
  private static final Bytes32 ACCOUNT_B = Bytes32.fromHexStringLenient("0xbb");

  /** Path-keyed node store, like Bonsai's trie branch segment. */
  private final Map<Bytes, Bytes> store = new ConcurrentHashMap<>();

  private final AtomicInteger storageReads = new AtomicInteger();
  private final ExecutorService executor = Executors.newFixedThreadPool(4);

  @AfterEach
  void tearDown() {
    executor.shutdownNow();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 7, 64})
  void prefetchCoversEveryNodeTheUpdateReads(final int batchSize) {
    final Bytes32 root = createTrie(null, 2_000, 1);
    final List<Bytes32> updated = keys(0, 2_000, 13); // existing keys
    final List<Bytes32> inserted = keys(5_000, 5_050, 1); // new keys

    final List<KeyPath> paths = new ArrayList<>();
    updated.forEach(k -> paths.add(KeyPath.of(k, false)));
    inserted.forEach(k -> paths.add(KeyPath.of(k, false)));

    final Result result =
        BalTrieNodePrefetcher.walk(
            List.of(new TrieWalk(null, root, paths)), this::load, batchSize, executor);

    assertThat(result.nodes()).isNotEmpty();
    // One round per trie level, not one per node.
    assertThat(result.rounds()).isLessThanOrEqualTo(10);

    storageReads.set(0);
    final MerkleTrie<Bytes, Bytes> prefetched = trie(null, root, result.nodes());
    updated.forEach(k -> prefetched.put(k, value(k, 2)));
    inserted.forEach(k -> prefetched.put(k, value(k, 2)));
    final Bytes32 prefetchedRoot = prefetched.getRootHash();
    assertThat(storageReads.get()).isZero();

    final MerkleTrie<Bytes, Bytes> reference = trie(null, root, Map.of());
    updated.forEach(k -> reference.put(k, value(k, 2)));
    inserted.forEach(k -> reference.put(k, value(k, 2)));
    assertThat(prefetchedRoot).isEqualTo(reference.getRootHash());
  }

  @Test
  void removalsPrefetchCollapseSiblings() {
    final Bytes32 root = createTrie(null, 300, 1);
    final List<Bytes32> removed = keys(0, 300, 3);

    final List<KeyPath> paths = new ArrayList<>();
    removed.forEach(k -> paths.add(KeyPath.of(k, true)));
    final Result result =
        BalTrieNodePrefetcher.walk(
            List.of(new TrieWalk(null, root, paths)), this::load, 16, executor);

    storageReads.set(0);
    final MerkleTrie<Bytes, Bytes> prefetched = trie(null, root, result.nodes());
    removed.forEach(prefetched::remove);
    final Bytes32 prefetchedRoot = prefetched.getRootHash();
    assertThat(storageReads.get()).isZero();

    final MerkleTrie<Bytes, Bytes> reference = trie(null, root, Map.of());
    removed.forEach(reference::remove);
    assertThat(prefetchedRoot).isEqualTo(reference.getRootHash());
  }

  @Test
  void walksStorageTriesOfSeveralAccountsInTheSameRounds() {
    final Bytes32 rootA = createTrie(ACCOUNT_A, 500, 1);
    final Bytes32 rootB = createTrie(ACCOUNT_B, 500, 3);
    final List<KeyPath> pathsA = new ArrayList<>();
    final List<KeyPath> pathsB = new ArrayList<>();
    keys(0, 500, 11).forEach(k -> pathsA.add(KeyPath.of(k, false)));
    keys(0, 500, 17).forEach(k -> pathsB.add(KeyPath.of(k, false)));

    final Result result =
        BalTrieNodePrefetcher.walk(
            List.of(
                new TrieWalk(org.hyperledger.besu.datatypes.Hash.wrap(ACCOUNT_A), rootA, pathsA),
                new TrieWalk(org.hyperledger.besu.datatypes.Hash.wrap(ACCOUNT_B), rootB, pathsB)),
            this::load,
            8,
            executor);

    storageReads.set(0);
    final MerkleTrie<Bytes, Bytes> trieA = trie(ACCOUNT_A, rootA, result.nodes());
    final MerkleTrie<Bytes, Bytes> trieB = trie(ACCOUNT_B, rootB, result.nodes());
    pathsA.forEach(p -> trieA.put(keyOf(p), Bytes.of(1)));
    pathsB.forEach(p -> trieB.put(keyOf(p), Bytes.of(2)));
    trieA.getRootHash();
    trieB.getRootHash();
    assertThat(storageReads.get()).isZero();
  }

  @Test
  void missingNodesAreSkipped() {
    final Bytes32 root = createTrie(null, 200, 1);
    store.clear();
    final Result result =
        BalTrieNodePrefetcher.walk(
            List.of(new TrieWalk(null, root, List.of(KeyPath.of(keys(0, 1, 1).get(0), false)))),
            this::load,
            0,
            executor);
    assertThat(result.nodes()).isEmpty();
    assertThat(result.rounds()).isEqualTo(1);
  }

  private List<Optional<Bytes>> load(final List<TrieNodeRequest> requests) {
    final List<Optional<Bytes>> results = new ArrayList<>(requests.size());
    for (final TrieNodeRequest request : requests) {
      final Bytes key =
          request.isAccountTrie()
              ? request.location()
              : Bytes.concatenate(request.accountHash().getBytes(), request.location());
      results.add(
          Optional.ofNullable(store.get(key))
              .filter(b -> Hash.keccak256(b).equals(request.nodeHash())));
    }
    return results;
  }

  private Bytes32 createTrie(final Bytes32 account, final int size, final int valueSeed) {
    final MerkleTrie<Bytes, Bytes> trie =
        new StoredMerklePatriciaTrie<>(
            (location, hash) -> Optional.empty(), Function.identity(), Function.identity());
    keys(0, size, 1).forEach(k -> trie.put(k, value(k, valueSeed)));
    trie.commit((location, hash, value) -> store.put(storageKey(account, location), value));
    return trie.getRootHash();
  }

  /** Trie serving {@code prefetched} first and counting reads that reach the store. */
  private MerkleTrie<Bytes, Bytes> trie(
      final Bytes32 account, final Bytes32 root, final Map<Bytes32, Bytes> prefetched) {
    final NodeLoader loader =
        (location, hash) -> {
          final Bytes node = prefetched.get(hash);
          if (node != null) {
            return Optional.of(node);
          }
          storageReads.incrementAndGet();
          return Optional.ofNullable(store.get(storageKey(account, location)));
        };
    return new StoredMerklePatriciaTrie<>(loader, root, Function.identity(), Function.identity());
  }

  private static Bytes storageKey(final Bytes32 account, final Bytes location) {
    return account == null ? location : Bytes.concatenate(account, location);
  }

  private static List<Bytes32> keys(final int from, final int to, final int step) {
    final List<Bytes32> keys = new ArrayList<>();
    for (int i = from; i < to; i += step) {
      keys.add(Hash.keccak256(Bytes.ofUnsignedInt(i)));
    }
    return keys;
  }

  private static Bytes value(final Bytes32 key, final int seed) {
    return Bytes.concatenate(key.slice(0, 8), Bytes.ofUnsignedInt(seed));
  }

  private static Bytes32 keyOf(final KeyPath path) {
    return Bytes32.wrap(CompactEncoding.pathToBytes(path.path()));
  }
}
