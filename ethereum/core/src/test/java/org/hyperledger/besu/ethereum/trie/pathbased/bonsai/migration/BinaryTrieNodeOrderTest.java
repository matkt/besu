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

import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.TrieNodeCodec;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.AscendingCollapseBinaryTrie;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/** The location order {@link SnapshotColumnWriter} relies on: {@code 0x00 ‖ path bits ‖ 1}. */
class BinaryTrieNodeOrderTest {

  private static final Comparator<Bytes> LOCATION_ORDER =
      (a, b) -> Arrays.compareUnsigned(a.toArrayUnsafe(), b.toArrayUnsafe());

  @Test
  void stemsSortByLocationInDepthFirstOrder() {
    final Random random = new Random(8347);
    for (int tree = 0; tree < 200; tree++) {
      final List<Bytes> stems = new ArrayList<>();
      final List<Bytes> nodes = new ArrayList<>();
      subtree(random, Bytes.EMPTY, 0, stems, nodes);

      assertThat(stems).isSortedAccordingTo(LOCATION_ORDER).doesNotHaveDuplicates();
      assertThat(nodes).doesNotHaveDuplicates();
    }
  }

  @Test
  void branchesSortByTheFirstBitOfTheirPrefix() {
    final Random random = new Random(7);
    for (int tree = 0; tree < 200; tree++) {
      final Bytes branch = TrieNodeCodec.childLocation(Bytes.EMPTY, randomBits(random), 5, 1);
      final byte[] prefix = randomBits(random);
      final int prefixLength = random.nextInt(9);
      final List<Bytes> below = new ArrayList<>();
      final List<Bytes> left = new ArrayList<>();
      subtree(random, TrieNodeCodec.childLocation(branch, prefix, prefixLength, 0), 1, left, below);
      final int leftSize = below.size();
      subtree(
          random,
          TrieNodeCodec.childLocation(branch, prefix, prefixLength, 1),
          1,
          new ArrayList<>(),
          below);

      for (int i = 0; i < below.size(); i++) {
        final int order = LOCATION_ORDER.compare(below.get(i), branch);
        if (prefixLength == 0) {
          assertThat(Integer.signum(order)).isEqualTo(i < leftSize ? -1 : 1);
        } else {
          final boolean firstBitSet = (prefix[0] & 0x80) != 0;
          assertThat(Integer.signum(order)).isEqualTo(firstBitSet ? 1 : -1);
        }
      }
    }
  }

  @Test
  void bulkLoadStoresStemsInLocationOrder() {
    final Random random = new Random(42);
    final TreeSet<Bytes> stems = new TreeSet<>(LOCATION_ORDER);
    while (stems.size() < 5_000) {
      // Clusters of stems sharing long prefixes, as accounts' storage does, then sparse ones.
      final byte[] stem = new byte[31];
      random.nextBytes(stem);
      if (random.nextInt(4) == 0 && !stems.isEmpty()) {
        System.arraycopy(stems.last().toArrayUnsafe(), 0, stem, 0, 1 + random.nextInt(30));
      }
      stems.add(Bytes.wrap(stem));
    }
    final List<Bytes> stored = new ArrayList<>();
    final List<Bytes> storedValues = new ArrayList<>();
    final AscendingCollapseBinaryTrie trie =
        new AscendingCollapseBinaryTrie(
            (location, hash, value) -> {
              stored.add(location);
              storedValues.add(value);
            });
    for (final Bytes stem : stems) {
      final List<Bytes> keys = new ArrayList<>();
      for (final int suffix : new TreeSet<>(List.of(random.nextInt(256), random.nextInt(256)))) {
        keys.add(Bytes.concatenate(stem, Bytes.of(suffix)));
      }
      final List<Bytes> values = keys.stream().map(key -> Bytes.random(32, random)).toList();
      trie.insert(trie.prepare(keys, values));
    }
    trie.rootHash();

    final List<Bytes> storedStems = new ArrayList<>();
    final Set<Bytes> seen = new HashSet<>();
    Bytes highest = null;
    for (int i = 0; i < stored.size(); i++) {
      final Bytes location = stored.get(i);
      assertThat(seen.add(location)).isTrue();
      if (storedValues.get(i).get(0) == TrieNodeCodec.STEM_TAG && !location.isEmpty()) {
        storedStems.add(location);
      }
      if (highest != null && LOCATION_ORDER.compare(location, highest) < 0) {
        assertThat(location.isEmpty() || storedValues.get(i).get(0) == TrieNodeCodec.BRANCH_TAG)
            .as("only branches and the root come back below a stored location")
            .isTrue();
      } else {
        highest = location;
      }
    }
    assertThat(storedStems).hasSize(stems.size()).isSortedAccordingTo(LOCATION_ORDER);
  }

  /**
   * A random subtree at {@code location}: its stems in depth-first order, and every node location
   * below it.
   */
  private static void subtree(
      final Random random,
      final Bytes location,
      final int depth,
      final List<Bytes> stems,
      final List<Bytes> nodes) {
    if (depth > 0 && (depth > 12 || random.nextInt(4) == 0)) {
      stems.add(location);
      nodes.add(location);
      return;
    }
    final byte[] prefix = randomBits(random);
    final int prefixLength = random.nextInt(9);
    subtree(
        random,
        TrieNodeCodec.childLocation(location, prefix, prefixLength, 0),
        depth + 1,
        stems,
        nodes);
    subtree(
        random,
        TrieNodeCodec.childLocation(location, prefix, prefixLength, 1),
        depth + 1,
        stems,
        nodes);
    nodes.add(location);
  }

  private static byte[] randomBits(final Random random) {
    final byte[] bits = new byte[1];
    random.nextBytes(bits);
    return bits;
  }
}
