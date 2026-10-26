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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.convert;

import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.AscendingCollapseBinaryTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.Leaf;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347Pipelines;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** PBT leaves gathered in an external sort, then the PBT built from them in key order. */
public final class Eip8347SortedLeaves {

  private Eip8347SortedLeaves() {}

  /** Receives the leaves of the PBT being built, in key order. */
  @FunctionalInterface
  public interface LeafSink {
    void accept(Leaf leaf) throws IOException;
  }

  /** Adds a leaf to the sort; its value is stored without leading zeros. */
  public static void add(final Eip8347ExternalSorter leaves, final Leaf leaf) throws IOException {
    leaves.add(leaf.key().toArrayUnsafe(), leaf.value().trimLeadingZeros().toArrayUnsafe());
  }

  /**
   * Builds {@code pbt} from the sorted leaves: stems are hashed on {@code pool}, then attached in
   * order on one thread, which passes their leaves to {@code sink}.
   *
   * @return the number of leaves
   */
  public static long build(
      final Iterator<Eip8347ExternalSorter.Entry> sorted,
      final AscendingCollapseBinaryTrie pbt,
      final LeafSink sink,
      final Executor pool,
      final int parallelism)
      throws IOException {
    final AtomicLong leafCount = new AtomicLong();
    Eip8347Pipelines.run(
        Eip8347Pipelines.fromLists("eip8347-pbt-stems", stems(sorted), List::size)
            .thenProcessAsyncOrdered(
                "eip8347-pbt-hash-stems",
                Eip8347Pipelines.async(
                    pool, stems -> stems.stream().map(stem -> hashStem(pbt, stem)).toList()),
                parallelism)
            .andFinishWith(
                "eip8347-pbt-attach",
                stems -> {
                  try {
                    for (final HashedStem stem : stems) {
                      pbt.insert(stem.subtree());
                      for (final Leaf leaf : stem.leaves()) {
                        sink.accept(leaf);
                      }
                      leafCount.addAndGet(stem.leaves().size());
                    }
                  } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                  }
                }));
    return leafCount.get();
  }

  /** The leaves of one stem, with their subtree hashed for the PBT. */
  private record HashedStem(List<Leaf> leaves, AscendingCollapseBinaryTrie.Subtree subtree) {}

  private static HashedStem hashStem(final AscendingCollapseBinaryTrie pbt, final List<Leaf> stem) {
    return new HashedStem(
        stem,
        pbt.prepare(
            stem.stream().map(Leaf::key).toList(), stem.stream().<Bytes>map(Leaf::value).toList()));
  }

  /** Groups sorted leaves by stem (every key but its last byte). */
  private static Iterator<List<Leaf>> stems(final Iterator<Eip8347ExternalSorter.Entry> sorted) {
    final PeekingIterator<Eip8347ExternalSorter.Entry> leaves = Iterators.peekingIterator(sorted);
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return leaves.hasNext();
      }

      @Override
      public List<Leaf> next() {
        final List<Leaf> stem = new ArrayList<>();
        final byte[] first = leaves.peek().key();
        while (leaves.hasNext() && sameStem(first, leaves.peek().key())) {
          final Eip8347ExternalSorter.Entry entry = leaves.next();
          stem.add(new Leaf(Bytes.wrap(entry.key()), Bytes32.leftPad(Bytes.wrap(entry.value()))));
        }
        return stem;
      }
    };
  }

  private static boolean sameStem(final byte[] a, final byte[] b) {
    return a.length == b.length && Arrays.equals(a, 0, a.length - 1, b, 0, b.length - 1);
  }
}
