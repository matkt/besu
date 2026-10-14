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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import java.util.Iterator;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

/** Disjoint ranges of block numbers, bounds included, adjacent ones merged. Not thread safe. */
final class BlockRanges {

  // closed-open ranges, which merge when adjacent
  private final RangeSet<Long> ranges = TreeRangeSet.create();

  void add(final long from, final long to) {
    ranges.add(Range.closedOpen(from, to + 1));
  }

  void remove(final long from, final long to) {
    ranges.remove(Range.closedOpen(from, to + 1));
  }

  /** Returns the start of the range holding {@code number}, or -1. */
  long startOf(final long number) {
    final Range<Long> range = ranges.rangeContaining(number);
    return range == null ? -1 : range.lowerEndpoint();
  }

  /** Returns the end of the highest range below {@code number}, or -1. */
  long endBelow(final long number) {
    final Iterator<Range<Long>> below =
        ranges.subRangeSet(Range.lessThan(number)).asDescendingSetOfRanges().iterator();
    return below.hasNext() ? below.next().upperEndpoint() - 1 : -1;
  }

  boolean isEmpty() {
    return ranges.isEmpty();
  }

  /**
   * Returns the highest run of at most {@code limit} numbers between {@code lowest} and {@code
   * highest} that is in neither set, as {from, to}, or null.
   */
  static long[] highestGap(
      final BlockRanges first,
      final BlockRanges second,
      final long lowest,
      final long highest,
      final int limit) {
    long top = highest;
    while (top >= lowest) {
      final long start = Math.max(first.startOf(top), second.startOf(top));
      if (start < 0) {
        break;
      }
      top = start - 1;
    }
    if (top < lowest) {
      return null;
    }
    final long covered = Math.max(first.endBelow(top), second.endBelow(top));
    final long bottom = Math.max(Math.max(lowest, covered + 1), top - limit + 1);
    return new long[] {bottom, top};
  }
}
