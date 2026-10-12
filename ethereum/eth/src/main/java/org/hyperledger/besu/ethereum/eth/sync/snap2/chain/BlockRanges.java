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

import java.util.Map;
import java.util.TreeMap;

/**
 * Disjoint ranges of block numbers, bounds included; adjacent ranges are merged. Not thread safe.
 */
final class BlockRanges {

  private final TreeMap<Long, Long> ranges = new TreeMap<>();

  void add(final long from, final long to) {
    long start = from;
    long end = to;
    final Map.Entry<Long, Long> before = ranges.floorEntry(from);
    if (before != null && before.getValue() >= from - 1) {
      start = before.getKey();
      end = Math.max(end, before.getValue());
    }
    Map.Entry<Long, Long> next = ranges.ceilingEntry(start);
    while (next != null && next.getKey() <= end + 1) {
      end = Math.max(end, next.getValue());
      ranges.remove(next.getKey());
      next = ranges.ceilingEntry(start);
    }
    ranges.put(start, end);
  }

  void remove(final long from, final long to) {
    final Map.Entry<Long, Long> before = ranges.floorEntry(from);
    if (before != null && before.getValue() >= from) {
      ranges.put(before.getKey(), from - 1);
      if (before.getValue() > to) {
        ranges.put(to + 1, before.getValue());
      }
      if (before.getKey() == from) {
        ranges.remove(from);
      }
    }
    Map.Entry<Long, Long> next = ranges.ceilingEntry(from);
    while (next != null && next.getKey() <= to) {
      ranges.remove(next.getKey());
      if (next.getValue() > to) {
        ranges.put(to + 1, next.getValue());
      }
      next = ranges.ceilingEntry(from);
    }
  }

  boolean covers(final long from, final long to) {
    final Map.Entry<Long, Long> range = ranges.floorEntry(from);
    return range != null && range.getValue() >= to;
  }

  /** Returns the start of the range holding {@code number}, or -1. */
  long startOf(final long number) {
    final Map.Entry<Long, Long> range = ranges.floorEntry(number);
    return range != null && range.getValue() >= number ? range.getKey() : -1;
  }

  /** Returns the end of the highest range below {@code number}, or -1. */
  long endBelow(final long number) {
    final Map.Entry<Long, Long> range = ranges.lowerEntry(number);
    return range == null ? -1 : Math.min(range.getValue(), number - 1);
  }

  long size() {
    long size = 0;
    for (final Map.Entry<Long, Long> range : ranges.entrySet()) {
      size += range.getValue() - range.getKey() + 1;
    }
    return size;
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
