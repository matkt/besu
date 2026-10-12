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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes32;

/** Thread-safe set of disjoint, inclusive key ranges; adjacent ranges are merged. */
public final class KeyRangeSet {

  private final TreeMap<Bytes32, Bytes32> ranges = new TreeMap<>();

  /** Adds {@code [start, end]}. */
  public synchronized void add(final Bytes32 start, final Bytes32 end) {
    if (start.compareTo(end) > 0) {
      throw new IllegalArgumentException("start after end");
    }
    // bounds often slice a whole network message, keep a compact copy instead
    Bytes32 newStart = start.copy();
    Bytes32 newEnd = end.copy();
    final Map.Entry<Bytes32, Bytes32> floor = ranges.floorEntry(start);
    if (floor != null && touches(floor.getValue(), start)) {
      newStart = floor.getKey();
      if (floor.getValue().compareTo(newEnd) > 0) {
        newEnd = floor.getValue();
      }
      ranges.remove(floor.getKey());
    }
    while (true) {
      final Map.Entry<Bytes32, Bytes32> next = ranges.ceilingEntry(newStart);
      if (next == null || !touches(newEnd, next.getKey())) {
        break;
      }
      if (next.getValue().compareTo(newEnd) > 0) {
        newEnd = next.getValue();
      }
      ranges.remove(next.getKey());
    }
    ranges.put(newStart, newEnd);
  }

  /** Returns whether {@code key} is in the set. */
  public synchronized boolean contains(final Bytes32 key) {
    final Map.Entry<Bytes32, Bytes32> floor = ranges.floorEntry(key);
    return floor != null && floor.getValue().compareTo(key) >= 0;
  }

  /** Returns whether the whole key space is covered. */
  public synchronized boolean isFull() {
    return ranges.size() == 1
        && ranges.firstKey().equals(Keys.MIN)
        && ranges.firstEntry().getValue().equals(Keys.MAX);
  }

  /** Returns whether nothing is covered. */
  public synchronized boolean isEmpty() {
    return ranges.isEmpty();
  }

  /** Returns the share of the key space covered. */
  public synchronized double coverage() {
    double total = 0;
    for (final Map.Entry<Bytes32, Bytes32> range : ranges.entrySet()) {
      total += Keys.fraction(range.getKey(), range.getValue());
    }
    return total;
  }

  /** Returns the end of the range holding {@code key}, if any. */
  public synchronized Optional<Bytes32> rangeEnd(final Bytes32 key) {
    final Map.Entry<Bytes32, Bytes32> floor = ranges.floorEntry(key);
    return floor != null && floor.getValue().compareTo(key) >= 0
        ? Optional.of(floor.getValue())
        : Optional.empty();
  }

  /** Returns a copy of the ranges, keyed by start. */
  public synchronized NavigableMap<Bytes32, Bytes32> snapshot() {
    return new TreeMap<>(ranges);
  }

  // whether a range ending at end touches or overlaps a range starting at start
  private static boolean touches(final Bytes32 end, final Bytes32 start) {
    return end.compareTo(start) >= 0 || Keys.next(end).map(start::equals).orElse(true);
  }
}
