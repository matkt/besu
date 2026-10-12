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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BlockRangesTest {

  @Test
  void mergesAdjacentAndOverlappingRanges() {
    final BlockRanges ranges = new BlockRanges();
    ranges.add(10, 19);
    ranges.add(30, 39);
    ranges.add(20, 29);

    assertThat(ranges.covers(10, 39)).isTrue();
    assertThat(ranges.size()).isEqualTo(30);

    ranges.add(5, 12);
    assertThat(ranges.covers(5, 39)).isTrue();
    assertThat(ranges.size()).isEqualTo(35);
  }

  @Test
  void removesInsideAndAcrossRanges() {
    final BlockRanges ranges = new BlockRanges();
    ranges.add(1, 100);
    ranges.remove(40, 49);

    assertThat(ranges.covers(1, 39)).isTrue();
    assertThat(ranges.covers(50, 100)).isTrue();
    assertThat(ranges.covers(39, 50)).isFalse();
    assertThat(ranges.size()).isEqualTo(90);

    ranges.remove(30, Long.MAX_VALUE);
    assertThat(ranges.size()).isEqualTo(29);
    assertThat(ranges.covers(1, 29)).isTrue();
  }

  @Test
  void findsTheHighestGapOfBothSets() {
    final BlockRanges stored = new BlockRanges();
    final BlockRanges fetching = new BlockRanges();
    stored.add(91, 100);
    fetching.add(81, 90);
    stored.add(61, 70);

    assertThat(BlockRanges.highestGap(stored, fetching, 1, 100, 32)).containsExactly(71, 80);
    fetching.add(71, 80);
    assertThat(BlockRanges.highestGap(stored, fetching, 1, 100, 32)).containsExactly(29, 60);
    assertThat(BlockRanges.highestGap(stored, fetching, 65, 100, 32)).isNull();
    assertThat(BlockRanges.highestGap(stored, fetching, 1, 100, 8)).containsExactly(53, 60);
  }
}
