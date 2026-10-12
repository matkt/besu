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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class KeyRangeSetTest {

  private static Bytes32 key(final long value) {
    return Bytes32.fromHexStringLenient(Long.toHexString(value));
  }

  @Test
  void mergesAdjacentAndOverlappingRanges() {
    final KeyRangeSet set = new KeyRangeSet();
    set.add(key(10), key(20));
    set.add(key(21), key(30));
    set.add(key(25), key(40));
    set.add(key(50), key(60));

    assertThat(set.snapshot())
        .containsExactly(
            java.util.Map.entry(key(10), key(40)), java.util.Map.entry(key(50), key(60)));
    assertThat(set.contains(key(10))).isTrue();
    assertThat(set.contains(key(40))).isTrue();
    assertThat(set.contains(key(41))).isFalse();
    assertThat(set.contains(key(9))).isFalse();
    assertThat(set.rangeEnd(key(15))).contains(key(40));

    set.add(key(41), key(49));
    assertThat(set.snapshot()).containsExactly(java.util.Map.entry(key(10), key(60)));
  }

  @Test
  void detectsTheFullKeySpace() {
    final KeyRangeSet set = new KeyRangeSet();
    final List<Bytes32[]> parts = Keys.split(Keys.MIN, Keys.MAX, 16);
    assertThat(parts).hasSize(16);
    for (int i = parts.size() - 1; i >= 0; i--) {
      assertThat(set.isFull()).isFalse();
      set.add(parts.get(i)[0], parts.get(i)[1]);
    }
    assertThat(set.isFull()).isTrue();
    assertThat(set.coverage()).isEqualTo(1.0);
  }

  @Test
  void stepsKeys() {
    assertThat(Keys.next(Keys.MAX)).isEmpty();
    assertThat(Keys.next(key(0xff))).contains(key(0x100));
    assertThat(Keys.previous(key(0x100))).isEqualTo(key(0xff));
  }
}
