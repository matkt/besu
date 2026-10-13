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

import org.hyperledger.besu.ethereum.trie.RangeManager;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class StorageTaskTest {

  private static final Bytes32 MIDDLE = Bytes32.fromHexString("0x80" + "00".repeat(31));

  @Test
  void reportsTheCompletionOfTheStorageOnce() {
    final StorageTask task = new StorageTask(Bytes32.ZERO, Bytes32.ZERO);

    assertThat(task.cover(MIDDLE, RangeManager.MAX_RANGE)).isFalse();
    assertThat(task.cover(RangeManager.MIN_RANGE, MIDDLE)).isTrue();
    // a range covered again, as a retried chunk does, completes nothing more
    assertThat(task.cover(RangeManager.MIN_RANGE, MIDDLE)).isFalse();
    assertThat(task.covers(MIDDLE)).isTrue();
  }
}
