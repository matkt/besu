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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Eip8347ExternalSorterTest {

  @TempDir Path tmp;

  @Test
  void inMemorySortWhenBufferIsNeverFull() throws Exception {
    assertSortsLikeArraysSort(Eip8347ExternalSorter.DEFAULT_BUFFER_BYTES, 500);
  }

  @Test
  void multiPassMergeBeyondFanIn() throws Exception {
    // One record per run and far more runs than MAX_FAN_IN: needs several merge passes.
    assertSortsLikeArraysSort(1, 20 * Eip8347ExternalSorter.MAX_FAN_IN);
  }

  private void assertSortsLikeArraysSort(final long bufferBytes, final int count) throws Exception {
    final Random random = new Random(42);
    final List<byte[]> keys = new ArrayList<>();
    try (final Eip8347ExternalSorter sorter = new Eip8347ExternalSorter(tmp, "t", bufferBytes)) {
      for (int i = 0; i < count; i++) {
        // Variable lengths and a few duplicates; unsigned order must hold (0x80.. > 0x7f..).
        final byte[] key = new byte[1 + random.nextInt(4)];
        random.nextBytes(key);
        keys.add(key);
        sorter.add(key, new byte[] {(byte) i});
      }
      keys.sort(Arrays::compareUnsigned);
      final List<byte[]> sorted = new ArrayList<>();
      for (final Iterator<Eip8347ExternalSorter.Entry> it = sorter.sorted(); it.hasNext(); ) {
        sorted.add(it.next().key());
      }
      assertThat(sorted).hasSize(count);
      for (int i = 0; i < count; i++) {
        assertThat(sorted.get(i)).isEqualTo(keys.get(i));
      }
    }
    try (final var left = Files.list(tmp)) {
      assertThat(left).isEmpty();
    }
  }
}
