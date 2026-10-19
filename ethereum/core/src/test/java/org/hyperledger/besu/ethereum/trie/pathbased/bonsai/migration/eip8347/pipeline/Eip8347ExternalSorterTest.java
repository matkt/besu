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
    // One record per run and more than twice MAX_FAN_IN runs: needs several merge passes.
    assertSortsLikeArraysSort(1, 2 * Eip8347ExternalSorter.MAX_FAN_IN + 1);
  }

  @Test
  void frontCodedRunsRestoreKeysSharingLongPrefixes() throws Exception {
    // Storage-like keys: a few 33-byte account prefixes, random tails, some keys prefixes of
    // others; a small buffer puts many records in each run and still forces several runs.
    final Random random = new Random(7);
    final byte[][] accounts = new byte[3][33];
    for (final byte[] account : accounts) {
      random.nextBytes(account);
    }
    final List<byte[]> keys = new ArrayList<>();
    try (final Eip8347ExternalSorter sorter = new Eip8347ExternalSorter(tmp, "f", 4096)) {
      for (int i = 0; i < 2000; i++) {
        final byte[] key = Arrays.copyOf(accounts[random.nextInt(3)], 33 + random.nextInt(34));
        final byte[] tail = new byte[key.length - 33];
        random.nextBytes(tail);
        System.arraycopy(tail, 0, key, 33, tail.length);
        keys.add(key);
        sorter.add(key, new byte[] {(byte) i});
      }
      keys.sort(Arrays::compareUnsigned);
      final Iterator<Eip8347ExternalSorter.Entry> it = sorter.sorted();
      for (final byte[] key : keys) {
        assertThat(it.next().key()).isEqualTo(key);
      }
      assertThat(it.hasNext()).isFalse();
    }
  }

  @Test
  void spilledRecordsKeepValuesOfEveryLengthClass() throws Exception {
    // Lengths around the 1-, 2- and 3-byte varint boundaries must survive a run on disk.
    final int[] lengths = {0, 1, 127, 128, 129, 16_383, 16_384, 70_000};
    try (final Eip8347ExternalSorter sorter = new Eip8347ExternalSorter(tmp, "v", 1)) {
      for (int i = 0; i < lengths.length; i++) {
        final byte[] value = new byte[lengths[i]];
        Arrays.fill(value, (byte) i);
        sorter.add(new byte[] {(byte) i}, value);
      }
      final Iterator<Eip8347ExternalSorter.Entry> it = sorter.sorted();
      for (int i = 0; i < lengths.length; i++) {
        final Eip8347ExternalSorter.Entry entry = it.next();
        assertThat(entry.key()).containsExactly((byte) i);
        assertThat(entry.value()).hasSize(lengths[i]);
        if (lengths[i] > 0) {
          assertThat(entry.value()[lengths[i] - 1]).isEqualTo((byte) i);
        }
      }
      assertThat(it.hasNext()).isFalse();
    }
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
