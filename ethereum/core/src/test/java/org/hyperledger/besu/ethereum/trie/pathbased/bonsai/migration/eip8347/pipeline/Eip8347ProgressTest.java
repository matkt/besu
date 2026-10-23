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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

class Eip8347ProgressTest {

  private static final long MINUTE = Eip8347Progress.INTERVAL.toMillis();

  @Test
  void givesALineOncePerInterval() {
    final Eip8347Progress progress = new Eip8347Progress(0);

    assertThat(progress.line(10, 100, MINUTE - 1)).isEmpty();
    assertThat(progress.hasLogged()).isFalse();
    assertThat(progress.line(1, 8, MINUTE)).contains("12.50%");
    assertThat(progress.line(30, 100, 2 * MINUTE - 1)).isEmpty();
    assertThat(progress.line(50, 100, 2 * MINUTE)).contains("50.00%");
    assertThat(progress.hasLogged()).isTrue();
  }

  @Test
  void clampsTheShareDone() {
    assertThat(new Eip8347Progress(0).line(120, 100, MINUTE)).contains("100.00%");
    assertThat(new Eip8347Progress(0).line(-5, 100, MINUTE)).contains("0.00%");
  }

  @Test
  void loggingHandsOutEveryItemAndLogsTheFileShareRead() {
    final Logger log = mock(Logger.class);
    final AtomicLong bytesRead = new AtomicLong();
    final AtomicLong now = new AtomicLong();
    final Iterator<Integer> items =
        Eip8347Progress.logging(
            log,
            "step 1/3 reading the preimages",
            List.of(1, 2, 3).iterator(),
            bytesRead::get,
            400,
            now::get);

    final List<Integer> read = new ArrayList<>();
    bytesRead.set(100);
    read.add(items.next());
    bytesRead.set(200);
    now.set(MINUTE);
    read.add(items.next());
    bytesRead.set(400);
    read.add(items.next());

    assertThat(read).containsExactly(1, 2, 3);
    assertThat(items.hasNext()).isFalse();
    // a single line: the third item came within the same minute
    verify(log)
        .info(
            "EIP-8347 verification in progress, {}: {}",
            "step 1/3 reading the preimages",
            "50.00%");
    verifyNoMoreInteractions(log);
  }
}
