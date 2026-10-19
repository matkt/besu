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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

class Eip8347PipelinesTest {

  @Test
  void asyncRunsTheFunctionOnTheGivenPool() {
    final List<Runnable> submitted = new ArrayList<>();

    final CompletableFuture<Integer> doubled =
        Eip8347Pipelines.<Integer, Integer>async(submitted::add, x -> 2 * x).apply(21);

    // Nothing runs on the calling thread or on the common pool: the task waits in the pool.
    assertThat(doubled).isNotDone();
    assertThat(submitted).hasSize(1);
    submitted.getFirst().run();
    assertThat(doubled).isCompletedWithValue(42);
  }
}
