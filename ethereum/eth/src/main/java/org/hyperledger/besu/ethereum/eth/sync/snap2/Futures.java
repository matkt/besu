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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/** Helpers for the asynchronous steps of the sync. */
final class Futures {

  private Futures() {}

  /** Completes after a delay, on the given executor. */
  static CompletableFuture<Void> delay(final Duration delay, final Executor executor) {
    return CompletableFuture.runAsync(
        () -> {},
        CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS, executor));
  }

  /** Returns the cause of a failure, without its completion wrappers. */
  static Throwable unwrap(final Throwable error) {
    Throwable cause = error;
    while (cause instanceof CompletionException && cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }
}
