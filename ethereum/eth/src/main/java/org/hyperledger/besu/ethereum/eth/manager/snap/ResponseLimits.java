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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import kotlin.Pair;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The limits of a snap response: its bytes, entries, time and end key. */
final class ResponseLimits {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResponseLimits.class);
  private static final int MAX_ENTRIES_PER_REQUEST = 100000;
  static final int MAX_RESPONSE_SIZE = 2 * 1024 * 1024;

  private ResponseLimits() {}

  /**
   * Predicate that doesn't immediately stop when the delegate predicate returns false, but instead
   * sets a flag to stop after the current element is processed.
   */
  static class ExceedingPredicate<T> implements Predicate<T> {
    private final Predicate<T> delegate;
    final AtomicBoolean shouldContinue = new AtomicBoolean(true);

    public ExceedingPredicate(final Predicate<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean test(final T candidate) {
      final boolean result = delegate.test(candidate);
      return shouldContinue.getAndSet(result);
    }

    public boolean shouldGetMore() {
      return shouldContinue.get();
    }
  }

  /** Predicate that stops when the end key is exceeded. */
  record EndKeyExceedsPredicate(Bytes endKey) implements Predicate<Pair<Bytes32, Bytes>> {

    @Override
    public boolean test(final Pair<Bytes32, Bytes> pair) {
      return endKey.compareTo(Bytes.wrap(pair.getFirst())) > 0;
    }
  }

  static class ResponseSizePredicate<T> implements Predicate<T> {
    // default to a max of 4 seconds per request
    static final long DEFAULT_MAX_MILLIS_PER_REQUEST = 4000;

    final AtomicInteger byteLimit = new AtomicInteger(0);
    final AtomicInteger recordLimit = new AtomicInteger(0);
    final AtomicBoolean shouldContinue = new AtomicBoolean(true);
    final Function<T, Integer> encodingSizeAccumulator;
    final StopWatch stopWatch;
    final int maxResponseBytes;
    final long maxMillisPerRequest;
    final String forWhat;

    ResponseSizePredicate(
        final String forWhat,
        final StopWatch stopWatch,
        final int maxResponseBytes,
        final long maxMillisPerRequest,
        final Function<T, Integer> encodingSizeAccumulator) {
      this.stopWatch = stopWatch;
      this.maxResponseBytes = maxResponseBytes;
      this.maxMillisPerRequest = maxMillisPerRequest;
      this.forWhat = forWhat;
      this.encodingSizeAccumulator = encodingSizeAccumulator;
    }

    @Override
    public boolean test(final T candidate) {
      LOGGER
          .atTrace()
          .setMessage("{} pre-accumulate limits, bytes: {} , stream count: {}")
          .addArgument(() -> forWhat)
          .addArgument(byteLimit::get)
          .addArgument(recordLimit::get)
          .log();
      if (stopWatch.getTime() > maxMillisPerRequest) {
        shouldContinue.set(false);
        LOGGER.warn(
            "{} took too long, stopped at {} ms with {} records and {} bytes",
            forWhat,
            stopWatch.formatTime(),
            recordLimit.get(),
            byteLimit.get());
        return false;
      }

      var underRecordLimit = recordLimit.addAndGet(1) <= MAX_ENTRIES_PER_REQUEST;
      var underByteLimit =
          byteLimit.accumulateAndGet(0, (cur, __) -> cur + encodingSizeAccumulator.apply(candidate))
              < maxResponseBytes;
      if (underRecordLimit && underByteLimit) {
        return true;
      } else {
        shouldContinue.set(false);
        LOGGER
            .atDebug()
            .setMessage("{} post-accumulate limits, bytes: {} , stream count: {}")
            .addArgument(() -> forWhat)
            .addArgument(byteLimit::get)
            .addArgument(recordLimit::get)
            .log();
        return false;
      }
    }
  }
}
