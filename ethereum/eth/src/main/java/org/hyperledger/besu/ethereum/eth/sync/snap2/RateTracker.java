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

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.OptionalDouble;

/**
 * The rate of a progress figure over its last few samples, which the ETAs of the logs rest on: a
 * moving average that only speaks once a few samples are in and the figure moves forward.
 */
public final class RateTracker {

  private static final int MIN_SAMPLES = 3;

  private record Sample(long millis, double value) {}

  private final Clock clock;
  private final int window;
  private final ArrayDeque<Sample> samples = new ArrayDeque<>();

  /**
   * Creates a tracker.
   *
   * @param window how many samples the average spans
   */
  public RateTracker(final Clock clock, final int window) {
    this.clock = clock;
    this.window = window;
  }

  /** Records the current value of the figure. */
  public synchronized void sample(final double value) {
    samples.addLast(new Sample(clock.millis(), value));
    while (samples.size() > window) {
      samples.removeFirst();
    }
  }

  /** Returns the rate per second over the samples kept, once a few are in and it is positive. */
  public synchronized OptionalDouble perSecond() {
    if (samples.size() < MIN_SAMPLES) {
      return OptionalDouble.empty();
    }
    final Sample first = samples.getFirst();
    final Sample last = samples.getLast();
    final double seconds = (last.millis() - first.millis()) / 1000.0;
    if (seconds <= 0) {
      return OptionalDouble.empty();
    }
    final double rate = (last.value() - first.value()) / seconds;
    return rate > 0 ? OptionalDouble.of(rate) : OptionalDouble.empty();
  }
}
