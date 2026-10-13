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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

class RateTrackerTest {

  private final SettableClock clock = new SettableClock();
  private final RateTracker tracker = new RateTracker(clock, 5);

  @Test
  void speaksOnlyAfterAFewSamples() {
    tracker.sample(0);
    clock.advance(Duration.ofSeconds(30));
    tracker.sample(300);
    assertThat(tracker.perSecond()).isEmpty();

    clock.advance(Duration.ofSeconds(30));
    tracker.sample(600);
    assertThat(tracker.perSecond()).hasValue(10);
  }

  @Test
  void averagesOverTheLastSamplesOnly() {
    for (int i = 0; i < 10; i++) {
      tracker.sample(i < 5 ? 0 : (i - 4) * 100);
      clock.advance(Duration.ofSeconds(10));
    }

    // the window holds the last five samples, 40 seconds apart end to end
    assertThat(tracker.perSecond()).hasValue(10);
  }

  @Test
  void givesNoRateWhenTheFigureStandsStillOrGoesBack() {
    for (int i = 0; i < 3; i++) {
      tracker.sample(42);
      clock.advance(Duration.ofSeconds(10));
    }
    assertThat(tracker.perSecond()).isEmpty();

    tracker.sample(0);
    assertThat(tracker.perSecond()).isEmpty();
  }

  @Test
  void givesNoRateWithoutTimeElapsed() {
    tracker.sample(0);
    tracker.sample(10);
    tracker.sample(20);

    assertThat(tracker.perSecond()).isEmpty();
  }

  private static final class SettableClock extends Clock {
    private Instant now = Instant.EPOCH;

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(final ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }

    void advance(final Duration duration) {
      now = now.plus(duration);
    }
  }
}
