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

import java.time.Duration;
import java.util.Iterator;
import java.util.Locale;
import java.util.Optional;
import java.util.function.LongSupplier;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;

/**
 * Progress of a long migration step, for a user to follow: the share done, at most once per {@link
 * #INTERVAL}.
 */
public final class Eip8347Progress {

  /** Least time between two progress lines. */
  public static final Duration INTERVAL = Duration.ofMinutes(1);

  private long nextLineMillis;
  private boolean logged;

  /** A step started at {@code startMillis} (epoch millis); its first line is due an interval on. */
  public Eip8347Progress(final long startMillis) {
    this.nextLineMillis = startMillis + INTERVAL.toMillis();
  }

  /**
   * {@code done} of {@code total} as {@code "45.12%"} once the interval has passed since the last
   * line, else empty.
   */
  public Optional<String> line(final long done, final long total, final long nowMillis) {
    if (nowMillis < nextLineMillis || total <= 0) {
      return Optional.empty();
    }
    nextLineMillis = nowMillis + INTERVAL.toMillis();
    logged = true;
    return Optional.of(
        String.format(Locale.ROOT, "%.2f%%", 100.0 * Math.clamp(done, 0, total) / total));
  }

  /** Whether a line has been given. */
  public boolean hasLogged() {
    return logged;
  }

  /**
   * {@code source}, logging how far {@code step} is through a file of {@code fileBytes} once an
   * interval has passed, as it hands out items.
   */
  public static <T> Iterator<T> logging(
      final Logger log,
      final String step,
      final Iterator<T> source,
      final LongSupplier bytesRead,
      final long fileBytes) {
    return logging(log, step, source, bytesRead, fileBytes, System::currentTimeMillis);
  }

  /** Same, on {@code clock} (epoch millis). */
  static <T> Iterator<T> logging(
      final Logger log,
      final String step,
      final Iterator<T> source,
      final LongSupplier bytesRead,
      final long fileBytes,
      final LongSupplier clock) {
    final Eip8347Progress progress = new Eip8347Progress(clock.getAsLong());
    return Iterators.transform(
        source,
        item -> {
          progress
              .line(bytesRead.getAsLong(), fileBytes, clock.getAsLong())
              .ifPresent(line -> log.info("EIP-8347 verification in progress, {}: {}", step, line));
          return item;
        });
  }
}
