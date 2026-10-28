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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import org.hyperledger.besu.plugin.services.exception.StorageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tries a write on the chain variables again, as RocksDB may fail to check the conflicts of its
 * transaction while a sync writes.
 */
public final class WriteRetry {

  private static final Logger LOG = LoggerFactory.getLogger(WriteRetry.class);
  private static final int ATTEMPTS = 6;

  /** Waits between two tries. */
  public interface Pause {
    void sleep(long millis) throws InterruptedException;
  }

  private WriteRetry() {}

  /** Runs the write up to six times, 20 ms to 320 ms apart; the last failure goes to the caller. */
  public static void run(final Runnable write, final Pause pause) {
    for (int attempt = 1; attempt < ATTEMPTS; attempt++) {
      try {
        write.run();
        return;
      } catch (final StorageException e) {
        LOG.debug("Write not committed, retrying: {}", e.getMessage());
        try {
          pause.sleep(10L << attempt);
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
    write.run();
  }
}
