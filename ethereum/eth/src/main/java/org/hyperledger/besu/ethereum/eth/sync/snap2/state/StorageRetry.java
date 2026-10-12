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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import org.hyperledger.besu.plugin.services.exception.StorageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retries idempotent writes. Under heavy concurrent writes, RocksDB optimistic transactions may
 * fail to commit because the memtable no longer holds what is needed to check them for conflicts.
 */
public final class StorageRetry {

  private static final Logger LOG = LoggerFactory.getLogger(StorageRetry.class);
  private static final int ATTEMPTS = 6;

  private StorageRetry() {}

  /** Runs a write, which must start its own transaction, until it commits. */
  public static void run(final Runnable write) {
    for (int attempt = 1; ; attempt++) {
      try {
        write.run();
        return;
      } catch (final StorageException e) {
        if (attempt == ATTEMPTS) {
          throw e;
        }
        LOG.debug("Write not committed, retrying: {}", e.getMessage());
        try {
          Thread.sleep(10L << attempt);
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
  }
}
