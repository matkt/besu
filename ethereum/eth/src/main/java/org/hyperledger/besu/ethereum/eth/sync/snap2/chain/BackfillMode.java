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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import java.time.Duration;

/**
 * How hard the back-fill of the bodies and receipts works beside the block import: its rate, its
 * requests in flight on as many peers, how long it keeps away after each block import, and how far
 * behind the wall clock the chain head may fall, past one slot, before it halves its rate. Every
 * mode stops while the node is behind the chain head.
 */
public enum BackfillMode {
  /** For validators on modest disks */
  SLOW(5L << 20, 2, Duration.ofSeconds(2), Duration.ofSeconds(2)),
  /** The default. */
  MEDIUM(14L << 20, 4, Duration.ofSeconds(1), Duration.ofSeconds(4)),
  /** For RPC or archive nodes that want the history quickly */
  FAST(32L << 20, 8, Duration.ZERO, Duration.ofSeconds(4));

  private final long bytesPerSecond;
  private final int requests;
  private final Duration importGrace;
  private final Duration headLagAllowance;

  BackfillMode(
      final long bytesPerSecond,
      final int requests,
      final Duration importGrace,
      final Duration headLagAllowance) {
    this.bytesPerSecond = bytesPerSecond;
    this.requests = requests;
    this.importGrace = importGrace;
    this.headLagAllowance = headLagAllowance;
  }

  /** Returns the most bytes of bodies and receipts stored per second. */
  public long bytesPerSecond() {
    return bytesPerSecond;
  }

  /** Returns the requests in flight, each to a different peer. */
  public int requests() {
    return requests;
  }

  /** Returns how long the back-fill keeps away after a block import. */
  public Duration importGrace() {
    return importGrace;
  }

  /** Returns how far past a slot the chain head may lag the wall clock before backing off. */
  public Duration headLagAllowance() {
    return headLagAllowance;
  }
}
