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
package org.hyperledger.besu.ethereum.eth.sync.snap2.net;

import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

/**
 * A request sent to one peer through the {@link RequestScheduler}.
 *
 * @param <T> the decoded response
 */
public interface SyncRequest<T> {

  /** Protocol a request is sent on. */
  enum Protocol {
    ETH,
    SNAP
  }

  /** Dispatch priority, most urgent first. */
  enum Priority {
    /** Block access lists and headers that block a pivot move. */
    PIVOT,
    /** World state ranges and bytecodes. */
    STATE,
    /** Block headers, bodies and receipts below the pivot. */
    CHAIN
  }

  Protocol protocol();

  Priority priority();

  /** Short name used in logs and metrics. */
  String name();

  /** Builds the request message for the peer it is dispatched to. */
  MessageData message(EthPeer peer);

  /**
   * Decodes and validates the response. Runs on a worker thread.
   *
   * @throws InvalidResponseException if the response is wrong
   * @throws EmptyResponseException if the peer had nothing to serve
   */
  T decode(MessageData response, EthPeer peer);

  /** Returns whether the peer can serve this request, beyond protocol support. */
  default boolean canServe(final EthPeer peer) {
    return true;
  }

  /** The peer had nothing to serve; it may have pruned the data. */
  final class EmptyResponseException extends RuntimeException {
    public EmptyResponseException(final String message) {
      super(message, null, false, false);
    }
  }

  /** The response is malformed or does not match what was requested. */
  final class InvalidResponseException extends RuntimeException {
    private final boolean disconnect;

    public InvalidResponseException(final String message, final boolean disconnect) {
      super(message, null, false, false);
      this.disconnect = disconnect;
    }

    /** Returns whether the peer deserves a disconnection rather than a lower score. */
    public boolean shouldDisconnect() {
      return disconnect;
    }
  }
}
