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
package org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch;

import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import org.apache.tuweni.bytes.Bytes;

/**
 * A block access list prefetch in progress. Once cancelled it reads nothing more: the lots that
 * have not started yet are skipped.
 */
public final class BalPrefetch {

  private volatile boolean cancelled;

  /** The encoding of the block access list it reads the state of, once it got it. */
  private volatile Bytes encodedBlockAccessList;

  /** Stops the prefetch, e.g. once its block is processed or rejected. */
  public void cancel() {
    cancelled = true;
  }

  public boolean isCancelled() {
    return cancelled;
  }

  void readsFrom(final Bytes encodedBlockAccessList) {
    this.encodedBlockAccessList = encodedBlockAccessList;
  }

  /**
   * Whether it reads the state of {@code blockAccessList}: one with the same encoding. A prefetch
   * started from the encoding of a payload's list is for the list decoded from that payload.
   *
   * @param blockAccessList the block access list of a block
   * @return whether this prefetch reads the state of that block access list
   */
  public boolean isFor(final BlockAccessList blockAccessList) {
    final Bytes encoded = encodedBlockAccessList;
    return encoded != null && blockAccessList.rawRlp().map(encoded::equals).orElse(false);
  }
}
