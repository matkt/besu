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

import org.hyperledger.besu.ethereum.core.SyncBlockWithReceipts;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;

import java.util.List;

/** Running sizes of the downloaded blocks, which size the requests to about one full response. */
final class BlockSizes {

  // peers cap a body or receipt response around 2 MiB
  private static final double RESPONSE_BYTES = 2 * 1024 * 1024;
  private static final int MIN_RANGE_BLOCKS = 8;
  private static final double INITIAL_BLOCK_BYTES = 64 * 1024;

  // averages of the downloaded blocks, without and with their receipts
  private double bodyBytes = INITIAL_BLOCK_BYTES;
  private double blockBytes = INITIAL_BLOCK_BYTES;

  /** Records the sizes of downloaded blocks and returns their total bytes. */
  synchronized long measure(final List<SyncBlockWithReceipts> blocks) {
    long bodies = 0;
    long receipts = 0;
    for (final SyncBlockWithReceipts block : blocks) {
      bodies += block.getBlock().getBody().getRlp().size();
      for (final SyncTransactionReceipt receipt : block.getReceipts()) {
        receipts += receipt.getRlpBytes() == null ? 0 : receipt.getRlpBytes().size();
      }
    }
    bodyBytes = 0.8 * bodyBytes + 0.2 * bodies / blocks.size();
    blockBytes = 0.8 * blockBytes + 0.2 * (bodies + receipts) / blocks.size();
    return bodies + receipts;
  }

  /** Returns the average bytes of a block with its receipts. */
  synchronized double blockBytes() {
    return blockBytes;
  }

  /** Returns how many blocks fill about one body response, so many when they are small. */
  synchronized int bodyBlocks(final int max) {
    final double blocks = RESPONSE_BYTES / Math.max(1, bodyBytes);
    return (int) Math.max(Math.min(MIN_RANGE_BLOCKS, max), Math.min(max, blocks));
  }

  /** Returns how many blocks fill about one receipt response. */
  synchronized int receiptBlocks(final int max) {
    final double receiptBytes = blockBytes - bodyBytes;
    return (int) Math.max(1, receiptBytes > 0 ? Math.min(max, RESPONSE_BYTES / receiptBytes) : max);
  }
}
