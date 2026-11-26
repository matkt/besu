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

import static org.hyperledger.besu.ethereum.eth.manager.snap.ResponseLimits.ExceedingPredicate;
import static org.hyperledger.besu.ethereum.eth.manager.snap.ResponseLimits.MAX_RESPONSE_SIZE;
import static org.hyperledger.besu.ethereum.eth.manager.snap.ResponseLimits.ResponseSizePredicate;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.eth.messages.snap.BlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetBlockAccessListsMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Answers GetBlockAccessLists of snap/2 from the block access lists the chain holds. */
final class BlockAccessListsResponder {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlockAccessListsResponder.class);
  static final BlockAccessListsMessage EMPTY = BlockAccessListsMessage.create(List.of());

  private final Optional<Blockchain> maybeBlockchain;
  private final long maxMillisPerRequest;

  BlockAccessListsResponder(
      final Optional<Blockchain> maybeBlockchain, final long maxMillisPerRequest) {
    this.maybeBlockchain = maybeBlockchain;
    this.maxMillisPerRequest = maxMillisPerRequest;
  }

  MessageData respond(final MessageData message) {
    final StopWatch stopWatch = StopWatch.createStarted();
    int requestedCount = 0;
    try {
      final GetBlockAccessListsMessage getBlockAccessLists =
          GetBlockAccessListsMessage.readFrom(message);
      final Iterable<Hash> blockHashes = getBlockAccessLists.blockHashes(true);
      final int maxResponseBytes =
          Math.min(getBlockAccessLists.responseBytes(true).intValue(), MAX_RESPONSE_SIZE);

      final List<Optional<Bytes>> blockAccessLists = new ArrayList<>();

      if (maybeBlockchain.isPresent()) {
        final var blockchain = maybeBlockchain.get();
        final ExceedingPredicate<Optional<Bytes>> blockAccessListsResponseSizePredicate =
            new ExceedingPredicate<>(
                new ResponseSizePredicate<>(
                    "block access lists",
                    stopWatch,
                    maxResponseBytes,
                    maxMillisPerRequest,
                    BlockAccessListsResponder::encodedSize));
        for (final Hash blockHash : blockHashes) {
          requestedCount++;
          final Optional<Bytes> maybeBlockAccessListRlp =
              blockchain.getBlockAccessListRlp(blockHash);

          if (blockAccessListsResponseSizePredicate.test(maybeBlockAccessListRlp)) {
            blockAccessLists.add(maybeBlockAccessListRlp);
          }

          if (!blockAccessListsResponseSizePredicate.shouldGetMore()) {
            break;
          }
        }
      }

      if (LOGGER.isDebugEnabled()) {
        final long unavailable = blockAccessLists.stream().filter(Optional::isEmpty).count();
        LOGGER
            .atDebug()
            .setMessage(
                "Served block access lists request: requested={}, returned={}, unavailable={}, took {} ms")
            .addArgument(requestedCount)
            .addArgument(blockAccessLists.size() - unavailable)
            .addArgument(unavailable)
            .addArgument(stopWatch.getTime())
            .log();
      }
      return BlockAccessListsMessage.createFromRlp(blockAccessLists);
    } catch (final RuntimeException e) {
      LOGGER.error("Unexpected exception serving block access lists request", e);
      return EMPTY;
    }
  }

  private static int encodedSize(final Optional<Bytes> maybeBlockAccessListRlp) {
    return maybeBlockAccessListRlp.map(Bytes::size).orElse(1);
  }
}
