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
import org.hyperledger.besu.ethereum.eth.messages.snap.ByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetByteCodesMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;

import java.util.List;
import java.util.Optional;

import kotlin.Pair;
import kotlin.collections.ArrayDeque;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Answers GetByteCodes from the code stored by code hash. */
final class ByteCodesResponder {
  private static final Logger LOGGER = LoggerFactory.getLogger(ByteCodesResponder.class);
  private static final int MAX_CODE_LOOKUPS_PER_REQUEST = 1024;
  static final ByteCodesMessage EMPTY = ByteCodesMessage.create(new ArrayDeque<>());

  private final WorldStateStorageCoordinator worldStateStorageCoordinator;
  private final long maxMillisPerRequest;

  ByteCodesResponder(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final long maxMillisPerRequest) {
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.maxMillisPerRequest = maxMillisPerRequest;
  }

  MessageData respond(final MessageData message) {
    StopWatch stopWatch = StopWatch.createStarted();

    final GetByteCodesMessage getByteCodesMessage = GetByteCodesMessage.readFrom(message);
    final int maxResponseBytes =
        Math.min(getByteCodesMessage.responseBytes(true).intValue(), MAX_RESPONSE_SIZE);
    LOGGER.atTrace().setMessage("Received get bytecodes message").log();

    try {
      List<Bytes> codeBytes = new ArrayDeque<>();
      final ExceedingPredicate<Pair<Bytes32, Bytes>> byteCodesResponseSizePredicate =
          new ExceedingPredicate<>(
              new ResponseSizePredicate<>(
                  "bytecodes",
                  stopWatch,
                  maxResponseBytes,
                  maxMillisPerRequest,
                  pair -> pair.getSecond().size()));
      int lookups = 0;
      for (Bytes32 codeHash : getByteCodesMessage.codeHashes(true)) {
        if (lookups >= MAX_CODE_LOOKUPS_PER_REQUEST) {
          break;
        }
        lookups++;
        final Optional<Bytes> maybeCode;
        if (Hash.EMPTY.getBytes().equals(codeHash)) {
          maybeCode = Optional.of(Bytes.EMPTY);
        } else {
          maybeCode = worldStateStorageCoordinator.getCode(Hash.wrap(codeHash), null);
        }

        if (maybeCode.isPresent()
            && byteCodesResponseSizePredicate.test(new Pair<>(codeHash, maybeCode.get()))) {
          codeBytes.add(maybeCode.get());
        }

        if (!byteCodesResponseSizePredicate.shouldGetMore()) {
          break;
        }
      }
      var resp = ByteCodesMessage.create(codeBytes);
      LOGGER.debug(
          "returned in {} code bytes message with {} entries, resp size {} of max {}",
          stopWatch,
          codeBytes.size(),
          resp.getSize(),
          maxResponseBytes);
      return resp;
    } catch (Exception ex) {
      LOGGER.error("Unexpected exception serving bytecodes request", ex);
      return EMPTY;
    }
  }
}
