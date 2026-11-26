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

import static org.hyperledger.besu.ethereum.eth.manager.snap.ResponseLimits.EndKeyExceedsPredicate;
import static org.hyperledger.besu.ethereum.eth.manager.snap.ResponseLimits.ExceedingPredicate;
import static org.hyperledger.besu.ethereum.eth.manager.snap.ResponseLimits.MAX_RESPONSE_SIZE;
import static org.hyperledger.besu.ethereum.eth.manager.snap.ResponseLimits.ResponseSizePredicate;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetStorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import kotlin.Pair;
import kotlin.collections.ArrayDeque;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Answers GetStorageRanges from the flat slots of a cached world state. */
final class StorageRangeResponder {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageRangeResponder.class);
  private static final int MAX_STORAGE_RANGE_ACCOUNTS_PER_REQUEST = 4096;
  static final StorageRangeMessage EMPTY =
      StorageRangeMessage.create(new ArrayDeque<>(), Collections.emptyList());

  private final SnapServer.WorldStates states;
  private final long maxMillisPerRequest;

  StorageRangeResponder(final SnapServer.WorldStates states, final long maxMillisPerRequest) {
    this.states = states;
    this.maxMillisPerRequest = maxMillisPerRequest;
  }

  MessageData respond(final MessageData message) {
    StopWatch stopWatch = StopWatch.createStarted();

    final GetStorageRangeMessage getStorageRangeMessage = GetStorageRangeMessage.readFrom(message);
    final GetStorageRangeMessage.StorageRange range = getStorageRangeMessage.range(true);
    final int maxResponseBytes = Math.min(range.responseBytes().intValue(), MAX_RESPONSE_SIZE);

    LOGGER
        .atTrace()
        .setMessage("Receive get storage range message size {} from {} to {} for {}")
        .addArgument(message::getSize)
        .addArgument(() -> SnapServer.asLogHash(Bytes32.wrap(range.startKeyHash().getBytes())))
        .addArgument(() -> SnapServer.asLogHash(Bytes32.wrap(range.endKeyHash().getBytes())))
        .addArgument(
            () ->
                StreamSupport.stream(range.accountHashes().spliterator(), false)
                    .map(SnapServer::asLogHash)
                    .collect(Collectors.joining(",", "[", "]")))
        .log();
    try {
      return states
          .read(
              range.worldStateRootHash(),
              storage -> {
                LOGGER.trace("obtained worldstate in {}", stopWatch);
                // reusable predicate to limit by rec count and bytes:
                var responsePredicate =
                    new ResponseSizePredicate<Pair<Bytes32, Bytes>>(
                        "storage",
                        stopWatch,
                        maxResponseBytes,
                        maxMillisPerRequest,
                        (pair) -> {
                          var slotRlpOutput = new BytesValueRLPOutput();
                          slotRlpOutput.startList();
                          slotRlpOutput.writeBytes(pair.getFirst());
                          slotRlpOutput.writeBytes(pair.getSecond());
                          slotRlpOutput.endList();
                          return slotRlpOutput.encodedSize();
                        });

                // only honor start and end hash if request is for a single account's storage:
                Bytes32 startKeyBytes, endKeyBytes;
                boolean isPartialRange = false;
                if (range.hasMultipleAccountHashes()) {
                  startKeyBytes = Bytes32.ZERO;
                  endKeyBytes = Bytes32.wrap(SnapServer.HASH_LAST.getBytes());
                } else {
                  startKeyBytes = Bytes32.wrap(range.startKeyHash().getBytes());
                  endKeyBytes = Bytes32.wrap(range.endKeyHash().getBytes());
                  isPartialRange =
                      !(startKeyBytes.equals(Bytes32.wrap(Hash.ZERO.getBytes()))
                          && endKeyBytes.equals(Bytes32.wrap(SnapServer.HASH_LAST.getBytes())));
                }

                ArrayDeque<NavigableMap<Bytes32, Bytes>> collectedStorages = new ArrayDeque<>();
                List<Bytes> proofNodes = new ArrayList<>();
                final var worldStateProof =
                    new WorldStateProofProvider(new WorldStateStorageCoordinator(storage));

                int accountLookups = 0;
                for (var forAccountHash : range.accountHashes()) {
                  if (accountLookups >= MAX_STORAGE_RANGE_ACCOUNTS_PER_REQUEST) {
                    break;
                  }
                  accountLookups++;
                  var predicate =
                      new ExceedingPredicate<>(
                          new EndKeyExceedsPredicate(endKeyBytes).and(responsePredicate));
                  var accountStorages =
                      storage.streamFlatStorages(
                          Hash.wrap(forAccountHash), startKeyBytes, predicate);

                  // address partial range queries that return empty
                  if (accountStorages.isEmpty() && isPartialRange) {
                    // fetch next slot after range, if it exists
                    LOGGER.debug(
                        "found no slots in range, taking first value starting from {}",
                        SnapServer.asLogHash(Bytes32.wrap(range.endKeyHash().getBytes())));
                    accountStorages =
                        storage.streamFlatStorages(
                            Hash.wrap(forAccountHash),
                            Bytes32.wrap(range.endKeyHash().getBytes()),
                            UInt256.MAX_VALUE,
                            1L);
                  }

                  // don't send empty storage ranges
                  if (!accountStorages.isEmpty()) {
                    collectedStorages.add(accountStorages);
                  }

                  // if a partial storage range was requested, or we interrupted storage due to
                  // request limits, send proofs:
                  if (isPartialRange || !predicate.shouldGetMore()) {
                    // send a proof for the left side range origin
                    proofNodes.addAll(
                        worldStateProof.getStorageProofRelatedNodes(
                            Bytes32.wrap(getAccountStorageRoot(forAccountHash, storage).getBytes()),
                            forAccountHash,
                            startKeyBytes));
                    if (!accountStorages.isEmpty()) {
                      // send a proof for the last key on the right
                      proofNodes.addAll(
                          worldStateProof.getStorageProofRelatedNodes(
                              Bytes32.wrap(
                                  getAccountStorageRoot(forAccountHash, storage).getBytes()),
                              forAccountHash,
                              accountStorages.lastKey()));
                    }
                  }

                  if (!predicate.shouldGetMore()) {
                    break;
                  }
                }

                var resp = StorageRangeMessage.create(collectedStorages, proofNodes);
                if (LOGGER.isTraceEnabled()) {
                  Bytes32 firstAccountHash = null, lastAccountHash = null;
                  for (var h : range.accountHashes()) {
                    if (firstAccountHash == null) firstAccountHash = h;
                    lastAccountHash = h;
                  }
                  LOGGER.trace(
                      "returned in {} storage {} to {} range {} to {} with {} storages and {} proofs, resp size {} of max {}",
                      stopWatch,
                      SnapServer.asLogHash(
                          firstAccountHash == null
                              ? Bytes32.wrap(Hash.ZERO.getBytes())
                              : firstAccountHash),
                      SnapServer.asLogHash(
                          lastAccountHash == null
                              ? Bytes32.wrap(Hash.ZERO.getBytes())
                              : lastAccountHash),
                      SnapServer.asLogHash(Bytes32.wrap(range.startKeyHash().getBytes())),
                      SnapServer.asLogHash(Bytes32.wrap(range.endKeyHash().getBytes())),
                      collectedStorages.size(),
                      proofNodes.size(),
                      resp.getSize(),
                      maxResponseBytes);
                }
                return resp;
              })
          .orElseGet(
              () -> {
                LOGGER.debug("returned empty storage range due to missing worldstate");
                return EMPTY;
              });
    } catch (Exception ex) {
      LOGGER.error("Unexpected exception serving storage range request", ex);
      return EMPTY;
    }
  }

  private static Hash getAccountStorageRoot(
      final Bytes32 accountHash, final BonsaiWorldStateKeyValueStorage storage) {
    return storage
        .getTrieNodeUnsafe(Bytes.concatenate(accountHash, Bytes.EMPTY))
        .map(Hash::hash)
        .orElse(Hash.EMPTY_TRIE_HASH);
  }
}
