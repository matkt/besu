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
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetAccountRangeMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import kotlin.Pair;
import kotlin.collections.ArrayDeque;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Answers GetAccountRange from the flat accounts of a cached world state. */
final class AccountRangeResponder {
  private static final Logger LOGGER = LoggerFactory.getLogger(AccountRangeResponder.class);
  static final AccountRangeMessage EMPTY =
      AccountRangeMessage.create(new HashMap<>(), new ArrayDeque<>());

  private final SnapServer.WorldStates states;
  private final long maxMillisPerRequest;

  AccountRangeResponder(final SnapServer.WorldStates states, final long maxMillisPerRequest) {
    this.states = states;
    this.maxMillisPerRequest = maxMillisPerRequest;
  }

  MessageData respond(final MessageData message) {
    StopWatch stopWatch = StopWatch.createStarted();

    final GetAccountRangeMessage getAccountRangeMessage = GetAccountRangeMessage.readFrom(message);
    final GetAccountRangeMessage.Range range = getAccountRangeMessage.range(true);
    final int maxResponseBytes = Math.min(range.responseBytes().intValue(), MAX_RESPONSE_SIZE);

    LOGGER
        .atTrace()
        .setMessage("Received getAccountRangeMessage for {} from {} to {}")
        .addArgument(
            () -> SnapServer.asLogHash(Bytes32.wrap(range.worldStateRootHash().getBytes())))
        .addArgument(() -> SnapServer.asLogHash(Bytes32.wrap(range.startKeyHash().getBytes())))
        .addArgument(() -> SnapServer.asLogHash(Bytes32.wrap(range.endKeyHash().getBytes())))
        .log();
    try {
      if (range.worldStateRootHash().equals(Hash.EMPTY_TRIE_HASH)) {
        return AccountRangeMessage.create(new HashMap<>(), List.of(MerkleTrie.EMPTY_TRIE_NODE));
      }
      return states
          .read(
              range.worldStateRootHash(),
              storage -> {
                LOGGER.trace("obtained worldstate in {}", stopWatch);
                // each account is encoded once, for its size and for the response
                final Map<Bytes32, Bytes> slimAccounts = new HashMap<>();
                ResponseSizePredicate<Pair<Bytes32, Bytes>> responseSizePredicate =
                    new ResponseSizePredicate<>(
                        "account",
                        stopWatch,
                        maxResponseBytes,
                        maxMillisPerRequest,
                        (pair) ->
                            Bytes32.SIZE
                                + slimAccounts
                                    .computeIfAbsent(pair.getFirst(), __ -> slim(pair.getSecond()))
                                    .size());

                final Bytes32 endKeyBytes = Bytes32.wrap(range.endKeyHash().getBytes());
                var shouldContinuePredicate =
                    new ExceedingPredicate<>(
                        new EndKeyExceedsPredicate(endKeyBytes).and(responseSizePredicate));

                NavigableMap<Bytes32, Bytes> accounts =
                    storage.streamFlatAccounts(
                        range.startKeyHash().getBytes(), shouldContinuePredicate);

                if (accounts.isEmpty() && shouldContinuePredicate.shouldContinue.get()) {
                  var fromNextHash =
                      range.endKeyHash().getBytes().compareTo(range.startKeyHash().getBytes()) >= 0
                          ? range.endKeyHash()
                          : range.startKeyHash();
                  // fetch next account after range, if it exists
                  LOGGER.debug(
                      "found no accounts in range, taking first value starting from {}",
                      SnapServer.asLogHash(Bytes32.wrap(fromNextHash.getBytes())));
                  accounts =
                      storage.streamFlatAccounts(fromNextHash.getBytes(), UInt256.MAX_VALUE, 1L);
                }

                final var worldStateProof =
                    new WorldStateProofProvider(new WorldStateStorageCoordinator(storage));
                final List<Bytes> proof =
                    worldStateProof.getAccountProofRelatedNodes(
                        range.worldStateRootHash(), Bytes32.wrap(range.startKeyHash().getBytes()));

                if (!accounts.isEmpty()) {
                  proof.addAll(
                      worldStateProof.getAccountProofRelatedNodes(
                          range.worldStateRootHash(), accounts.lastKey()));
                }
                accounts.replaceAll(
                    (hash, account) -> slimAccounts.computeIfAbsent(hash, __ -> slim(account)));
                var resp = AccountRangeMessage.createFromSlimAccounts(accounts, proof);
                if (accounts.isEmpty()) {
                  LOGGER.debug(
                      "returned empty account range message for {} to  {}, proof count {}",
                      SnapServer.asLogHash(Bytes32.wrap(range.startKeyHash().getBytes())),
                      SnapServer.asLogHash(Bytes32.wrap(range.endKeyHash().getBytes())),
                      proof.size());
                }
                LOGGER.debug(
                    "returned in {} account range {} to {} with {} accounts and {} proofs, resp size {} of max {}",
                    stopWatch,
                    SnapServer.asLogHash(Bytes32.wrap(range.startKeyHash().getBytes())),
                    SnapServer.asLogHash(Bytes32.wrap(range.endKeyHash().getBytes())),
                    accounts.size(),
                    proof.size(),
                    resp.getSize(),
                    maxResponseBytes);
                return resp;
              })
          .orElseGet(
              () -> {
                LOGGER.debug("returned empty account range due to worldstate not present");
                return EMPTY;
              });
    } catch (Exception ex) {
      LOGGER.error("Unexpected exception serving account range request", ex);
    }
    return EMPTY;
  }

  private static Bytes slim(final Bytes account) {
    return AccountRangeMessage.toSlimAccount(RLP.input(account));
  }
}
