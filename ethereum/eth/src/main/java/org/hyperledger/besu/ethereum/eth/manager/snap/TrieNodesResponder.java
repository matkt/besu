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
import org.hyperledger.besu.ethereum.eth.messages.snap.GetTrieNodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.TrieNodesMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;

import java.util.ArrayList;
import java.util.Optional;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Answers GetTrieNodes of snap/1 from the trie nodes of a cached world state. */
final class TrieNodesResponder {
  private static final Logger LOGGER = LoggerFactory.getLogger(TrieNodesResponder.class);
  static final TrieNodesMessage EMPTY = TrieNodesMessage.create(new ArrayList<>());

  private final SnapServer.WorldStates states;
  private final long maxMillisPerRequest;

  TrieNodesResponder(final SnapServer.WorldStates states, final long maxMillisPerRequest) {
    this.states = states;
    this.maxMillisPerRequest = maxMillisPerRequest;
  }

  MessageData respond(final MessageData message) {
    StopWatch stopWatch = StopWatch.createStarted();

    final GetTrieNodesMessage getTrieNodesMessage = GetTrieNodesMessage.readFrom(message);
    final GetTrieNodesMessage.TrieNodesPaths triePaths = getTrieNodesMessage.paths(true);
    final int maxResponseBytes = Math.min(triePaths.responseBytes().intValue(), MAX_RESPONSE_SIZE);
    LOGGER.atTrace().setMessage("Received get trie nodes message").log();

    try {
      return states
          .read(
              triePaths.worldStateRootHash(),
              storage -> {
                LOGGER.trace("obtained worldstate in {}", stopWatch);
                ArrayList<Bytes> trieNodes = new ArrayList<>();
                final ExceedingPredicate<Bytes> trieNodesResponseSizePredicate =
                    new ExceedingPredicate<>(
                        new ResponseSizePredicate<>(
                            "trie nodes",
                            stopWatch,
                            maxResponseBytes,
                            maxMillisPerRequest,
                            trieNode -> trieNode.size()));
                outerLoop:
                for (var group : triePaths.paths()) {
                  final var pathIter = group.iterator();
                  if (!pathIter.hasNext()) {
                    LOGGER.debug("returned empty trie nodes message due to invalid path");
                    return EMPTY;
                  }
                  final Bytes firstPath = pathIter.next();
                  if (!pathIter.hasNext()) {
                    // single path: compact-encoded account trie node
                    final Bytes location = CompactEncoding.decode(firstPath);
                    var optStorage = storage.getTrieNodeUnsafe(location);
                    if (optStorage.isEmpty() && location.isEmpty()) {
                      optStorage = Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
                    }
                    var trieNode = optStorage.orElse(Bytes.EMPTY);
                    if (trieNodesResponseSizePredicate.test(trieNode)) {
                      trieNodes.add(trieNode);
                    }
                    if (!trieNodesResponseSizePredicate.shouldGetMore()) {
                      break outerLoop;
                    }
                  } else {
                    // multiple paths: first is account hash, rest are compact-encoded storage paths
                    final Bytes32 accountPrefix = Bytes32.leftPad(firstPath);
                    var optAccount = storage.getAccount(Hash.wrap(accountPrefix));
                    if (optAccount.isEmpty()) {
                      continue;
                    }
                    while (pathIter.hasNext()) {
                      final Bytes path = pathIter.next();
                      final Bytes location = CompactEncoding.decode(path);
                      var optStorage =
                          storage.getTrieNodeUnsafe(Bytes.concatenate(accountPrefix, location));
                      if (optStorage.isEmpty() && location.isEmpty()) {
                        optStorage = Optional.of(MerkleTrie.EMPTY_TRIE_NODE);
                      }
                      var trieNode = optStorage.orElse(Bytes.EMPTY);
                      if (trieNodesResponseSizePredicate.test(trieNode)) {
                        trieNodes.add(trieNode);
                      }
                      if (!trieNodesResponseSizePredicate.shouldGetMore()) {
                        break outerLoop;
                      }
                    }
                  }
                }
                var resp = TrieNodesMessage.create(trieNodes);
                LOGGER.debug(
                    "returned in {} trie nodes message with {} entries, resp size {} of max {}",
                    stopWatch,
                    trieNodes.size(),
                    resp.getSize(),
                    maxResponseBytes);
                return resp;
              })
          .orElseGet(
              () -> {
                LOGGER.debug("returned empty trie nodes message due to missing worldstate");
                return EMPTY;
              });
    } catch (Exception ex) {
      LOGGER.error("Unexpected exception serving trienodes request", ex);
      return EMPTY;
    }
  }
}
