/*
 * Copyright contributors to Hyperledger Besu
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

import static java.util.Collections.emptyMap;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.PendingPeerRequest;
import org.hyperledger.besu.ethereum.eth.manager.task.AbstractPeerRequestTask;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV1;
import org.hyperledger.besu.ethereum.eth.messages.snap.TrieNodes;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import kotlin.collections.ArrayDeque;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tuweni.bytes.Bytes;

public class GetTrieNodeFromPeerTask extends AbstractPeerRequestTask<Map<Hash, Bytes>> {

  private static final Logger LOG = LogManager.getLogger();

  private final List<Hash> hashes;
  private final List<List<Bytes>> paths;
  private final BlockHeader blockHeader;

  private GetTrieNodeFromPeerTask(
      final EthContext ethContext,
      final List<Hash> hashes,
      final List<List<Bytes>> paths,
      final BlockHeader blockHeader,
      final MetricsSystem metricsSystem) {
    super(ethContext, SnapV1.TRIE_NODES, metricsSystem);
    this.hashes = hashes;
    this.paths = paths;
    this.blockHeader = blockHeader;
  }

  public static GetTrieNodeFromPeerTask forTrieNodes(
      final EthContext ethContext,
      final List<Hash> hashes,
      final List<List<Bytes>> paths,
      final BlockHeader blockHeader,
      final MetricsSystem metricsSystem) {
    return new GetTrieNodeFromPeerTask(ethContext, hashes, paths, blockHeader, metricsSystem);
  }

  @Override
  protected PendingPeerRequest sendRequest() {
    return sendRequestToPeer(
        peer -> {
          LOG.trace("Requesting trie nodes from {} : {} .", paths.size(), peer);
          return peer.getSnapTrieNode(blockHeader.getStateRoot(), paths);
        },
        blockHeader.getNumber());
  }

  @Override
  protected Optional<Map<Hash, Bytes>> processResponse(
      final boolean streamClosed, final MessageData message, final EthPeer peer) {
    if (streamClosed) {
      // We don't record this as a useless response because it's impossible to know if a peer has
      // the data we're requesting.
      return Optional.of(emptyMap());
    }
    final TrieNodes trieNodes = TrieNodes.readFrom(message);
    final ArrayDeque<Bytes> nodes = trieNodes.nodes(true);
    if (nodes.size() > paths.size()) {
      // Can't be the response to our request
      return Optional.empty();
    }
    System.out.println("node size " + nodes.size());
    return mapNodeDataByPath(nodes);
  }

  private Optional<Map<Hash, Bytes>> mapNodeDataByPath(final ArrayDeque<Bytes> nodeData) {
    final Map<Hash, Bytes> nodeDataByPath = new HashMap<>();
    for (int i = 0; i < nodeData.size(); i++) {
      Hash hash = Hash.hash(nodeData.get(i));
      if (hashes.get(i).equals(hash)) {
        nodeDataByPath.put(hash, nodeData.get(i));
      }
    }
    return Optional.of(nodeDataByPath);
  }
}
