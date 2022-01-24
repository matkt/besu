/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static org.hyperledger.besu.ethereum.eth.sync.snapsync.RequestType.TRIE_NODE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldDownloadState;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.TrieNodeDecoder;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;

public abstract class TrieNodeDataHealRequest extends SnapDataRequest {

  private final Hash nodeHash;
  private final Optional<Bytes> location;
  private long priority;
  private int depth;

  protected TrieNodeDataHealRequest(
      final Hash nodeHash, final Hash originalRootHash, final Optional<Bytes> location) {
    super(TRIE_NODE, originalRootHash);
    this.nodeHash = nodeHash;
    this.location = location;
  }

  @Override
  public Stream<SnapDataRequest> getChildRequests(final WorldStateStorage worldStateStorage) {
    if (getData().isEmpty()) {
      // If this node hasn't been downloaded yet, we can't return any child data
      return Stream.empty();
    }

    final List<Node<Bytes>> nodes =
        TrieNodeDecoder.decodeNodes(location.orElse(Bytes.EMPTY), getData().orElseThrow());
    return nodes.stream()
        .flatMap(
            node -> {
              if (nodeIsHashReferencedDescendant(node)) {
                return Stream.of(
                    createChildNodeDataRequest(Hash.wrap(node.getHash()), node.getLocation()));
              } else {
                return node.getValue()
                    .map(
                        value ->
                            getRequestsFromTrieNodeValue(
                                worldStateStorage, node.getLocation(), node.getPath(), value))
                    .orElseGet(Stream::empty);
              }
            })
        .peek(request -> request.registerParent(this));
  }

  private boolean nodeIsHashReferencedDescendant(final Node<Bytes> node) {
    return !Objects.equals(node.getHash(), nodeHash) && node.isReferencedByHash();
  }

  protected abstract SnapDataRequest createChildNodeDataRequest(
      final Hash childHash, final Optional<Bytes> location);

  protected abstract Stream<SnapDataRequest> getRequestsFromTrieNodeValue(
      final WorldStateStorage worldStateStorage,
      final Optional<Bytes> location,
      final Bytes path,
      final Bytes value);

  public Hash getNodeHash() {
    return nodeHash;
  }

  public Optional<Bytes> getLocation() {
    return location;
  }

  @Override
  public long getPriority() {
    return priority;
  }

  @Override
  public int getDepth() {
    return depth;
  }

  @Override
  protected boolean isTaskCompleted(
      final WorldDownloadState<SnapDataRequest> downloadState,
      final SnapSyncState fastSyncState,
      final EthPeers ethPeers,
      final WorldStateProofProvider worldStateProofProvider) {
    return true; // todo: did we receive valid params?????
  }

  public abstract List<Bytes> getTrieNodePath();
}
