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
package org.hyperledger.besu.ethereum.eth.sync.worldstate;

import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.TrieNodeDecoder;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;

abstract class TrieNodeDataRequest extends NodeDataRequest {

  protected TrieNodeDataRequest(
      final RequestType requestType, final Hash hash, final Optional<Bytes> location) {
    super(requestType, hash, location);
  }

  @Override
  public Stream<NodeDataRequest> getChildRequests() {
    if (getData() == null) {
      // If this node hasn't been downloaded yet, we can't return any child data
      return Stream.empty();
    }

    final List<Node<Bytes>> nodes =
        TrieNodeDecoder.decodeNodes(getLocation().orElse(null), getData());
    return nodes.stream()
        .flatMap(
            node -> {
              if (nodeIsHashReferencedDescendant(node)) {
                return Stream.of(
                    createChildNodeDataRequest(Hash.wrap(node.getHash()), node.getLocation()));
              } else {
                return node.getValue()
                    .map(this::getRequestsFromTrieNodeValue)
                    .orElseGet(Stream::empty);
              }
            });
  }

  private boolean nodeIsHashReferencedDescendant(final Node<Bytes> node) {
    return !Objects.equals(node.getHash(), getHash()) && node.isReferencedByHash();
  }

  protected abstract NodeDataRequest createChildNodeDataRequest(
      final Hash childHash, final Optional<Bytes> location);

  protected abstract Stream<NodeDataRequest> getRequestsFromTrieNodeValue(final Bytes value);
}
