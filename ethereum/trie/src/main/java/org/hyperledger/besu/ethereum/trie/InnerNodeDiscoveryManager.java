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
package org.hyperledger.besu.ethereum.trie;

import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;

public class InnerNodeDiscoveryManager<V> extends StoredNodeFactory<V> {

  public List<Bytes> innerNodes = new ArrayList<>();

  private final Bytes startKeyHash, endKeyHash;

  private boolean allowMissingElementInRange;

  public InnerNodeDiscoveryManager(
      final NodeLoader nodeLoader,
      final Function<V, Bytes> valueSerializer,
      final Function<Bytes, V> valueDeserializer,
      final Bytes32 startKeyHash,
      final Bytes32 endKeyHash,
      final boolean allowMissingElementInRange) {
    super(nodeLoader, valueSerializer, valueDeserializer);
    this.startKeyHash = createPath(startKeyHash);
    this.endKeyHash = createPath(endKeyHash);
    this.allowMissingElementInRange = allowMissingElementInRange;
  }

  public void setAllowMissingElementInRange(final boolean allowMissingElementInRange) {
    this.allowMissingElementInRange = allowMissingElementInRange;
  }

  @Override
  protected Node<V> decodeExtension(
      final Bytes location,
      final Bytes path,
      final RLPInput valueRlp,
      final Supplier<String> errMessage) {
    final ExtensionNode<V> vNode =
        (ExtensionNode<V>) super.decodeExtension(location, path, valueRlp, errMessage);
    if (isInRange(Bytes.concatenate(location, Bytes.of(0)))) {
      innerNodes.add(Bytes.concatenate(location, Bytes.of(0)));
    }
    return vNode;
  }

  @Override
  protected BranchNode<V> decodeBranch(
      final Bytes location, final RLPInput nodeRLPs, final Supplier<String> errMessage) {
    final BranchNode<V> vBranchNode = super.decodeBranch(location, nodeRLPs, errMessage);
    final List<Node<V>> children = vBranchNode.getChildren();
    for (int i = 0; i < children.size(); i++) {
      if (isInRange(Bytes.concatenate(location, Bytes.of(i)))) {
        innerNodes.add(Bytes.concatenate(location, Bytes.of(i)));
      }
    }
    return vBranchNode;
  }

  @Override
  protected LeafNode<V> decodeLeaf(
      final Bytes location,
      final Bytes path,
      final RLPInput valueRlp,
      final Supplier<String> errMessage) {
    final LeafNode<V> vLeafNode = super.decodeLeaf(location, path, valueRlp, errMessage);
    final Bytes concatenatePath = Bytes.concatenate(location, path);
    if (isInRange(concatenatePath.slice(0, concatenatePath.size() - 1))) {
      innerNodes.add(Bytes.concatenate(location, path));
    }
    return vLeafNode;
  }

  @Override
  public Optional<Node<V>> retrieve(final Bytes location, final Bytes32 hash)
      throws MerkleTrieException {
    return super.retrieve(location, hash)
        .or(
            () -> {
              if (!allowMissingElementInRange && isInRange(location)) {
                return Optional.empty();
              }
              return Optional.of(new MissingNode<>(hash, location));
            });
  }

  public List<Bytes> getInnerNodes() {
    return innerNodes;
  }

  private boolean isInRange(final Bytes location) {
    return location.size() >= startKeyHash.size()
        && location.size() <= endKeyHash.size()
        && location.compareTo(startKeyHash) >= 0
        && location.compareTo(endKeyHash) <= 0;
  }

  private Bytes createPath(final Bytes bytes) {
    final MutableBytes path = MutableBytes.create(bytes.size() * 2);
    int j = 0;
    for (int i = 0; i < bytes.size(); i += 1, j += 2) {
      final byte b = bytes.get(i);
      path.set(j, (byte) ((b >>> 4) & 0x0f));
      path.set(j + 1, (byte) (b & 0x0f));
    }
    return path;
  }
}
