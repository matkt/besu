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
package org.hyperledger.besu.ethereum.trie.sparse;

import static java.lang.String.format;

import org.hyperledger.besu.ethereum.trie.patricia.LeafNode;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeFactory;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.StoredNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.plugin.data.Hash;

@SuppressWarnings("unused")
public class StoredNodeFactory<V> implements NodeFactory<V> {

  @SuppressWarnings("rawtypes")
  public static final NullNode NULL_NODE = NullNode.instance();

  @SuppressWarnings("rawtypes")
  private static final int NB_CHILD = 2;

  private final NodeLoader nodeLoader;
  private final Function<V, Bytes> valueSerializer;
  private final Function<Bytes, V> valueDeserializer;

  public StoredNodeFactory(
      final NodeLoader nodeLoader,
      final Function<V, Bytes> valueSerializer,
      final Function<Bytes, V> valueDeserializer) {
    this.nodeLoader = nodeLoader;
    this.valueSerializer = valueSerializer;
    this.valueDeserializer = valueDeserializer;
  }

  @Override
  public Node<V> createExtension(final Bytes path, final Node<V> child) {
    throw new UnsupportedOperationException("cannot create extension in the sparse merkle trie");
  }

  @SuppressWarnings("unchecked")
  @Override
  public Node<V> createBranch(
      final byte leftIndex, final Node<V> left, final byte rightIndex, final Node<V> right) {
    assert (leftIndex <= NB_CHILD);
    assert (rightIndex <= NB_CHILD);
    assert (leftIndex != rightIndex);

    final ArrayList<Node<V>> children =
        new ArrayList<>(Collections.nCopies(NB_CHILD, (Node<V>) NULL_NODE));

    if (leftIndex == NB_CHILD) {
      children.set(rightIndex, right);
      return createBranch(children, left.getValue());
    } else if (rightIndex == NB_CHILD) {
      children.set(leftIndex, left);
      return createBranch(children, right.getValue());
    } else {
      children.set(leftIndex, left);
      children.set(rightIndex, right);
      return createBranch(children, Optional.empty());
    }
  }

  @Override
  public Node<V> createBranch(final List<Node<V>> children, final Optional<V> value) {
    return handleNewNode(new BranchNode<>(children, value, this, valueSerializer));
  }

  @Override
  public Node<V> createLeaf(final Bytes path, final V value) {
    return handleNewNode(new LeafNode<>(path, value, this, valueSerializer));
  }

  private Node<V> handleNewNode(final Node<V> node) {
    node.markDirty();
    return node;
  }

  @Override
  public Optional<Node<V>> retrieve(final Bytes location, final Bytes32 hash)
      throws MerkleTrieException {
    return nodeLoader
        .getNode(location, hash)
        .map(
            encodedBytes -> {
              final Node<V> node =
                  decode(location, encodedBytes, () -> format("Invalid RLP value for hash %s", hash));
              // recalculating the node.hash() is expensive, so we only do this as an assertion
              assert (hash.equals(node.getHash()))
                  : "Node hash " + node.getHash() + " not equal to expected " + hash;
              return node;
            });
  }

  public Node<V> decode(final Bytes location, final Bytes rlp) {
    return decode(location, rlp, () -> String.format("Failed to decode value %s", rlp.toString()));
  }


  private Node<V> decode(
      final Bytes location, final Bytes input, final Supplier<String> errMessage) {

    int type = input.size()==Hash.SIZE*2?1:2; // a leaf will only be bigger (leaf opening) or smaller (zero leaf)

    switch (type){
      case 1 -> {
        return decodeBranch(location, input, errMessage);
      }
      case 2 -> {
        return decodeLeaf(location, input, errMessage);
      }
      default ->
        throw new MerkleTrieException(
                errMessage.get() + format(": invalid node %s", type));
    }
  }

  @SuppressWarnings("unchecked")
  protected BranchNode<V> decodeBranch(
      final Bytes location, final Bytes input, final Supplier<String> errMessage) {
    final ArrayList<Node<V>> children = new ArrayList<>(NB_CHILD);
    final int nbChilds = input.size()/Hash.SIZE;
    for (int i = 0; i < nbChilds; i++) {
      final Bytes32 childHash =  Bytes32.wrap(input.slice(0,Hash.SIZE));
      children.add(
              new StoredNode<>(
                      this,
                      location == null ? null : Bytes.concatenate(location, Bytes.of((byte) i)),
                      childHash));
    }
    return new BranchNode<>(location, children, Optional.empty(), this, valueSerializer);
  }

  protected Node<V> decodeLeaf(
      final Bytes location,
      final Bytes input,
      final Supplier<String> errMessage) {
    if(input.equals(Bytes32.ZERO)){
      return EmptyLeafNode.instance();
    }
    throw new MerkleTrieException(errMessage.get() + ": leaf has null value");
  }

}
