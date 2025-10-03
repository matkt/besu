/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.trie.patricia;

import static java.lang.String.format;

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
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

public class StoredNodeFactory<V> implements NodeFactory<V> {
  @SuppressWarnings("rawtypes")
  private static final NullNode NULL_NODE = NullNode.instance();

  private static final int RADIX = 16;

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
    return handleNewNode(new ExtensionNode<>(path, child, this));
  }

  @SuppressWarnings("unchecked")
  @Override
  public Node<V> createBranch(
      final byte leftIndex, final Node<V> left, final byte rightIndex, final Node<V> right) {
    assert (leftIndex <= RADIX);
    assert (rightIndex <= RADIX);
    assert (leftIndex != rightIndex);

    final ArrayList<Node<V>> children =
        new ArrayList<>(Collections.nCopies(RADIX, (Node<V>) NULL_NODE));

    if (leftIndex == RADIX) {
      children.set(rightIndex, right);
      return createBranch(children, left.getValue());
    } else if (rightIndex == RADIX) {
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
            rlp -> {
              final Node<V> node =
                  decode(location, hash, rlp, () -> format("Invalid RLP value for hash %s", hash));
              node.setHash(hash);
              node.setEncodedBytes(rlp);
              // recalculating the node.hash() is expensive, so we only do this as an assertion
              assert (hash.equals(node.getHash()))
                  : "Node hash " + node.getHash() + " not equal to expected " + hash;
              return node;
            });
  }

  public Node<V> decode(final Bytes location, final Bytes32 hash, final Bytes rlp) {
    return decode(location,hash, rlp, () -> String.format("Failed to decode value %s", rlp.toString()));
  }

  private Node<V> decode(final Bytes location, final Bytes32 hash, final Bytes rlp, final Supplier<String> errMessage)
      throws MerkleTrieException {
    try {
      return decode(location, hash, RLP.input(rlp), errMessage);
    } catch (final RLPException ex) {
      throw new MerkleTrieException(errMessage.get(), ex);
    }
  }

  private Node<V> decode(
      final Bytes location, final Bytes32 hash, final RLPInput nodeRLPs, final Supplier<String> errMessage) {
    final int nodesCount = nodeRLPs.enterList();
    switch (nodesCount) {
      case 1:
        final NullNode<V> nullNode = decodeNull(nodeRLPs, errMessage);
        nodeRLPs.leaveList();
        return nullNode;

      case 2:
        final Bytes encodedPath = nodeRLPs.readBytes();
        final Bytes path;
        try {
          path = CompactEncoding.decode(encodedPath);
        } catch (final IllegalArgumentException ex) {
          throw new MerkleTrieException(errMessage.get() + ": invalid path " + encodedPath, ex);
        }

        final int size = path.size();
        if (size > 0 && path.get(size - 1) == CompactEncoding.LEAF_TERMINATOR) {
          final LeafNode<V> leafNode = decodeLeaf(location, hash,path, nodeRLPs, errMessage);
          nodeRLPs.leaveList();
          return leafNode;
        } else {
          final Node<V> extensionNode = decodeExtension(location, hash,path, nodeRLPs, errMessage);
          nodeRLPs.leaveList();
          return extensionNode;
        }

      case (RADIX + 1):
        final BranchNode<V> branchNode = decodeBranch(location, hash,nodeRLPs, errMessage);
        nodeRLPs.leaveList();
        return branchNode;

      default:
        throw new MerkleTrieException(
            errMessage.get() + format(": invalid list size %s", nodesCount));
    }
  }

  protected Node<V> decodeExtension(
      final Bytes location,
      final Bytes32 hash,
      final Bytes path,
      final RLPInput valueRlp,
      final Supplier<String> errMessage) {
    final RLPInput extensionRlp = valueRlp.readAsRlp();
    if (extensionRlp.nextIsList()) {
      final Node<V> childNode =
          decode(location == null ? null : Bytes.concatenate(location, path), hash, extensionRlp, errMessage);
      return new ExtensionNode<>(location, path, childNode, this);
    } else {
        final RLPInput childRlp = extensionRlp.readAsRlp();
        final Bytes32 childHash = childRlp.readBytes32();
      final StoredNode<V> childNode =
          new StoredNode<>(
              this, location == null ? null : Bytes.concatenate(location, path), childHash, childRlp.raw());
      return new ExtensionNode<>(location, hash, path, extensionRlp.raw(), childNode, this);
    }
  }

  @SuppressWarnings("unchecked")
  protected BranchNode<V> decodeBranch(
      final Bytes location, final Bytes32 hash, final RLPInput branchRLP, final Supplier<String> errMessage) {
    final ArrayList<Node<V>> children = new ArrayList<>(RADIX);
    for (int i = 0; i < RADIX; ++i) {
      if (branchRLP.nextIsNull()) {
        branchRLP.skipNext();
        children.add(NULL_NODE);
      } else if (branchRLP.nextIsList()) {
        final Node<V> child =
            decode(
                location == null ? null : Bytes.concatenate(location, Bytes.of((byte) i)),
                hash,
                branchRLP,
                errMessage);
        children.add(child);
      } else {
          final RLPInput childRlP = branchRLP.readAsRlp();
          final Bytes32 childHash = childRlP.readBytes32();
        children.add(
            new StoredNode<>(
                this,
                location == null ? null : Bytes.concatenate(location, Bytes.of((byte) i)),
                childHash, childRlP.raw()));
      }
    }

    final Optional<V> value;
    if (branchRLP.nextIsNull()) {
      branchRLP.skipNext();
      value = Optional.empty();
    } else {
      value = Optional.of(decodeValue(branchRLP, errMessage));
    }

    return new BranchNode<>(location, hash , children, value, branchRLP.raw(), this, valueSerializer);
  }

  protected LeafNode<V> decodeLeaf(
      final Bytes location,
      final Bytes32 hash,
      final Bytes path,
      final RLPInput valueRlp,
      final Supplier<String> errMessage) {
    if (valueRlp.nextIsNull()) {
      throw new MerkleTrieException(errMessage.get() + ": leaf has null value");
    }
    final V value = decodeValue(valueRlp, errMessage);
    return new LeafNode<>(location, hash, path, value,valueRlp.raw(), this, valueSerializer);
  }

  @SuppressWarnings("unchecked")
  private NullNode<V> decodeNull(final RLPInput nodeRLPs, final Supplier<String> errMessage) {
    if (!nodeRLPs.nextIsNull()) {
      throw new MerkleTrieException(errMessage.get() + ": list size 1 but not null");
    }
    nodeRLPs.skipNext();
    return NULL_NODE;
  }

  private V decodeValue(final RLPInput valueRlp, final Supplier<String> errMessage) {
    final Bytes bytes;
    try {
      bytes = valueRlp.readBytes();
    } catch (final RLPException ex) {
      throw new MerkleTrieException(
          errMessage.get() + ": failed decoding value rlp " + valueRlp, ex);
    }
    return deserializeValue(errMessage, bytes);
  }

  private V deserializeValue(final Supplier<String> errMessage, final Bytes bytes) {
    final V value;
    try {
      value = valueDeserializer.apply(bytes);
    } catch (final IllegalArgumentException ex) {
      throw new MerkleTrieException(errMessage.get() + ": failed deserializing value " + bytes, ex);
    }
    return value;
  }
}
