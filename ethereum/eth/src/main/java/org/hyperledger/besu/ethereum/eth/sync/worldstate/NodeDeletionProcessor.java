/*
 *
 *  * Copyright Hyperledger Besu Contributors.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  * the License. You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  * specific language governing permissions and limitations under the License.
 *  *
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.hyperledger.besu.ethereum.eth.sync.worldstate;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.bonsai.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.trie.BranchNode;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.ExtensionNode;
import org.hyperledger.besu.ethereum.trie.LeafNode;
import org.hyperledger.besu.ethereum.trie.MerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.worldstate.StateTrieAccountValue;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class NodeDeletionProcessor {

  private static final int MAX_CHILDREN = 16;

  public static void deletePotentialOldAccountEntries(
      final BonsaiWorldStateKeyValueStorage worldStateStorage, final Node<Bytes> newNode) {

    final Bytes location = newNode.getLocation().orElseThrow();

    if (newNode instanceof LeafNode) {
      final Bytes encodedPathToExclude = Bytes.concatenate(location, newNode.getPath());
      // cleaning the account trie node in this location
      worldStateStorage.pruneTrieNode(Bytes.EMPTY, location, Optional.of(encodedPathToExclude));
      // check state of the current account
      newNode
          .getValue()
          .ifPresent(
              value -> {
                final StateTrieAccountValue account =
                    StateTrieAccountValue.readFrom(new BytesValueRLPInput(value, false));
                // if storage is deleted
                final Hash accountHash =
                    Hash.wrap(Bytes32.wrap(CompactEncoding.pathToBytes(encodedPathToExclude)));
                if (account.getStorageRoot().equals(MerklePatriciaTrie.EMPTY_TRIE_NODE_HASH)) {
                  worldStateStorage.pruneTrieNode(accountHash, Bytes.EMPTY, Optional.empty());
                }
                // if code is deleted
                if (account.getCodeHash().equals(Hash.EMPTY)) {
                  worldStateStorage.pruneCodeState(accountHash);
                }
              });

    } else if (newNode instanceof ExtensionNode) {
      ((ExtensionNode<Bytes>) newNode)
          .getChild()
          .getLocation()
          .ifPresent(
              subLocation ->
                  worldStateStorage.pruneTrieNode(Bytes.EMPTY, location, Optional.of(subLocation)));
    } else if (newNode instanceof BranchNode) {
      final List<Node<Bytes>> children = newNode.getChildren();
      for (int i = 0; i < MAX_CHILDREN; i++) {
        if (i >= children.size() || !children.get(i).isReferencedByHash()) {
          worldStateStorage.pruneTrieNode(
              Bytes.EMPTY, Bytes.concatenate(location, Bytes.of(i)), Optional.empty());
        }
      }
    }
  }

  public static void deletePotentialOldStorageEntries(
      final BonsaiWorldStateKeyValueStorage worldStateStorage,
      final Bytes accountHash,
      final Node<Bytes> newNode) {

    final Bytes location = newNode.getLocation().orElseThrow();

    if (newNode instanceof LeafNode) {
      final Bytes encodedPathToExclude = Bytes.concatenate(location, newNode.getPath());
      worldStateStorage.pruneTrieNode(accountHash, location, Optional.of(encodedPathToExclude));
    } else if (newNode instanceof ExtensionNode) {
      ((ExtensionNode<Bytes>) newNode)
          .getChild()
          .getLocation()
          .ifPresent(
              subLocation ->
                  worldStateStorage.pruneTrieNode(accountHash, location, Optional.of(subLocation)));
    } else if (newNode instanceof BranchNode) {
      final List<Node<Bytes>> children = newNode.getChildren();
      for (int i = 0; i < MAX_CHILDREN; i++) {
        if (i >= children.size() || !children.get(i).isReferencedByHash()) {
          worldStateStorage.pruneTrieNode(
              accountHash, Bytes.concatenate(location, Bytes.of(i)), Optional.empty());
        }
      }
    }
  }
}
