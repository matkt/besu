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

package org.hyperledger.besu.ethereum.eth.sync.snapsync.request;

import org.apache.tuweni.bytes.MutableBytes;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.bonsai.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.TrieNodeDecoder;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.jetbrains.annotations.NotNull;

import static com.google.common.base.Preconditions.checkArgument;

public class NodeDeletionProcessor {


  public static void cleanAccountNode(final BonsaiWorldStateKeyValueStorage worldStateStorage, final Bytes location, final Bytes data) {
    deletePotentialOldChildren(worldStateStorage, new BonsaiAccountInnerNode(location, data));
  }

  public static void cleanStorageNode(
          final BonsaiWorldStateKeyValueStorage worldStateStorage, final Hash accountHash, final Bytes location, final Bytes data) {
    deletePotentialOldChildren(worldStateStorage, new BonsaiStorageInnerNode(accountHash, location, data));
  }

  private static void deletePotentialOldChildren(final BonsaiWorldStateKeyValueStorage worldStateStorage, final BonsaiAccountInnerNode newNode) {
    retrieveStoredInnerAccountNode(worldStateStorage, newNode.getLocation())
        .ifPresent(
            oldNode -> {
              final List<Node<Bytes>> oldChildren = oldNode.decodeData();
              final List<Node<Bytes>> newChildren = newNode.decodeData();

              for (int i = 0; i < oldChildren.size(); i++) {
                if (!oldChildren.get(i).getHash().equals(Hash.EMPTY_TRIE_HASH)) {
                  if ((!(oldChildren.get(i) instanceof NullNode)
                      && (newChildren.size() <= i || newChildren.get(i) instanceof NullNode))) {
                    final Node<Bytes> childToDelete = oldChildren.get(i);
                    System.out.println("account child to delete "+childToDelete.getLocation());
                    childToDelete
                        .getLocation()
                        .ifPresent(worldStateStorage::clearAccountFlatDatabaseInRange);
                  }
                }
              }
            });
  }

  private static void deletePotentialOldChildren(final BonsaiWorldStateKeyValueStorage worldStateStorage , final BonsaiStorageInnerNode newNode) {
    retrieveStoredInnerStorageNode(worldStateStorage, newNode.getAccountHash(), newNode.getLocation())
        .ifPresent(
            oldNode -> {
              final List<Node<Bytes>> oldChildren = oldNode.decodeData();
              final List<Node<Bytes>> newChildren = newNode.decodeData();
              if(!oldNode.getData().equals(newNode.getData())){
                System.out.println("found difference "+oldNode.getAccountHash()+" "+oldNode.getLocation()+" "+oldNode.getData()+" "+newNode.getData());
              }
              for (int i = 0; i < oldChildren.size(); i++) {
                if (!oldChildren.get(i).getHash().equals(MerklePatriciaTrie.EMPTY_TRIE_NODE_HASH)) {
                  if (newChildren.size() <= i || newChildren.get(i) instanceof NullNode) {
                    final Node<Bytes> childToDelete = oldChildren.get(i);
                    System.out.println("to delete "+oldNode.getAccountHash()+" "+oldNode.getLocation()+" "+childToDelete.getLocation()+" "+childToDelete.getHash());
                    childToDelete
                        .getLocation()
                        .ifPresent(
                            location -> {
                              worldStateStorage.clearStorageFlatDatabaseInRange(
                                  oldNode.getAccountHash(), location);
                            });
                  }
                }
              }
            });
  }

  private static Optional<BonsaiNode> retrieveStoredInnerAccountNode(final BonsaiWorldStateKeyValueStorage worldStateStorage, final Bytes location) {
    return worldStateStorage
        .getStateTrieNode(location)
        .map(oldData -> new BonsaiAccountInnerNode(location,oldData));
  }

  private static Optional<BonsaiStorageInnerNode> retrieveStoredInnerStorageNode(
          final BonsaiWorldStateKeyValueStorage worldStateStorage, final Hash accountHash, final Bytes location) {
    return worldStateStorage
        .getStateTrieNode(Bytes.concatenate(accountHash, location))
        .map(
            oldData ->
                new BonsaiStorageInnerNode(accountHash, location, oldData));
  }

  public abstract static class BonsaiNode {
    private final Bytes location;
    private final Bytes data;

    protected BonsaiNode(final Bytes location,final Bytes data) {
      this.location = location;
      this.data = data;
    }

    public Bytes getLocation() {
      return location;
    }

    public Bytes getData() {
      return data;
    }

    @NotNull
    public List<Node<Bytes>> decodeData() {
      return TrieNodeDecoder.decodeNodes(getLocation(), getData());
    }
  }

  public abstract static class BonsaiStorageNode extends BonsaiNode {
    private final Hash accountHash;

    protected BonsaiStorageNode(
        final Hash accountHash, final Bytes location, final Bytes data) {
      super(location, data);
      this.accountHash = accountHash;
    }

    public Hash getAccountHash() {
      return accountHash;
    }
  }

  public static class BonsaiAccountInnerNode extends BonsaiNode {

    protected BonsaiAccountInnerNode(
        final Bytes location,final Bytes data) {
      super(location, data);
    }
  }

  public static class BonsaiStorageInnerNode extends BonsaiStorageNode {
    public BonsaiStorageInnerNode(
        final Hash accountHash, final Bytes location, final Bytes data) {
      super(accountHash, location, data);
    }
  }
}
