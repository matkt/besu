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

import org.apache.commons.lang3.tuple.Pair;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.bonsai.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.BranchNode;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.ExtensionNode;
import org.hyperledger.besu.ethereum.trie.LeafNode;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NullNode;
import org.hyperledger.besu.ethereum.trie.TrieNodeDecoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import static com.google.common.base.Preconditions.checkArgument;

public class NodeDeletionProcessor {
  
  private static final int MAX_CHILDREN = 16;

  public static void deletePotentialOldChildren(final BonsaiWorldStateKeyValueStorage worldStateStorage, final AccountTrieNodeDataRequest accountTrieNodeDataRequest) {
    final Node<Bytes> newNode = TrieNodeDecoder.decode(accountTrieNodeDataRequest.getLocation(), accountTrieNodeDataRequest.data);

    newNode.getLocation().ifPresent(location -> {
      if(newNode instanceof LeafNode){
        final Bytes encodedPathToExclude = Bytes.concatenate(location, newNode.getPath());
        if(location.size()<encodedPathToExclude.size()){
          final List<Bytes> excludedLocation = List.of(Bytes.concatenate(location,Bytes.of(encodedPathToExclude.get(location.size()))));
          worldStateStorage.clearAccountFlatDatabaseInRange(1, location,excludedLocation, accountTrieNodeDataRequest.data);
        }
      } else if(newNode instanceof ExtensionNode){
        ((ExtensionNode<Bytes>) newNode).getChild().getLocation().ifPresent(subLocation ->{
          final List<Bytes> excludedLocation = List.of(subLocation);
          worldStateStorage.clearAccountFlatDatabaseInRange(2, location, excludedLocation, accountTrieNodeDataRequest.data);
        });
      } else if(newNode instanceof BranchNode) {
        final List<Node<Bytes>> children = newNode.getChildren();
        final List<Bytes> excludedLocation = new ArrayList<>();
        for (int i = 0; i < MAX_CHILDREN; i++) {
          if (i < children.size() && !(children.get(i) instanceof NullNode)) {
            excludedLocation.add(Bytes.concatenate(location,Bytes.of(i)));
          }
        }
        worldStateStorage.clearAccountFlatDatabaseInRange(3, location,excludedLocation, accountTrieNodeDataRequest.data);
      }
    });
  }


  public static void deletePotentialOldChildren(final BonsaiWorldStateKeyValueStorage worldStateStorage , final StorageTrieNodeDataRequest storageTrieNodeDataRequest) {
    final Node<Bytes> newNode = TrieNodeDecoder.decode(storageTrieNodeDataRequest.getLocation(), storageTrieNodeDataRequest.data);

    newNode.getLocation().ifPresent(location -> {
      final Bytes accountHash = storageTrieNodeDataRequest.getAccountHash();
      if(newNode instanceof LeafNode){
        final Bytes encodedPathToExclude = Bytes.concatenate(location, newNode.getPath());
        if(location.size()<encodedPathToExclude.size()){
          final List<Bytes> excludedLocation = List.of(Bytes.concatenate(location,Bytes.of(encodedPathToExclude.get(location.size()))));
          worldStateStorage.clearStorageFlatDatabaseInRange(1, accountHash, location,excludedLocation, storageTrieNodeDataRequest.data);
        }
      } else if(newNode instanceof ExtensionNode){
        ((ExtensionNode<Bytes>) newNode).getChild().getLocation().ifPresent(subLocation ->{
          final List<Bytes> excludedLocation = List.of(subLocation);
          worldStateStorage.clearStorageFlatDatabaseInRange(2, accountHash,  location, excludedLocation, storageTrieNodeDataRequest.data);
        });
      } else if(newNode instanceof BranchNode) {
        final List<Node<Bytes>> children = newNode.getChildren();
        final List<Bytes> excludedLocation = new ArrayList<>();
        for (int i = 0; i < MAX_CHILDREN; i++) {
          if (i < children.size() && !(children.get(i) instanceof NullNode)) {
            excludedLocation.add(Bytes.concatenate(location,Bytes.of(i)));
          }
        }
        worldStateStorage.clearStorageFlatDatabaseInRange(3,accountHash, location,excludedLocation, storageTrieNodeDataRequest.data);
      }
    });
  }
}
