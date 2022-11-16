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
        for (int i = 0; i < MAX_CHILDREN; i++) {
          if(location.size()<encodedPathToExclude.size() && encodedPathToExclude.get(location.size())!=i){
            worldStateStorage.clearAccountFlatDatabaseInRange(1, Bytes.concatenate(location, Bytes.of(i)), accountTrieNodeDataRequest.data);
          }
        }
      } else if(newNode instanceof ExtensionNode){
        ((ExtensionNode<Bytes>) newNode).getChild().getLocation().ifPresent(subLocation ->{
          for (int i = 0; i < MAX_CHILDREN; i++) {
            if(subLocation.get(subLocation.size()-1)!=i){
              worldStateStorage.clearAccountFlatDatabaseInRange(2, Bytes.concatenate(location, Bytes.of(i)), accountTrieNodeDataRequest.data);
            }
          }
        });
      } else if(newNode instanceof BranchNode) {
        final List<Node<Bytes>> children = newNode.getChildren();
        for (int i = 0; i < MAX_CHILDREN; i++) {
          if (i>=children.size() || children.get(i) instanceof NullNode) {
            worldStateStorage.clearAccountFlatDatabaseInRange(3, Bytes.concatenate(location,Bytes.of(i)), accountTrieNodeDataRequest.data);
          }
        }
      }
    });
  }


  public static void deletePotentialOldChildren(final BonsaiWorldStateKeyValueStorage worldStateStorage , final StorageTrieNodeDataRequest storageTrieNodeDataRequest) {
    final Node<Bytes> newNode = TrieNodeDecoder.decode(storageTrieNodeDataRequest.getLocation(), storageTrieNodeDataRequest.data);

    newNode.getLocation().ifPresent(location -> {
      final Bytes accountHash = storageTrieNodeDataRequest.getAccountHash();
      if(newNode instanceof LeafNode){
        final Bytes encodedPathToExclude = Bytes.concatenate(location, newNode.getPath());
        for (int i = 0; i < MAX_CHILDREN; i++) {
            if(location.size()<encodedPathToExclude.size() && encodedPathToExclude.get(location.size())!=i){
              worldStateStorage.clearStorageFlatDatabaseInRange(3, accountHash, Bytes.concatenate(location, Bytes.of(i)), storageTrieNodeDataRequest.data);
            }
        }
      } else if(newNode instanceof ExtensionNode){
        ((ExtensionNode<Bytes>) newNode).getChild().getLocation().ifPresent(subLocation ->{
          for (int i = 0; i < MAX_CHILDREN; i++) {
            if(subLocation.get(subLocation.size()-1)!=i){
              worldStateStorage.clearStorageFlatDatabaseInRange(4, accountHash, Bytes.concatenate(location, Bytes.of(i)), storageTrieNodeDataRequest.data);
            }
          }
        });
      } else if(newNode instanceof BranchNode) {
        final List<Node<Bytes>> children = newNode.getChildren();
        for (int i = 0; i < MAX_CHILDREN; i++) {
          if (i>=children.size() || children.get(i) instanceof NullNode) {
            worldStateStorage.clearStorageFlatDatabaseInRange(5, accountHash, Bytes.concatenate(location,Bytes.of(i)), storageTrieNodeDataRequest.data);
          }
        }
      }
    });
  }
}
