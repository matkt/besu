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
      if (newNode instanceof LeafNode) {
        final Bytes encodedPathToExclude = Bytes.concatenate(location, newNode.getPath());
        if (location.size() < encodedPathToExclude.size()) {
          final List<Bytes> excludedLocation = List.of(Bytes.concatenate(location, Bytes.of(encodedPathToExclude.get(location.size()))));
          worldStateStorage.clearStorageFlatDatabaseInRange(1, accountHash, location, excludedLocation, storageTrieNodeDataRequest.data);
        }
      } else if (newNode instanceof ExtensionNode) {
        ((ExtensionNode<Bytes>) newNode).getChild().getLocation().ifPresent(subLocation -> {
          final List<Bytes> excludedLocation = List.of(subLocation);
          worldStateStorage.clearStorageFlatDatabaseInRange(2, accountHash, location, excludedLocation, storageTrieNodeDataRequest.data);
        });
      } else if (newNode instanceof BranchNode) {
        final List<Node<Bytes>> children = newNode.getChildren();
        final List<Bytes> excludedLocation = new ArrayList<>();
        for (int i = 0; i < MAX_CHILDREN; i++) {
          if (i < children.size() && !(children.get(i) instanceof NullNode)) {
            excludedLocation.add(Bytes.concatenate(location, Bytes.of(i)));
          }
        }
        worldStateStorage.clearStorageFlatDatabaseInRange(3, accountHash, location, excludedLocation, storageTrieNodeDataRequest.data);
      }
    });
  }

  public static void main(final String[] args) {

    /*
    “found next 0x47682af25e2a65349e9c6d27f5fce2b26718611ef8b7de9ad8cfaacc4f5b2ae01b6847dc741a1b0cd08d278845f9d819d87b734759afb55fe2de5cb82a9ae672 0x47682af25e2a65349e9c6d27f5fce2b26718611ef8b7de9ad8cfaacc4f5b2ae01b68400000000000000000000000000000000000000000000000000000000000 0x47682af25e2a65349e9c6d27f5fce2b26718611ef8b7de9ad8cfaacc4f5b2ae01b684fffffffffffffffffffffffffffffffffffffffffffffffffffffffffff”
     */
    //0x47682af25e2a65349e9c6d27f5fce2b26718611ef8b7de9ad8cfaacc4f5b2ae0
    Node<Bytes> newNode = TrieNodeDecoder.decode(Bytes.fromHexString("0x0c050602"), Bytes.fromHexString("0xe216a084db975aea24b119d7e6518d802e5e4294377758cc5b3580ce8a91acad4df51b"));

    /**
     * 0x0c96056c1c9bcc88be39a38267c0d2fa22ba51dbf7e0677e8d16637e3c02ce7cc562600000000000000000000000000000000000000000000000000000000000
     * found with method 4 to check accountHash
     * 0x0c96056c1c9bcc88be39a38267c0d2fa22ba51dbf7e0677e8d16637e3c02ce7c
     * 0x0c96056c1c9bcc88be39a38267c0d2fa22ba51dbf7e0677e8d16637e3c02ce7cc562694ff267ad918a4aa21913f751bfb05db7573c09758346bb73ca7a2116ab
     * from 0x0c96056c1c9bcc88be39a38267c0d2fa22ba51dbf7e0677e8d16637e3c02ce7cc562000000000000000000000000000000000000000000000000000000000000 to
     * 0x0c96056c1c9bcc88be39a38267c0d2fa22ba51dbf7e0677e8d16637e3c02ce7cc562ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
     * for data 0xe216a084db975aea24b119d7e6518d802e5e4294377758cc5b3580ce8a91acad4df51b
     * and location 0x0c050602 0x000c09060005060c010c090b0c0c08080b0e03090a03080206070c000d020f0a02020b0a05010d0b0f070e000607070e080d01060603070e030c00020c0e070c100c0506020
     */
    System.out.println(((ExtensionNode<Bytes>) newNode).getChild().getLocation());
    final Bytes accountHash = Bytes.fromHexString("0x0c96056c1c9bcc88be39a38267c0d2fa22ba51dbf7e0677e8d16637e3c02ce7c");

    if(newNode instanceof ExtensionNode) {

      final Bytes filteredLocation = CompactEncoding.bytesToPath(Bytes.fromHexString("0x0c96056c1c9bcc88be39a38267c0d2fa22ba51dbf7e0677e8d16637e3c02ce7cc562694ff267ad918a4aa21913f751bfb05db7573c09758346bb73ca7a2116ab")).slice(accountHash.size() * 2);

      final boolean shouldExclude = List.of(Bytes.fromHexString("0x0c05060206")).stream().anyMatch(bytes -> filteredLocation.commonPrefixLength(bytes) == bytes.size());

      System.out.println(shouldExclude);

    }}

}
