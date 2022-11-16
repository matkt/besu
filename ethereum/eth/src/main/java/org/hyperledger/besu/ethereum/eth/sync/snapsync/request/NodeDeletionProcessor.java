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
        final Bytes encodedPathToExclude = Bytes.concatenate(location,
                newNode.getPath().size()>0?Bytes.of(newNode.getPath().get(0)):Bytes.EMPTY);
        worldStateStorage.clearAccountFlatDatabaseInRange(0, location,Optional.of(encodedPathToExclude), accountTrieNodeDataRequest.data);
      } else if(newNode instanceof ExtensionNode){
        final Optional<Bytes> encodedPathToExclude = ((ExtensionNode<Bytes>) newNode).getChild().getLocation();
        worldStateStorage.clearAccountFlatDatabaseInRange(1, location, encodedPathToExclude,accountTrieNodeDataRequest.data );
      } else if(newNode instanceof BranchNode) {
        final List<Node<Bytes>> children = newNode.getChildren();
        for (int i = 0; i < MAX_CHILDREN; i++) {
          if (i>=children.size() || children.get(i) instanceof NullNode) {
            worldStateStorage.clearAccountFlatDatabaseInRange(2, Bytes.concatenate(location,Bytes.of(i)), accountTrieNodeDataRequest.data);
          }
        }
      }
    });
  }


  public static void deletePotentialOldChildren(final BonsaiWorldStateKeyValueStorage worldStateStorage , final StorageTrieNodeDataRequest storageTrieNodeDataRequest) {
    final Node<Bytes> newNode = TrieNodeDecoder.decode(storageTrieNodeDataRequest.getLocation(), storageTrieNodeDataRequest.data);

    newNode.getLocation().ifPresent(location -> {
      final Bytes accountHash = storageTrieNodeDataRequest.getAccountHash();
      final Bytes accountHashToPath = BonsaiWorldStateKeyValueStorage.bytesToPath(accountHash);
      if(newNode instanceof LeafNode){
        final Bytes encodedPathToExclude = Bytes.concatenate(accountHashToPath, location,
                newNode.getPath().size()>0?Bytes.of(newNode.getPath().get(0)):Bytes.EMPTY);
        worldStateStorage.clearStorageFlatDatabaseInRange(3, accountHash, location,Optional.of(encodedPathToExclude), storageTrieNodeDataRequest.data);
      } else if(newNode instanceof ExtensionNode){
        final Optional<Bytes> encodedPathToExclude = ((ExtensionNode<Bytes>) newNode).getChild().getLocation()
                .map(childLocation -> Bytes.concatenate(accountHashToPath, childLocation));
        worldStateStorage.clearStorageFlatDatabaseInRange(4, accountHash, location, encodedPathToExclude, storageTrieNodeDataRequest.data);
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

    final Bytes accountHashToPath = BonsaiWorldStateKeyValueStorage.bytesToPath(accountHash);
    if(newNode instanceof ExtensionNode){
      final Optional<Bytes> encodedPathToExclude = ((ExtensionNode<Bytes>) newNode).getChild().getLocation()
              .map(childLocation -> Bytes.concatenate(accountHashToPath, childLocation));

//0x000c09060005060c010c090b0c0c08080b0e03090a03080206070c000d020f0a02020b0a05010d0b0f070e000607070e080d01060603070e030c00020c0e070c0c0506020609040f0f0206070a0d0901080a040a0a02010901030f0705010b0f0b00050d0b070507030c00090705080304060b0b07030c0a070a020101060a0b10
      //0x000c09060005060c010c090b0c0c08080b0e03090a03080206070c000d020f0a02020b0a05010d0b0f070e000607070e080d01060603070e030c00020c0e070c100c05060206
      final boolean shouldExclude = encodedPathToExclude
              .map(exclude -> {
                System.out.println(exclude.commonPrefixLength(CompactEncoding.bytesToPath(Bytes.fromHexString("0x0c96056c1c9bcc88be39a38267c0d2fa22ba51dbf7e0677e8d16637e3c02ce7cc562694ff267ad918a4aa21913f751bfb05db7573c09758346bb73ca7a2116ab"))));
                System.out.println(CompactEncoding.bytesToPath(Bytes.fromHexString("0x0c96056c1c9bcc88be39a38267c0d2fa22ba51dbf7e0677e8d16637e3c02ce7cc562694ff267ad918a4aa21913f751bfb05db7573c09758346bb73ca7a2116ab")));
                System.out.println(exclude);
                return exclude.commonPrefixLength(CompactEncoding.bytesToPath(Bytes.fromHexString("0x0c96056c1c9bcc88be39a38267c0d2fa22ba51dbf7e0677e8d16637e3c02ce7cc562794ff267ad918a4aa21913f751bfb05db7573c09758346bb73ca7a2116ab")))==exclude.size();
              })
              .orElse(false);
      System.out.println(shouldExclude);
    }
    if(newNode instanceof BranchNode) {
      final List<Node<Bytes>> children = newNode.getChildren();
      for (int i = 0; i < MAX_CHILDREN; i++) {
        if (i>=children.size() || children.get(i) instanceof NullNode) {
          System.out.println(BonsaiWorldStateKeyValueStorage.generateRangeFromLocation(Bytes.fromHexString("0x47682af25e2a65349e9c6d27f5fce2b26718611ef8b7de9ad8cfaacc4f5b2ae0"),Bytes.concatenate(newNode.getLocation().orElseThrow(), Bytes.of(i)) ));
          System.out.println("ihi "+children.get(i)+" "+Bytes.of(i));
          children.get(i).getLocation()
                  .ifPresent(childLocation -> {
                    System.out.println("delete child "+childLocation);
                  });
        }
      }
    }

    System.out.println(Hash.hash(Bytes.fromHexString("0xf851808080808080808080808080a01f68fcb25fd6eeef44b0fb8584d22b0c40b675d4f23e15c5cc59ebafdeb18197a08de71d07a19ed68487c92d12c150bcf37b329672628d2bacf96c3de86ec42f22808080")));
    System.out.println(""+((LeafNode<Bytes>)TrieNodeDecoder.decodeNodes(Bytes.fromHexString("0x0d"), Bytes.fromHexString("0xf869a03000003d185089b460b64f2539373d34f162824499a4443b3332a351d8e02f7db846f8440180a0db6720fd9640cb59eb92ab41fa2d7fc7a250bc5125225b5cf1bbaf985596f79fa0adbf8a3b59f81a7f101c18364cdc3e43cce128658e3502688020ce2de428a1bf")).get(0)).getPath());


    System.out.println(TrieNodeDecoder.decodeNodes(Bytes.fromHexString("0x0c0e02010900"), Bytes.fromHexString("0xe21fa028ae6e9117b93e4552f52d1f80e4e9586a621b751165f4239c008b3a08c7318c")).get(0));

    System.out.println(TrieNodeDecoder.decodeNodes(Bytes.fromHexString("0x0c0e02010900"), Bytes.fromHexString("0xe21fa028ae6e9117b93e4552f52d1f80e4e9586a621b751165f4239c008b3a08c7318c")).get(1).getLocation());

  }
}
