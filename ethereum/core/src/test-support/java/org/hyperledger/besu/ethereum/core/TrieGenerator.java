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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.StateTrieAccountValue;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

public class TrieGenerator {

  public static List<Hash> accountHash = new ArrayList<>();

  public static MerklePatriciaTrie<Bytes32, Bytes> generateTrie(
      final WorldStateStorage worldStateStorage, final int nbAccounts) {
    final List<Hash> accountHash = new ArrayList<>();
    final MerklePatriciaTrie<Bytes32, Bytes> accountStateTrie =
        emptyAccountStateTrie(worldStateStorage);
    // Add some storage values
    for (int i = 0; i < nbAccounts; i++) {
      final WorldStateStorage.Updater updater = worldStateStorage.updater();

      accountHash.add(Hash.wrap(Bytes32.leftPad(Bytes.of(i + 1))));
      final MerklePatriciaTrie<Bytes32, Bytes> storageTrie =
          emptyStorageTrie(worldStateStorage, accountHash.get(i));
      writeStorageValue(storageTrie, UInt256.ONE, UInt256.valueOf(2L));
      writeStorageValue(storageTrie, UInt256.valueOf(2L), UInt256.valueOf(4L));
      writeStorageValue(storageTrie, UInt256.valueOf(3L), UInt256.valueOf(6L));
      int accountIndex = i;
      storageTrie.commit(
          (location, hash, value) ->
              updater.putAccountStorageTrieNode(
                  accountHash.get(accountIndex), location, hash, value));
      final Bytes code = Bytes32.leftPad(Bytes.of(i + 10));
      final Hash codeHash = Hash.hash(code);
      final StateTrieAccountValue accountValue =
          new StateTrieAccountValue(1L, Wei.of(2L), Hash.wrap(storageTrie.getRootHash()), codeHash);
      accountStateTrie.put(accountHash.get(i), RLP.encode(accountValue::writeTo));
      accountStateTrie.commit(updater::putAccountStateTrieNode);
      updater.putCode(codeHash, code);
      // Persist updates
      updater.commit();
    }
    return accountStateTrie;
  }

  public static StoredMerklePatriciaTrie<Bytes, Bytes> generateTrie2(
      final WorldStateStorage worldStateStorage, final int nbAccounts) {
    accountHash = new ArrayList<>();
    final StoredMerklePatriciaTrie<Bytes, Bytes> accountStateTrie =
        new StoredMerklePatriciaTrie<>(worldStateStorage::getAccountStateTrieNode, b -> b, b -> b);
    // Add some storage values
    for (int i = 0; i < nbAccounts; i++) {
      final WorldStateStorage.Updater updater = worldStateStorage.updater();
      if (i == 0) {
        accountHash.add(
            Hash.hash(Address.fromHexString("0xe308bd1ac5fda103967359b2712dd89deffb7973")));
      } else if (i == 1) {
        accountHash.add(
            Hash.hash(Address.fromHexString("0xe408bd1ac5fda103967359b2712dd89deffb7973")));
      } else {
        accountHash.add(Hash.wrap(Bytes32.random()));
      }
      final MerklePatriciaTrie<Bytes32, Bytes> storageTrie =
          emptyStorageTrie(worldStateStorage, accountHash.get(i));
      writeStorageValue(storageTrie, UInt256.ONE, UInt256.valueOf(2L));
      writeStorageValue(storageTrie, UInt256.valueOf(2L), UInt256.valueOf(4L));
      writeStorageValue(storageTrie, UInt256.valueOf(3L), UInt256.valueOf(6L));
      int accountIndex = i;
      storageTrie.commit(
          (location, hash, value) ->
              updater.putAccountStorageTrieNode(
                  accountHash.get(accountIndex), location, hash, value));
      final Bytes code = Bytes32.leftPad(Bytes.of(i + 10));
      final Hash codeHash = Hash.hash(code);
      final StateTrieAccountValue accountValue =
          new StateTrieAccountValue(1L, Wei.of(2L), Hash.wrap(storageTrie.getRootHash()), codeHash);
      accountStateTrie.put(accountHash.get(i), RLP.encode(accountValue::writeTo));
      accountStateTrie.commit(updater::putAccountStateTrieNode);
      updater.putCode(codeHash, code);
      // Persist updates
      updater.commit();
    }
    return accountStateTrie;
  }

  private static void writeStorageValue(
      final MerklePatriciaTrie<Bytes32, Bytes> storageTrie,
      final UInt256 key,
      final UInt256 value) {
    storageTrie.put(storageKeyHash(key), encodeStorageValue(value));
  }

  private static Bytes32 storageKeyHash(final UInt256 storageKey) {
    return Hash.hash(storageKey);
  }

  private static Bytes encodeStorageValue(final UInt256 storageValue) {
    return RLP.encode(out -> out.writeBytes(storageValue.toMinimalBytes()));
  }

  public static MerklePatriciaTrie<Bytes32, Bytes> emptyStorageTrie(
      final WorldStateStorage worldStateStorage, final Hash accountHash) {
    return new StoredMerklePatriciaTrie<>(
        (location, hash) ->
            worldStateStorage.getAccountStorageTrieNode(accountHash, location, hash),
        b -> b,
        b -> b);
  }

  public static MerklePatriciaTrie<Bytes32, Bytes> emptyAccountStateTrie(
      final WorldStateStorage worldStateStorage) {
    return new StoredMerklePatriciaTrie<>(
        worldStateStorage::getAccountStateTrieNode, b -> b, b -> b);
  }
}
