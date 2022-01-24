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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.bonsai.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.StateTrieAccountValue;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

class AccountTrieNodeDataHealRequest extends TrieNodeDataHealRequest {

  AccountTrieNodeDataHealRequest(
      final Hash hash, final Hash originalRootHash, final Optional<Bytes> location) {
    super(hash, originalRootHash, location);
  }

  @Override
  protected int doPersist(
      final WorldStateStorage worldStateStorage, final WorldStateStorage.Updater updater) {
    updater.putAccountStateTrieNode(
        getLocation().orElse(Bytes.EMPTY), getNodeHash(), getData().orElseThrow());
    return 1;
  }

  @Override
  public Optional<Bytes> getExistingData(final WorldStateStorage worldStateStorage) {
    return worldStateStorage
        .getAccountStateTrieNode(getLocation().orElse(Bytes.EMPTY), getNodeHash())
        .filter(data -> !getLocation().orElse(Bytes.EMPTY).isEmpty())
        .filter(data -> Hash.hash(data).compareTo(getNodeHash()) == 0);
  }

  @Override
  protected SnapDataRequest createChildNodeDataRequest(
      final Hash childHash, final Optional<Bytes> location) {
    return createAccountDataRequest(childHash, getOriginalRootHash(), location);
  }

  @Override
  protected Stream<SnapDataRequest> getRequestsFromTrieNodeValue(
      final WorldStateStorage worldStateStorage,
      final Optional<Bytes> location,
      final Bytes path,
      final Bytes value) {
    final Stream.Builder<SnapDataRequest> builder = Stream.builder();
    final StateTrieAccountValue accountValue = StateTrieAccountValue.readFrom(RLP.input(value));
    // Add code, if appropriate

    final Optional<Hash> accountHash =
        Optional.of(
            Hash.wrap(
                Bytes32.wrap(
                    CompactEncoding.pathToBytes(
                        Bytes.concatenate(getLocation().orElse(Bytes.EMPTY), path)))));
    if (worldStateStorage instanceof BonsaiWorldStateKeyValueStorage) {
      ((BonsaiWorldStateKeyValueStorage.Updater) worldStateStorage.updater())
          .putAccountInfoState(accountHash.get(), value)
          .commit();
    }

    if (!accountValue.getCodeHash().equals(Hash.EMPTY)) {
      builder.add(
          createBytecodeRequest(
              new ArrayDeque<>(List.of(accountHash.orElseThrow())),
              new ArrayDeque<>(List.of(accountValue.getCodeHash()))));
    }
    // Add storage, if appropriate
    if (!accountValue.getStorageRoot().equals(MerklePatriciaTrie.EMPTY_TRIE_NODE_HASH)) {
      // If storage is non-empty queue download

      final SnapDataRequest storageNode =
          createStorageDataRequest(
              accountValue.getStorageRoot(), accountHash, getOriginalRootHash(), Optional.empty());
      builder.add(storageNode);
    }
    return builder.build();
  }

  @Override
  public List<Bytes> getTrieNodePath() {
    return List.of(CompactEncoding.encode(getLocation().orElse(Bytes.EMPTY)));
  }
}
