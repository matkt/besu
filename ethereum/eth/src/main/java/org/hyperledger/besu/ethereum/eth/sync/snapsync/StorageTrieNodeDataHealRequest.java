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
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage.Updater;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.rlp.RLP;

class StorageTrieNodeDataHealRequest extends TrieNodeDataHealRequest {

  final Optional<Hash> accountHash;

  StorageTrieNodeDataHealRequest(
      final Hash hash,
      final Optional<Hash> accountHash,
      final Hash originalRootHash,
      final Optional<Bytes> location) {
    super(hash, originalRootHash, location);
    this.accountHash = accountHash;
  }

  @Override
  protected int doPersist(final WorldStateStorage worldStateStorage, final Updater updater) {
    updater.putAccountStorageTrieNode(
        accountHash.orElse(Hash.EMPTY),
        getLocation().orElse(Bytes.EMPTY),
        getNodeHash(),
        getData().orElseThrow());
    return 1;
  }

  @Override
  public Optional<Bytes> getExistingData(final WorldStateStorage worldStateStorage) {
    return worldStateStorage
        .getAccountStorageTrieNode(
            getAccountHash().orElse(Hash.EMPTY), getLocation().orElse(Bytes.EMPTY), getNodeHash())
        .filter(data -> Hash.hash(data).compareTo(getNodeHash()) == 0);
  }

  @Override
  protected SnapDataRequest createChildNodeDataRequest(
      final Hash childHash, final Optional<Bytes> location) {
    return SnapDataRequest.createStorageDataRequest(
        childHash, getAccountHash(), getOriginalRootHash(), location);
  }

  @Override
  protected Stream<SnapDataRequest> getRequestsFromTrieNodeValue(
      final WorldStateStorage worldStateStorage,
      final Optional<Bytes> location,
      final Bytes path,
      final Bytes value) {
    if (worldStateStorage instanceof BonsaiWorldStateKeyValueStorage) {
      ((BonsaiWorldStateKeyValueStorage.Updater) worldStateStorage.updater())
          .putStorageValueBySlotHash(
              accountHash.get(),
              getSlotHash(location, path),
              Bytes32.leftPad(RLP.decodeValue(value)))
          .commit();
    }
    return Stream.empty();
  }

  public Optional<Hash> getAccountHash() {
    return accountHash;
  }

  private Hash getSlotHash(final Optional<Bytes> location, final Bytes path) {
    return Hash.wrap(
        Bytes32.wrap(
            CompactEncoding.pathToBytes(Bytes.concatenate(location.orElse(Bytes.EMPTY), path))));
  }

  @Override
  public List<Bytes> getTrieNodePath() {
    return List.of(
        accountHash.orElseThrow(), CompactEncoding.encode(getLocation().orElse(Bytes.EMPTY)));
  }
}
