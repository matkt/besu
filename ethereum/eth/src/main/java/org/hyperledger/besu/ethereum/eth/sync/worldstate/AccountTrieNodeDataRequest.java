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
package org.hyperledger.besu.ethereum.eth.sync.worldstate;

import org.hyperledger.besu.ethereum.bonsai.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.ethereum.trie.MerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.StateTrieAccountValue;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage.Updater;

import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;

class AccountTrieNodeDataRequest extends TrieNodeDataRequest {

  private static final byte LEAF_PREFIX = 0x20;

  AccountTrieNodeDataRequest(final Hash hash, final Optional<Bytes> location) {
    super(RequestType.ACCOUNT_TRIE_NODE, hash, location);
  }

  @Override
  protected void doPersist(final Updater updater) {
    updater.putAccountStateTrieNode(getLocation().orElse(Bytes.EMPTY), getHash(), getData());
  }

  @Override
  public Optional<Bytes> getExistingData(final WorldStateStorage worldStateStorage) {
    return worldStateStorage.getNodeData(getLocation().orElse(Bytes.EMPTY), getHash());
  }

  @Override
  protected NodeDataRequest createChildNodeDataRequest(
      final Hash childHash, final Optional<Bytes> location) {
    return createAccountDataRequest(childHash, location);
  }

  @Override
  protected Stream<NodeDataRequest> getRequestsFromTrieNodeValue(
      final WorldStateStorage worldStateStorage, final Bytes value) {
    final Stream.Builder<NodeDataRequest> builder = Stream.builder();
    final StateTrieAccountValue accountValue = StateTrieAccountValue.readFrom(RLP.input(value));
    // Add code, if appropriate

    final Optional<Hash> accountHash =
        (worldStateStorage instanceof BonsaiWorldStateKeyValueStorage)
            ? getAccountHash()
            : Optional.empty();
    accountHash.ifPresent(
        hash ->
            ((BonsaiWorldStateKeyValueStorage.Updater) worldStateStorage.updater())
                .putAccountInfoState(hash, value)
                .commit());

    if (!accountValue.getCodeHash().equals(Hash.EMPTY)) {
      builder.add(createCodeRequest(accountValue.getCodeHash(), accountHash));
    }
    // Add storage, if appropriate
    if (!accountValue.getStorageRoot().equals(MerklePatriciaTrie.EMPTY_TRIE_NODE_HASH)) {
      // If storage is non-empty queue download

      final NodeDataRequest storageNode =
          createStorageDataRequest(accountValue.getStorageRoot(), accountHash, getLocation());
      builder.add(storageNode);
    }
    return builder.build();
  }

  @Override
  protected void writeTo(final RLPOutput out) {
    out.startList();
    out.writeByte(getRequestType().getValue());
    out.writeBytes(getHash());
    getLocation().ifPresent(out::writeBytes);
    out.endList();
  }

  private Optional<Hash> getAccountHash() {
    if (getLocation().isPresent()) {
      final Bytes location = getLocation().orElseThrow();
      final RLPInput input = RLP.input(getData());
      input.enterList();
      return Optional.of(Hash.wrap(Bytes32.wrap(mergePath(location, input.readBytes()))));
    } else {
      return Optional.empty();
    }
  }

  private static Bytes mergePath(final Bytes path, final Bytes data) {
    int pathIndex = 0;
    int dataIndex = 0;
    int accountHashIndex = 0;
    final MutableBytes accountHash = MutableBytes.create(Bytes32.SIZE);
    if (data.get(0) == LEAF_PREFIX) {
      dataIndex = 1;
    }
    while (pathIndex < path.size()) {
      final byte high = path.get(pathIndex++);
      final byte low;
      if (pathIndex >= path.size()) {
        low = data.get(dataIndex++);
      } else {
        low = path.get(pathIndex++);
      }
      accountHash.set(accountHashIndex++, (byte) (high << 4 | (low & 0x0f)));
    }
    while (accountHashIndex < accountHash.size()) {
      accountHash.set(accountHashIndex++, data.get(dataIndex++));
    }
    return accountHash;
  }
}
