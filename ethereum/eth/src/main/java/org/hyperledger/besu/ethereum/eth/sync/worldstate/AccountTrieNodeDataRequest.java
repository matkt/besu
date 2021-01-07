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

import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.ethereum.trie.MerklePatriciaTrie;
import org.hyperledger.besu.ethereum.worldstate.StateTrieAccountValue;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage.Updater;

import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;

class AccountTrieNodeDataRequest extends TrieNodeDataRequest {

  AccountTrieNodeDataRequest(
      final Hash hash, final Optional<Bytes> location) {
    super(RequestType.ACCOUNT_TRIE_NODE, hash, location);
  }

  @Override
  protected void doPersist(final Updater updater) {
    updater.putAccountStateTrieNode(getLocation().orElse(null), getHash(), getData());
  }

  @Override
  public Optional<Bytes> getExistingData(final WorldStateStorage worldStateStorage) {
    return worldStateStorage.getAccountStateTrieNode(getLocation().orElse(null), getHash());
  }

  @Override
  protected NodeDataRequest createChildNodeDataRequest(
          final Hash childHash, final Optional<Bytes> location) {
    return createAccountDataRequest(childHash, location);
  }

  @Override
  protected Stream<NodeDataRequest> getRequestsFromTrieNodeValue(final Bytes value) {
    final Stream.Builder<NodeDataRequest> builder = Stream.builder();
    final StateTrieAccountValue accountValue = StateTrieAccountValue.readFrom(RLP.input(value));
    // Add code, if appropriate
    if (!accountValue.getCodeHash().equals(Hash.EMPTY)) {
      builder.add(createCodeRequest(getHash(), accountValue.getCodeHash()));
    }
    // Add storage, if appropriate
    if (!accountValue.getStorageRoot().equals(MerklePatriciaTrie.EMPTY_TRIE_NODE_HASH)) {
      // If storage is non-empty queue download

      final NodeDataRequest storageNode =
          createStorageDataRequest(getHash(), accountValue.getStorageRoot(), getLocation());
      builder.add(storageNode);
    }
    return builder.build();
  }

  @Override
  protected void writeTo(final RLPOutput out) {
    out.startList();
    out.writeByte(getRequestType().getValue());
    out.writeBytes(getHash());
    out.endList();
  }


}
