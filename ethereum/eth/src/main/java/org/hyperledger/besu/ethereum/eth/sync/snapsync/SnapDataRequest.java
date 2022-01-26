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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hyperledger.besu.ethereum.eth.sync.fastsync.worldstate.NodeDataRequest.MAX_CHILDREN;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldDownloadState;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloaderException;
import org.hyperledger.besu.ethereum.proof.WorldStateProofProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;
import org.hyperledger.besu.services.tasks.TasksPriorityProvider;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.immutables.value.Value;

public abstract class SnapDataRequest implements TasksPriorityProvider {

  private final RequestType requestType;

  private final Hash originalRootHash;

  private Optional<Bytes> data;
  private Optional<SnapDataRequest> possibleParent = Optional.empty();
  private int depth;
  private long priority;
  private final AtomicInteger pendingChildren = new AtomicInteger(0);
  private boolean requiresPersisting = true;

  protected SnapDataRequest(final RequestType requestType, final Hash originalRootHash) {
    this.requestType = requestType;
    this.originalRootHash = originalRootHash;
    this.data = Optional.empty();
  }

  public static AccountRangeDataRequest createAccountRangeDataRequest(
      final Hash rootHash, final Bytes32 startKeyHash, final Bytes32 endKeyHash) {
    return new AccountRangeDataRequest(rootHash, startKeyHash, endKeyHash);
  }

  public StorageRangeDataRequest createStorageRangeDataRequest(
      final ArrayDeque<Bytes32> accountsHashes,
      final ArrayDeque<Bytes32> storageRoots,
      final Bytes32 startKeyHash,
      final Bytes32 endKeyHash) {
    return new StorageRangeDataRequest(
        getOriginalRootHash(), accountsHashes, storageRoots, startKeyHash, endKeyHash);
  }

  public GetBytecodeRequest createBytecodeRequest(
      final ArrayDeque<Bytes32> accountHashes, final ArrayDeque<Bytes32> codeHashes) {

    return new GetBytecodeRequest(getOriginalRootHash(), accountHashes, codeHashes);
  }

  public static AccountTrieNodeDataHealRequest createAccountDataRequest(
      final Hash hash, final Hash originalRootHash, final Optional<Bytes> location) {
    return new AccountTrieNodeDataHealRequest(hash, originalRootHash, location);
  }

  public static StorageTrieNodeDataHealRequest createStorageDataRequest(
      final Hash hash,
      final Optional<Hash> accountHash,
      final Hash originalRootHash,
      final Optional<Bytes> location) {
    return new StorageTrieNodeDataHealRequest(hash, accountHash, originalRootHash, location);
  }

  public RequestType getRequestType() {
    return requestType;
  }

  public Hash getOriginalRootHash() {
    return originalRootHash;
  }

  public Optional<Bytes> getData() {
    return data;
  }

  public SnapDataRequest setData(final Bytes data) {
    this.setData(Optional.ofNullable(data));
    return this;
  }

  public SnapDataRequest setData(final Optional<Bytes> data) {
    this.data = data;
    return this;
  }

  public int persist(
      final WorldStateStorage worldStateStorage, final WorldStateStorage.Updater updater) {
    if (pendingChildren.get() > 0) {
      return 0; // we do nothing. Our last child will eventually persist us.
    }
    int saved = 0;
    if (requiresPersisting) {
      checkNotNull(getData(), "Must set data before node can be persisted.");
      saved = doPersist(worldStateStorage, updater);
    }
    if (possibleParent.isPresent()) {
      return possibleParent.get().saveParent(worldStateStorage, updater) + saved;
    }
    return saved;
  }

  protected abstract int doPersist(
      final WorldStateStorage worldStateStorage, final WorldStateStorage.Updater updater);

  protected abstract boolean isTaskCompleted(
      WorldDownloadState<SnapDataRequest> downloadState,
      SnapSyncState fastSyncState,
      EthPeers ethPeers,
      WorldStateProofProvider worldStateProofProvider);

  public abstract Stream<SnapDataRequest> getChildRequests(
      final WorldStateStorage worldStateStorage);

  public void clear() {}

  protected void registerParent(final SnapDataRequest parent) {
    if (this.possibleParent.isPresent()) {
      throw new WorldStateDownloaderException("Cannot set parent twice");
    }
    this.possibleParent = Optional.of(parent);
    this.depth = parent.depth + 1;
    this.priority = parent.priority * MAX_CHILDREN + parent.incrementChildren();
  }

  private int saveParent(
      final WorldStateStorage worldStateStorage, final WorldStateStorage.Updater updater) {
    if (pendingChildren.decrementAndGet() == 0) {
      return persist(worldStateStorage, updater);
    }
    return 0;
  }

  public boolean isRoot() {
    return possibleParent.isEmpty();
  }

  private int incrementChildren() {
    return pendingChildren.incrementAndGet();
  }

  public void setRequiresPersisting(final boolean requiresPersisting) {

    this.requiresPersisting = requiresPersisting;
  }

  @Value.Immutable
  public abstract static class ExistingData {
    @Value.Default
    public boolean isFoundInCache() {
      return false;
    }

    @Value.Parameter
    public abstract Optional<Bytes> data();
  }

  public Optional<Bytes> getExistingData(final WorldStateStorage worldStateStorage) {
    return Optional.empty();
  }
  ;
}
