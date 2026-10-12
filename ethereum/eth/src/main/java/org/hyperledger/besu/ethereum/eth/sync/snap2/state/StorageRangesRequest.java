/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SnapMessages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.RangeProof;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Requests the storage of several accounts, or a slot range of one account, and verifies every
 * returned range against the storage root of its account.
 */
final class StorageRangesRequest implements SyncRequest<StorageRangesRequest.Result> {

  /**
   * Verified slots of one account.
   *
   * @param hashes the slot hashes
   * @param values the RLP encoded slot values
   * @param complete whether no slot exists after the last one
   */
  record Slots(List<Bytes32> hashes, List<Bytes> values, boolean complete) {}

  /**
   * Verified slots of the served accounts, in request order.
   *
   * @param accounts the slots of each served account
   */
  record Result(List<Slots> accounts) {}

  final int epoch;
  private final long pivotNumber;
  final Bytes32 root;
  final List<StorageTask> tasks;
  final List<Bytes32> storageRoots;
  final Bytes32 origin;
  final Bytes32 limit;
  final RangeTask chunk;
  private final long bytes;

  StorageRangesRequest(
      final int epoch,
      final long pivotNumber,
      final Bytes32 root,
      final List<StorageTask> tasks,
      final Bytes32 origin,
      final Bytes32 limit,
      final RangeTask chunk,
      final long bytes) {
    this.epoch = epoch;
    this.pivotNumber = pivotNumber;
    this.root = root;
    this.tasks = tasks;
    this.storageRoots = tasks.stream().map(StorageTask::storageRoot).toList();
    this.origin = origin;
    this.limit = limit;
    this.chunk = chunk;
    this.bytes = bytes;
  }

  @Override
  public Protocol protocol() {
    return Protocol.SNAP;
  }

  @Override
  public Priority priority() {
    return Priority.STATE;
  }

  @Override
  public long minimumPeerHeight() {
    return pivotNumber;
  }

  @Override
  public String name() {
    return chunk == null ? "storage_ranges" : "storage_chunk";
  }

  @Override
  public MessageData message(final EthPeer peer) {
    return SnapMessages.getStorageRanges(
        root, tasks.stream().map(StorageTask::accountHash).toList(), origin, limit, bytes);
  }

  @Override
  public Result decode(final MessageData response, final EthPeer peer) {
    final SnapMessages.StorageRanges ranges = SnapMessages.storageRanges(response);
    final int served = ranges.slotHashes().size();
    if (served == 0) {
      throw new EmptyResponseException("no storage for root " + root);
    }
    if (served > tasks.size()) {
      throw new InvalidResponseException("more storage ranges than requested", true);
    }
    final List<Slots> accounts = new ArrayList<>(served);
    for (int i = 0; i < served; i++) {
      final List<Bytes32> hashes = ranges.slotHashes().get(i);
      final List<Bytes> values = ranges.slotValues().get(i);
      final boolean proven = i == served - 1 && !ranges.proof().isEmpty();
      final RangeProof.Result verified =
          RangeProof.verify(
              storageRoots.get(i), origin, hashes, values, proven ? ranges.proof() : List.of());
      if (!verified.valid()) {
        throw new InvalidResponseException("invalid storage range", true);
      }
      accounts.add(new Slots(hashes, values, !verified.hasMore()));
    }
    return new Result(accounts);
  }
}
