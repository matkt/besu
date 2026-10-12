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

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes32;

/**
 * Storage of an account that is not yet fully downloaded: the slot ranges already persisted and the
 * storage root responses are verified against.
 */
public final class StorageTask {

  private final Bytes32 accountHash;
  private volatile Bytes32 storageRoot;
  private volatile boolean rootStale;
  private final KeyRangeSet covered = new KeyRangeSet();

  // download state, only touched by the state downloader
  final List<RangeTask> chunks = new ArrayList<>();
  boolean queued;
  boolean refreshQueued;
  int inFlight;
  boolean refreshing;

  public StorageTask(final Bytes32 accountHash, final Bytes32 storageRoot) {
    // copies, as hashes decoded from a response slice the whole message
    this.accountHash = accountHash.copy();
    this.storageRoot = storageRoot.copy();
  }

  public Bytes32 accountHash() {
    return accountHash;
  }

  /** Returns the storage root at the current pivot, unless {@link #isRootStale()}. */
  public Bytes32 storageRoot() {
    return storageRoot;
  }

  /** Returns whether the storage changed since the root was learned, so it must be refreshed. */
  public boolean isRootStale() {
    return rootStale;
  }

  public void markRootStale() {
    rootStale = true;
  }

  public void refreshRoot(final Bytes32 root) {
    storageRoot = root.copy();
    rootStale = false;
  }

  /** Returns whether the slot is in a persisted range. */
  public boolean covers(final Bytes32 slotHash) {
    return covered.contains(slotHash);
  }

  public void cover(final Bytes32 from, final Bytes32 to) {
    covered.add(from, to);
  }

  public boolean isComplete() {
    return covered.isFull();
  }

  public KeyRangeSet covered() {
    return covered;
  }
}
