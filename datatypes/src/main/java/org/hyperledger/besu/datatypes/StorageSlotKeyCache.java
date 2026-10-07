/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.datatypes;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * Storage slot keys whose hash is computed once. A block shares one cache between its accumulators
 * and its access location trackers, so a slot touched by several transactions is hashed once.
 * Thread safe.
 */
public final class StorageSlotKeyCache {

  private final Map<UInt256, Hash> slotHashes = new ConcurrentHashMap<>();

  /** Creates an empty cache. */
  public StorageSlotKeyCache() {}

  /**
   * Returns the key of a storage slot, hashing the slot only the first time.
   *
   * @param slot the storage slot
   * @return the slot key with its hash
   */
  public StorageSlotKey slotKey(final UInt256 slot) {
    return new StorageSlotKey(slotHash(slot), Optional.of(slot));
  }

  /**
   * Returns keccak256 of a storage slot, computed only the first time.
   *
   * @param slot the storage slot
   * @return the hash of the slot
   */
  public Hash slotHash(final UInt256 slot) {
    // computeIfAbsent: transactions running in parallel must not hash the same slot twice
    return slotHashes.computeIfAbsent(slot, Hash::hash);
  }

  /**
   * Records a slot key whose hash is already known, such as one decoded from a block access list,
   * so the slot is not hashed again.
   *
   * @param slotKey a slot key that has its slot
   */
  public void add(final StorageSlotKey slotKey) {
    slotKey.getSlotKey().ifPresent(slot -> slotHashes.putIfAbsent(slot, slotKey.getSlotHash()));
  }

  /** Forgets every hash, at the end of a block. */
  public void clear() {
    slotHashes.clear();
  }
}
