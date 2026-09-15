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
package org.hyperledger.besu.ethereum.trie.pathbased.common.storage.cache.headmapdb;

/**
 * Controls whether MapDB may be consulted for reads and whether writes may update the active head
 * MapDB cache.
 */
public enum HeadStateCacheAccessPolicy {
  /** Live canonical head: read snapshot/active MapDB, write-back on key-value storage miss, writes allowed. */
  CANONICAL_HEAD,
  /**
   * Frozen newPayload view: read only from a pinned MapDB snapshot; never mutates active head MapDB;
   * may fall back to key-value storage if the snapshot was removed.
   */
  FROZEN_SNAPSHOT,
  /** Historical or evicted-from-head cached state: key-value storage only. */
  KEY_VALUE_STORAGE_ONLY
}
