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

import org.apache.tuweni.bytes.Bytes;

/** Logical cache partitions stored in the head MapDB instance. */
public enum HeadMapDbCacheCategory {
  ACCOUNT_FLAT((byte) 1),
  ACCOUNT_TRIE((byte) 2),
  STORAGE_TRIE((byte) 3),
  CODE((byte) 4),
  STORAGE_FLAT((byte) 8),
  FLAT_ACCOUNT_RESOLUTION((byte) 5),
  TRIE_NODE_RESOLUTION((byte) 6),
  ACCESS_METADATA((byte) 7);

  private final byte id;

  HeadMapDbCacheCategory(final byte id) {
    this.id = id;
  }

  public byte id() {
    return id;
  }

  public Bytes encodeKey(final Bytes logicalKey) {
    return Bytes.concatenate(Bytes.of(id), logicalKey);
  }
}
