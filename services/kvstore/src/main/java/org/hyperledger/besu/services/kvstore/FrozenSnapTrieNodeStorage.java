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
package org.hyperledger.besu.services.kvstore;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Immutable-by-convention trie node store populated during snap sync (keyed by node hash). After
 * {@link #freeze()}, writes are rejected and the backing Chronicle Map is reopened read-only.
 */
public interface FrozenSnapTrieNodeStorage extends AutoCloseable {

  /** Opens or creates a persisted Chronicle Map at {@code directory}. */
  static FrozenSnapTrieNodeStorage open(final Path directory) {
    return ChronicleMapFrozenSnapTrieNodeStorage.open(directory);
  }

  void put(Bytes32 hash, Bytes value);

  void putAll(Map<Bytes32, Bytes> entries);

  Optional<Bytes> get(Bytes32 hash);

  /** Seals the store: no further writes; subsequent opens are read-only. */
  void freeze();

  boolean isFrozen();

  /** Number of hash-keyed trie nodes currently in the map. */
  long entryCount();

  @Override
  void close();
}
