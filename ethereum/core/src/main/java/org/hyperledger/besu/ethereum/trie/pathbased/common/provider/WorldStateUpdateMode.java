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
package org.hyperledger.besu.ethereum.trie.pathbased.common.provider;

/**
 * How block validation should treat the world state after processing.
 *
 * <ul>
 *   <li>{@link #HEAD} – apply changes to the canonical head (import / FCU roll).
 *   <li>{@link #PAYLOAD_LAYER} – write a durable in-memory layer (flat + trie) and trie log without
 *       updating the head or flushing RocksDB; used by engine newPayload when layered head is
 *       enabled.
 *   <li>{@link #READ_ONLY} – compute root / trie log on a frozen snapshot without retaining writes
 *       (legacy newPayload / historical queries).
 * </ul>
 */
public enum WorldStateUpdateMode {
  HEAD,
  PAYLOAD_LAYER,
  READ_ONLY;

  public boolean updatesHead() {
    return this == HEAD;
  }

  public boolean retainsDurableLayer() {
    return this == PAYLOAD_LAYER;
  }

  /** Maps the historical boolean flag: {@code true} → HEAD, {@code false} → READ_ONLY. */
  public static WorldStateUpdateMode fromShouldUpdateHead(final boolean shouldUpdateHead) {
    return shouldUpdateHead ? HEAD : READ_ONLY;
  }
}
