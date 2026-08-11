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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;

import java.lang.reflect.Field;

import org.apache.tuweni.bytes.Bytes;

/**
 * Shared fixture helpers for {@link
 * org.hyperledger.besu.plugin.services.storage.DataStorageFormat#BINARY} tests.
 *
 * <p>Genesis state is written with a Merkle root, but the BINARY state-root committer expects an
 * empty binary trie root ({@link Hash#ZERO}). These helpers reset the stored world root hash to
 * {@link Hash#ZERO} and clear the stale Merkle root stored at the {@link Bytes#EMPTY} location so
 * binary-trie tests start from a clean empty root.
 */
public final class BinaryTestSupport {

  private BinaryTestSupport() {}

  /**
   * Resets the stored world root hash to {@link Hash#ZERO} (empty binary trie root) and removes the
   * stale Merkle root stored at the {@link Bytes#EMPTY} location.
   *
   * <p>This must be called on a head world state obtained with {@code shouldWorldStateUpdateHead =
   * true}, since it commits the reset root to the underlying storage.
   */
  public static void initializeEmptyBinaryTrieRoot(final BonsaiWorldState worldState) {
    final BonsaiWorldStateKeyValueStorage.Updater updater =
        worldState.getWorldStateStorage().updater();
    updater.getWorldStateTransaction().put(TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY, new byte[32]);
    updater.getWorldStateTransaction().remove(TRIE_BRANCH_STORAGE, Bytes.EMPTY.toArrayUnsafe());
    updater.commit();
    setWorldStateRootHash(worldState, Hash.ZERO);
  }

  /**
   * Reflectively sets the in-memory {@code worldStateRootHash} field of a {@link BonsaiWorldState}.
   * Useful when a test needs the live head world state to report {@link Hash#ZERO} before any
   * persist reloads it from storage.
   */
  public static void setWorldStateRootHash(
      final BonsaiWorldState worldState, final Hash stateRoot) {
    try {
      final Field rootField = BonsaiWorldState.class.getDeclaredField("worldStateRootHash");
      rootField.setAccessible(true);
      rootField.set(worldState, stateRoot);
    } catch (ReflectiveOperationException e) {
      throw new LinkageError("Failed to set world state root hash for BINARY test setup", e);
    }
  }
}
