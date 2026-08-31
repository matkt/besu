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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import org.hyperledger.besu.datatypes.Hash;

import org.apache.tuweni.bytes.Bytes;

public class MigrationScopedWorldStateKeyValueStorage extends BonsaiWorldStateKeyValueStorage {

  /**
   * Builds a migration-scoped view over the real, shared world-state storage.
   *
   * @param real the live {@link BonsaiWorldStateKeyValueStorage} owned by the provider; this view
   *     borrows its internals but never closes them
   */
  public MigrationScopedWorldStateKeyValueStorage(final BonsaiWorldStateKeyValueStorage real) {
    super(
        real.flatDbStrategyProvider,
        real.composedWorldStateStorage,
        real.trieLogStorage,
        real.cacheManager,
        real.getCurrentVersion(),
        real.getDataStorageFormat());
  }

  /**
   * Returns a base {@link Updater} whose composed transaction filters writes to the binary-trie
   * branch segment and whose trie-log transaction is a no-op.
   *
   * <p>Overridden (rather than reusing {@code CachedUpdater}) to prevent migration-suppressed flat
   * writes from polluting the shared flat-DB cache.
   */
  @Override
  public Updater updater() {
    return new Updater(
        composedWorldStateStorage.startTransaction(),
        trieLogStorage.startTransaction(),
        getFlatDbStrategy(),
        composedWorldStateStorage) {

      @Override
      public Updater removeCodeByAddress(final Hash accountHash) {
        return this;
      }

      @Override
      public Updater removeCodeByHash(final Hash codeHash) {
        return this;
      }

      @Override
      public Updater putCode(final Hash accountHash, final Bytes code) {
        return this;
      }

      @Override
      public Updater putCode(final Hash accountHash, final Hash codeHash, final Bytes code) {
        return this;
      }

      @Override
      public Updater removeAccountInfoState(final Hash accountHash) {
        return this;
      }

      @Override
      public Updater putAccountInfoState(final Hash accountHash, final Bytes accountValue) {
        return this;
      }

      @Override
      public synchronized Updater putStorageValueBySlotHash(
          final Hash accountHash, final Hash slotHash, final Bytes storageValue) {
        return this;
      }

      @Override
      public synchronized void removeStorageValueBySlotHash(
          final Hash accountHash, final Hash slotHash) {}

      @Override
      public void commit() {
        composedWorldStateTransaction.commit();
      }

      @Override
      public void commitTrieLogOnly() {}

      @Override
      public void commitComposedOnly() {
        composedWorldStateTransaction.commit();
      }

      @Override
      public void rollback() {
        composedWorldStateTransaction.rollback();
      }
    };
  }
}
