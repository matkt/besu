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
package org.hyperledger.besu.ethereum.eth.sync.snap2.storage;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.SNAPSYNC_FLAT_ACCOUNTS;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.SNAPSYNC_FLAT_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Sync storage for Forest: the flat state is staged in dedicated segments and cleared once the
 * tries, keyed by hash, are built.
 */
public final class ForestSyncStateStorage extends AbstractSyncStateStorage {

  private final ForestWorldStateKeyValueStorage forest;

  public ForestSyncStateStorage(
      final ForestWorldStateKeyValueStorage forest, final StorageProvider storageProvider) {
    this(
        forest,
        storageProvider.getStorageBySegmentIdentifiers(
            List.of(SNAPSYNC_FLAT_ACCOUNTS, SNAPSYNC_FLAT_STORAGE)));
  }

  ForestSyncStateStorage(
      final ForestWorldStateKeyValueStorage forest, final SegmentedKeyValueStorage staging) {
    super(staging, SNAPSYNC_FLAT_ACCOUNTS, SNAPSYNC_FLAT_STORAGE);
    this.forest = forest;
  }

  @Override
  public boolean hasCode(final Bytes32 codeHash) {
    return forest.getCode(Hash.wrap(codeHash)).isPresent();
  }

  @Override
  public Batch batch() {
    final SegmentedKeyValueStorageTransaction tx = flat.startUnloggedBatchTransaction();
    return new Batch() {
      private ForestWorldStateKeyValueStorage.Updater codeUpdater;

      @Override
      public void putAccount(final Bytes32 accountHash, final Bytes account) {
        tx.put(accountSegment, accountHash.toArrayUnsafe(), account.toArrayUnsafe());
      }

      @Override
      public void removeAccount(final Bytes32 accountHash) {
        tx.remove(accountSegment, accountHash.toArrayUnsafe());
      }

      @Override
      public void putStorage(
          final Bytes32 accountHash, final Bytes32 slotHash, final Bytes32 value) {
        tx.put(
            storageSegment,
            slotKey(accountHash, slotHash),
            value.trimLeadingZeros().toArrayUnsafe());
      }

      @Override
      public void removeStorage(final Bytes32 accountHash, final Bytes32 slotHash) {
        tx.remove(storageSegment, slotKey(accountHash, slotHash));
      }

      @Override
      public void putCode(final Bytes32 codeHash, final Bytes code) {
        if (codeUpdater == null) {
          codeUpdater = forest.unloggedUpdater();
        }
        codeUpdater.putCode(codeHash, code);
      }

      @Override
      public void commit() {
        if (codeUpdater != null) {
          codeUpdater.commit();
        }
        tx.commit();
      }
    };
  }

  @Override
  public boolean hasStorageTrie(final Bytes32 accountHash, final Bytes32 storageRoot) {
    return forest.getAccountStorageTrieNode(storageRoot).isPresent();
  }

  @Override
  public TrieWriter trieWriter() {
    return new TrieWriter() {
      private ForestWorldStateKeyValueStorage.Updater updater;
      private long pendingBytes;

      @Override
      public void accountNode(final Bytes location, final Bytes32 hash, final Bytes rlp) {
        updater().putAccountStateTrieNode(hash, rlp);
        maybeCommit(32 + rlp.size());
      }

      @Override
      public void storageNode(
          final Bytes32 accountHash, final Bytes location, final Bytes32 hash, final Bytes rlp) {
        updater().putAccountStorageTrieNode(hash, rlp);
        maybeCommit(32 + rlp.size());
      }

      @Override
      public void commit() {
        if (updater != null) {
          updater.commit();
          updater = null;
          pendingBytes = 0;
        }
      }

      private ForestWorldStateKeyValueStorage.Updater updater() {
        if (updater == null) {
          updater = forest.unloggedUpdater();
        }
        return updater;
      }

      private void maybeCommit(final int bytes) {
        pendingBytes += bytes;
        if (pendingBytes >= BYTES_PER_COMMIT) {
          commit();
        }
      }
    };
  }

  @Override
  public void saveWorldState(final BlockHeader header, final Bytes rootNode) {
    final ForestWorldStateKeyValueStorage.Updater updater = forest.unloggedUpdater();
    updater.saveWorldState(Bytes32.wrap(header.getStateRoot().getBytes()), rootNode);
    updater.commit();
  }

  @Override
  public void prepareForSync() {
    // nodes are keyed by hash, so the existing world state can stay
    clearStaging();
  }

  @Override
  public void flush() {
    flat.flush();
    forest.flush();
  }

  @Override
  public void completeSync() {
    clearStaging();
  }

  private void clearStaging() {
    flat.clear(accountSegment);
    flat.clear(storageSegment);
  }
}
