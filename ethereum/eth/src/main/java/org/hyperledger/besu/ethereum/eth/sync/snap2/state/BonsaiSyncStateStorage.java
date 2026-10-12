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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode.TrieNodeKey;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Sync storage writing straight into the Bonsai flat database and trie node store. */
public final class BonsaiSyncStateStorage extends AbstractSyncStateStorage {

  // bounds the write batch each builder thread keeps before committing
  private static final int BYTES_PER_COMMIT = 8 * 1024 * 1024;

  private final BonsaiWorldStateKeyValueStorage bonsai;

  public BonsaiSyncStateStorage(final BonsaiWorldStateKeyValueStorage bonsai) {
    super(bonsai.getComposedWorldStateStorage(), ACCOUNT_INFO_STATE, ACCOUNT_STORAGE_STORAGE);
    this.bonsai = bonsai;
  }

  @Override
  public boolean hasCode(final Bytes32 codeHash) {
    return flat.containsKey(CODE_STORAGE, codeHash.toArrayUnsafe());
  }

  @Override
  public Batch batch() {
    final BonsaiWorldStateKeyValueStorage.Updater updater = newUpdater();
    return new Batch() {
      @Override
      public void putAccount(final Bytes32 accountHash, final Bytes account) {
        updater
            .getWorldStateTransaction()
            .put(ACCOUNT_INFO_STATE, accountHash.toArrayUnsafe(), account.toArrayUnsafe());
      }

      @Override
      public void removeAccount(final Bytes32 accountHash) {
        updater.getWorldStateTransaction().remove(ACCOUNT_INFO_STATE, accountHash.toArrayUnsafe());
      }

      @Override
      public void putStorage(
          final Bytes32 accountHash, final Bytes32 slotHash, final Bytes32 value) {
        updater
            .getWorldStateTransaction()
            .put(ACCOUNT_STORAGE_STORAGE, slotKey(accountHash, slotHash), value.toArrayUnsafe());
      }

      @Override
      public void removeStorage(final Bytes32 accountHash, final Bytes32 slotHash) {
        updater
            .getWorldStateTransaction()
            .remove(ACCOUNT_STORAGE_STORAGE, slotKey(accountHash, slotHash));
      }

      @Override
      public void putCode(final Bytes32 accountHash, final Bytes32 codeHash, final Bytes code) {
        updater.putCode(Hash.wrap(accountHash), Hash.wrap(codeHash), code);
      }

      @Override
      public void commit() {
        updater.commitComposedOnly();
      }
    };
  }

  @Override
  public boolean hasStorageTrie(final Bytes32 accountHash, final Bytes32 storageRoot) {
    return bonsai
        .getTrieNode(TrieNodeKey.of(Hash.wrap(accountHash), Bytes.EMPTY), storageRoot)
        .isPresent();
  }

  @Override
  public TrieWriter trieWriter() {
    return new TrieWriter() {
      private BonsaiWorldStateKeyValueStorage.Updater updater;
      private long pendingBytes;

      @Override
      public void accountNode(final Bytes location, final Bytes32 hash, final Bytes rlp) {
        updater().putTrieNode(location, hash, rlp);
        maybeCommit(location.size() + rlp.size());
      }

      @Override
      public void storageNode(
          final Bytes32 accountHash, final Bytes location, final Bytes32 hash, final Bytes rlp) {
        updater().putTrieNode(TrieNodeKey.of(Hash.wrap(accountHash), location), hash, rlp);
        maybeCommit(32 + location.size() + rlp.size());
      }

      @Override
      public void commit() {
        if (updater != null) {
          updater.commitComposedOnly();
          updater = null;
          pendingBytes = 0;
        }
      }

      private BonsaiWorldStateKeyValueStorage.Updater updater() {
        if (updater == null) {
          updater = newUpdater();
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
    final BonsaiWorldStateKeyValueStorage.Updater updater = newUpdater();
    updater.saveWorldState(
        header.getBlockHash().getBytes(), Bytes32.wrap(header.getStateRoot().getBytes()), rootNode);
    updater
        .getWorldStateTransaction()
        .put(
            TRIE_BRANCH_STORAGE,
            WORLD_BLOCK_NUMBER_KEY,
            Bytes.ofUnsignedLong(header.getNumber()).toArrayUnsafe());
    updater.commitComposedOnly();
  }

  @Override
  public void prepareForSync() {
    bonsai.clear();
    if (!bonsai.getFlatDbStrategy().isCodeByCodeHash()) {
      throw new IllegalStateException(
          "SNAP2 sync needs Bonsai to store bytecode by code hash (--Xbonsai-code-using-code-hash-enabled)");
    }
  }

  @Override
  public void completeSync() {
    if (bonsai.getFlatDbMode() != FlatDbMode.FULL) {
      bonsai.upgradeToFullFlatDbMode();
    }
    bonsai.clearCrossBlockCache();
  }

  private BonsaiWorldStateKeyValueStorage.Updater newUpdater() {
    return new BonsaiWorldStateKeyValueStorage.Updater(
        flat.startTransaction(),
        bonsai.getTrieLogStorage().startTransaction(),
        bonsai.getFlatDbStrategy(),
        flat,
        bonsai.getTrieNodeStrategy());
  }
}
