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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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

  // writes are kept until the commit, which applies them in one short transaction and retries it
  @Override
  public Batch batch() {
    return new Batch() {
      private final List<Consumer<BonsaiWorldStateKeyValueStorage.Updater>> writes =
          new ArrayList<>();

      @Override
      public void putAccount(final Bytes32 accountHash, final Bytes account) {
        final byte[] key = accountHash.toArray();
        final byte[] value = account.toArray();
        writes.add(
            updater -> updater.getWorldStateTransaction().put(ACCOUNT_INFO_STATE, key, value));
      }

      @Override
      public void removeAccount(final Bytes32 accountHash) {
        final byte[] key = accountHash.toArray();
        writes.add(updater -> updater.getWorldStateTransaction().remove(ACCOUNT_INFO_STATE, key));
      }

      @Override
      public void putStorage(
          final Bytes32 accountHash, final Bytes32 slotHash, final Bytes32 value) {
        final byte[] key = slotKey(accountHash, slotHash);
        final byte[] stored = value.toArray();
        writes.add(
            updater ->
                updater.getWorldStateTransaction().put(ACCOUNT_STORAGE_STORAGE, key, stored));
      }

      @Override
      public void removeStorage(final Bytes32 accountHash, final Bytes32 slotHash) {
        final byte[] key = slotKey(accountHash, slotHash);
        writes.add(
            updater -> updater.getWorldStateTransaction().remove(ACCOUNT_STORAGE_STORAGE, key));
      }

      @Override
      public void putCode(final Bytes32 accountHash, final Bytes32 codeHash, final Bytes code) {
        final Hash account = Hash.wrap(accountHash.copy());
        final Hash hash = Hash.wrap(codeHash.copy());
        final Bytes copy = code.copy();
        writes.add(updater -> updater.putCode(account, hash, copy));
      }

      @Override
      public void commit() {
        StorageRetry.run(
            () -> {
              final BonsaiWorldStateKeyValueStorage.Updater updater = newUpdater();
              writes.forEach(write -> write.accept(updater));
              updater.commitComposedOnly();
            });
        writes.clear();
      }
    };
  }

  @Override
  public boolean hasStorageTrie(final Bytes32 accountHash, final Bytes32 storageRoot) {
    return bonsai
        .getTrieNode(TrieNodeKey.of(Hash.wrap(accountHash), Bytes.EMPTY), storageRoot)
        .isPresent();
  }

  // nodes are kept until the commit, which writes them in one short transaction and retries it
  @Override
  public TrieWriter trieWriter() {
    return new TrieWriter() {
      private final List<Bytes[]> nodes = new ArrayList<>();
      private long pendingBytes;

      @Override
      public void accountNode(final Bytes location, final Bytes32 hash, final Bytes rlp) {
        add(location.copy(), hash, rlp);
      }

      @Override
      public void storageNode(
          final Bytes32 accountHash, final Bytes location, final Bytes32 hash, final Bytes rlp) {
        add(TrieNodeKey.of(Hash.wrap(accountHash), location).copy(), hash, rlp);
      }

      @Override
      public void commit() {
        if (nodes.isEmpty()) {
          return;
        }
        StorageRetry.run(
            () -> {
              final BonsaiWorldStateKeyValueStorage.Updater updater = newUpdater();
              for (final Bytes[] node : nodes) {
                updater.putTrieNode(node[0], Bytes32.wrap(node[1]), node[2]);
              }
              updater.commitComposedOnly();
            });
        nodes.clear();
        pendingBytes = 0;
      }

      private void add(final Bytes key, final Bytes32 hash, final Bytes rlp) {
        nodes.add(new Bytes[] {key, hash.copy(), rlp.copy()});
        pendingBytes += key.size() + rlp.size();
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
