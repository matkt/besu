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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.VERKLE_TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.BinTrieAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.flat.BinTrieFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.flat.BinTrieLegacyFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.flat.BinTrieStemFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

public class BinTrieWorldStateKeyValueStorage extends PathBasedWorldStateKeyValueStorage
    implements WorldStateKeyValueStorage {

  protected final FlatDbStrategyProvider flatDbStrategyProvider;
  protected MetricsSystem metricsSystem;
  protected final DataStorageConfiguration dataStorageConfiguration;

  public BinTrieWorldStateKeyValueStorage(
      final StorageProvider provider,
      final DataStorageConfiguration dataStorageConfiguration,
      final MetricsSystem metricsSystem) {
    super(
        provider.getStorageBySegmentIdentifiers(List.of(CODE_STORAGE, VERKLE_TRIE_BRANCH_STORAGE)),
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE),
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.PREIMAGE));
    this.metricsSystem = metricsSystem;
    this.dataStorageConfiguration = dataStorageConfiguration;
    this.flatDbStrategyProvider =
        new BinTrieFlatDbStrategyProvider(
            metricsSystem, dataStorageConfiguration, composedWorldStateStorage);
    this.flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);
  }

  public BinTrieWorldStateKeyValueStorage(
      final SegmentedKeyValueStorage composedWorldStateStorage,
      final KeyValueStorage trieLogStorage,
      final KeyValueStorage preImage,
      final DataStorageConfiguration dataStorageConfiguration,
      final MetricsSystem metricsSystem) {
    super(composedWorldStateStorage, trieLogStorage, preImage);
    this.metricsSystem = metricsSystem;
    this.dataStorageConfiguration = dataStorageConfiguration;
    this.flatDbStrategyProvider =
        new BinTrieFlatDbStrategyProvider(
            metricsSystem, dataStorageConfiguration, composedWorldStateStorage);
  }

  @Override
  public FlatDbStrategy getFlatDbStrategy() {
    return flatDbStrategyProvider.getFlatDbStrategy(composedWorldStateStorage);
  }

  @Override
  public DataStorageFormat getDataStorageFormat() {
    return DataStorageFormat.BINTRIE;
  }

  @Override
  public FlatDbMode getFlatDbMode() {
    return flatDbStrategyProvider.getFlatDbMode();
  }

  public Optional<Bytes> getCode(final Hash codeHash, final Hash accountHash) {
    if (codeHash.equals(Hash.EMPTY)) {
      return Optional.of(Bytes.EMPTY);
    } else {
      return getFlatDbStrategy().getFlatCode(codeHash, accountHash, composedWorldStateStorage);
    }
  }

  public Optional<BinTrieAccount> getAccount(
      final Address address, final PathBasedWorldView context) {
    final FlatDbStrategy flatDbStrategy = getFlatDbStrategy();
    // TODO this code is not 100% clean but legacy flat db will be removed in the future
    if (flatDbStrategy instanceof BinTrieLegacyFlatDbStrategy legacyFlatDbStrategy) {
      return legacyFlatDbStrategy.getFlatAccount(address, context, composedWorldStateStorage);
    } else if (flatDbStrategy instanceof BinTrieStemFlatDbStrategy stemFlatDbStrategy) {
      return stemFlatDbStrategy.getFlatAccount(address, context, composedWorldStateStorage);
    }
    return Optional.empty();
  }

  public Optional<UInt256> getStorageValueByStorageSlotKey(
      final Address address, final StorageSlotKey storageSlotKey) {
    final FlatDbStrategy flatDbStrategy = getFlatDbStrategy();
    // TODO this code is not 100% clean but legacy flat db will be removed in the future
    if (flatDbStrategy instanceof BinTrieLegacyFlatDbStrategy legacyFlatDbStrategy) {
      return legacyFlatDbStrategy
          .getFlatStorageValueByStorageSlotKey(address, storageSlotKey, composedWorldStateStorage)
          .map(UInt256::fromBytes);
    } else if (flatDbStrategy instanceof BinTrieStemFlatDbStrategy stemFlatDbStrategy) {
      return stemFlatDbStrategy
          .getFlatStorageValueByStorageSlotKey(address, storageSlotKey, composedWorldStateStorage)
          .map(UInt256::fromBytes);
    }
    return Optional.empty();
  }

  @Override
  public void clear() {
    super.clear();
    this.flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);
  }

  @Override
  public SegmentIdentifier getTrieBranchSegmentIdentifier() {
    return VERKLE_TRIE_BRANCH_STORAGE;
  }

  @Override
  public Updater updater() {
    return new Updater(
        composedWorldStateStorage.startTransaction(),
        trieLogStorage.startTransaction(),
        preImage.startTransaction(),
        getFlatDbStrategy(),
        composedWorldStateStorage);
  }

  public static class Updater implements PathBasedWorldStateKeyValueStorage.Updater {

    private final SegmentedKeyValueStorageTransaction composedWorldStateTransaction;
    private final KeyValueStorageTransaction trieLogStorageTransaction;
    private final KeyValueStorageTransaction preImageStorageTransaction;
    private final FlatDbStrategy flatDbStrategy;
    private final SegmentedKeyValueStorage worldStorage;

    public Updater(
        final SegmentedKeyValueStorageTransaction composedWorldStateTransaction,
        final KeyValueStorageTransaction trieLogStorageTransaction,
        final KeyValueStorageTransaction preImageStorageTransaction,
        final FlatDbStrategy flatDbStrategy,
        final SegmentedKeyValueStorage worldStorage) {

      this.composedWorldStateTransaction = composedWorldStateTransaction;
      this.trieLogStorageTransaction = trieLogStorageTransaction;
      this.preImageStorageTransaction = preImageStorageTransaction;
      this.flatDbStrategy = flatDbStrategy;
      this.worldStorage = worldStorage;
    }

    public Updater removeCode(final Hash accountHash, final Hash codeHash) {
      flatDbStrategy.removeFlatCode(
          worldStorage, composedWorldStateTransaction, accountHash, codeHash);
      return this;
    }

    public Updater putCode(final Hash accountHash, final Bytes code) {
      // Skip the hash calculation for empty code
      final Hash codeHash = code.size() == 0 ? Hash.EMPTY : Hash.hash(code);
      return putCode(accountHash, codeHash, code);
    }

    public Updater putCode(final Hash accountHash, final Hash codeHash, final Bytes code) {
      if (code.size() == 0) {
        // Don't save empty values
        return this;
      }
      flatDbStrategy.putFlatCode(
          worldStorage, composedWorldStateTransaction, accountHash, codeHash, code);
      return this;
    }

    public Updater removeAccountInfoState(final Hash accountHash) {
      flatDbStrategy.removeFlatAccount(worldStorage, composedWorldStateTransaction, accountHash);
      return this;
    }

    public Updater addPreImage(final Hash hash, final Bytes preImage) {
      preImageStorageTransaction.put(hash.toArrayUnsafe(), preImage.toArrayUnsafe());
      return this;
    }

    public Updater putAccountInfoState(final Hash accountHash, final Bytes accountValue) {
      if (accountValue.size() == 0) {
        // Don't save empty values
        return this;
      }
      flatDbStrategy.putFlatAccount(
          worldStorage, composedWorldStateTransaction, accountHash, accountValue);
      return this;
    }

    public Updater putStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash, final Bytes storage) {
      flatDbStrategy.putFlatAccountStorageValueByStorageSlotHash(
          worldStorage, composedWorldStateTransaction, accountHash, slotHash, storage);
      return this;
    }

    public void removeStorageValueBySlotHash(final Hash accountHash, final Hash slotHash) {
      flatDbStrategy.removeFlatAccountStorageValueByStorageSlotHash(
          worldStorage, composedWorldStateTransaction, accountHash, slotHash);
    }

    @Override
    public Updater saveWorldStateAndRootNode(
        final Bytes blockHash, final long blockNumber, final Bytes32 nodeHash, final Bytes node) {
      composedWorldStateTransaction.put(
          VERKLE_TRIE_BRANCH_STORAGE, Bytes.EMPTY.toArrayUnsafe(), node.toArrayUnsafe());
      saveWorldState(blockHash, blockNumber, nodeHash);
      return this;
    }

    @Override
    public Updater saveWorldState(
        final Bytes blockHash, final long blockNumber, final Bytes32 nodeHash) {
      composedWorldStateTransaction.put(
          VERKLE_TRIE_BRANCH_STORAGE, WORLD_ROOT_HASH_KEY, nodeHash.toArrayUnsafe());
      composedWorldStateTransaction.put(
          VERKLE_TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY, blockHash.toArrayUnsafe());
      return this;
    }

    @Override
    public SegmentedKeyValueStorageTransaction getWorldStateTransaction() {
      return composedWorldStateTransaction;
    }

    @Override
    public KeyValueStorageTransaction getTrieLogStorageTransaction() {
      return trieLogStorageTransaction;
    }

    @Override
    public void commit() {
      // write the log ahead, then the worldstate
      trieLogStorageTransaction.commit();
      composedWorldStateTransaction.commit();
      preImageStorageTransaction.commit();
    }

    @Override
    public void commitTrieLogOnly() {
      trieLogStorageTransaction.commit();
    }

    @Override
    public void commitComposedOnly() {
      composedWorldStateTransaction.commit();
    }

    @Override
    public void rollback() {
      composedWorldStateTransaction.rollback();
      trieLogStorageTransaction.rollback();
      preImageStorageTransaction.rollback();
    }
  }
}
