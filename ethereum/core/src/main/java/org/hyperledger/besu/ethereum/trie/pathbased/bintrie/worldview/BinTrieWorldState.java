/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie.worldview;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.stateless.bintrie.adapter.TrieKeyFactory;
import org.hyperledger.besu.ethereum.stateless.bintrie.hasher.StemHasher;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.bintrie.BinaryTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.BinTrieAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.BinTrieWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.LeafBuilder;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.BinTrieLayeredWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.BinTrieWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.cache.PathBasedCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"unused", "MismatchedQueryAndUpdateOfCollection", "ModifiedButNotUsed"})
public class BinTrieWorldState extends PathBasedWorldState {

  private static final Logger LOG = LoggerFactory.getLogger(BinTrieWorldState.class);

  private final CodeCache codeCache;

  public BinTrieWorldState(
      final BinTrieWorldStateProvider archive,
      final BinTrieWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final CodeCache codeCache) {
    this(
        worldStateKeyValueStorage,
        archive.getCachedWorldStorageManager(),
        archive.getTrieLogManager(),
        evmConfiguration,
        worldStateConfig,
        codeCache);
  }

  public BinTrieWorldState(
      final BinTrieWorldStateKeyValueStorage worldStateKeyValueStorage,
      final PathBasedCachedWorldStorageManager cachedWorldStorageManager,
      final TrieLogManager trieLogManager,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig,
      final CodeCache codeCache) {
    super(worldStateKeyValueStorage, cachedWorldStorageManager, trieLogManager, worldStateConfig);
    this.setAccumulator(new BinTrieWorldStateUpdateAccumulator(this, evmConfiguration, codeCache));
    this.codeCache = codeCache;
  }

  @Override
  public BinTrieWorldStateKeyValueStorage getWorldStateStorage() {
    return (BinTrieWorldStateKeyValueStorage) worldStateKeyValueStorage;
  }

  @Override
  public CodeCache codeCache() {
    return codeCache;
  }

  @Override
  protected Hash calculateRootHash(
      final Optional<PathBasedWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final PathBasedWorldStateUpdateAccumulator<?> worldStateUpdater) {
    return internalCalculateRootHash(
        maybeStateUpdater.map(BinTrieWorldStateKeyValueStorage.Updater.class::cast),
        (BinTrieWorldStateUpdateAccumulator) worldStateUpdater);
  }

  protected Hash internalCalculateRootHash(
      final Optional<BinTrieWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final BinTrieWorldStateUpdateAccumulator worldStateUpdater) {

    final BinaryTrie stateTrie =
        createTrie((location, hash) -> worldStateKeyValueStorage.getStateTrieNode(location));

    final Set<Address> addressesToPersist = getAddressesToPersist(worldStateUpdater);
    for (final Address accountKey : addressesToPersist) {
      updateState(accountKey, stateTrie, maybeStateUpdater, worldStateUpdater);
    }

    LOG.info("start commit ");
    maybeStateUpdater.ifPresent(
        binTrieUpdater ->
            stateTrie.commit(
                (location, hash, value) -> {
                  if (value == null) {
                    removeTrieNode(
                        TRIE_BRANCH_STORAGE, binTrieUpdater.getWorldStateTransaction(), location);
                    return;
                  }
                  writeTrieNode(
                      TRIE_BRANCH_STORAGE,
                      binTrieUpdater.getWorldStateTransaction(),
                      location,
                      value);
                }));

    LOG.info("end commit ");
    // LOG.info(stateTrie.toDotTree());
    final Bytes32 rootHash = stateTrie.getRootHash();
    LOG.info("end commit " + rootHash);

    return Hash.wrap(rootHash);
  }

  private static boolean codeIsEmpty(final Bytes value) {
    return value == null || value.isEmpty();
  }

  private void generateAccountValues(
      final Address accountKey,
      final LeafBuilder leafBuilder,
      final Optional<BinTrieWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final BinTrieWorldStateUpdateAccumulator worldStateUpdater) {
    var accountUpdate = worldStateUpdater.getAccountsToUpdate().get(accountKey);
    if (accountUpdate == null || accountUpdate.isUnchanged()) {
      return;
    }
    if (accountUpdate.getUpdated() == null) {
      leafBuilder.generateAccountKeyForRemoval(accountKey);
      leafBuilder.generateCodeHashKeyForRemoval(accountKey);
      final Hash addressHash = hashAndSavePreImage(accountKey);
      maybeStateUpdater.ifPresent(updater -> updater.removeAccountInfoState(addressHash));
      return;
    }

    handleCoupledCodeAccountUpdates(accountKey, leafBuilder, accountUpdate, worldStateUpdater);

    final BinTrieAccount updatedAcount = accountUpdate.getUpdated();
    leafBuilder.generateAccountKeyValueForUpdate(
        accountKey, updatedAcount.getNonce(), updatedAcount.getBalance());
    maybeStateUpdater.ifPresent(
        updater ->
            updater.putAccountInfoState(
                hashAndSavePreImage(accountKey), updatedAcount.serializeAccount()));
  }

  private void handleCoupledCodeAccountUpdates(
      final Address accountKey,
      final LeafBuilder leafBuilder,
      final PathBasedValue<BinTrieAccount> accountUpdate,
      final BinTrieWorldStateUpdateAccumulator worldStateUpdater) {
    final BinTrieAccount priorAccount = accountUpdate.getPrior();
    final BinTrieAccount updatedAccount = accountUpdate.getUpdated();

    // creating new account adds in codehash as well
    if (priorAccount == null) {
      leafBuilder.generateCodeHashKeyValueForUpdate(accountKey, updatedAccount.getCodeHash());
      return;
    }
    Optional<Bytes> currentCode =
        worldStateUpdater.getCode(accountKey, updatedAccount.getCodeHash());
    currentCode.ifPresent(
        code -> leafBuilder.generateCodeSizeKeyValueForUpdate(accountKey, code.size()));
  }

  private void generateCodeValues(
      final Address accountKey,
      final LeafBuilder leafBuilder,
      final Optional<BinTrieWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final PathBasedValue<Bytes> codeUpdate) {
    if (codeUpdate == null
        || codeUpdate.isUnchanged()
        || (codeIsEmpty(codeUpdate.getPrior()) && codeIsEmpty(codeUpdate.getUpdated()))) {
      return;
    }
    if (codeUpdate.getUpdated() == null) {
      final Hash priorCodeHash = Hash.hash(codeUpdate.getPrior());
      leafBuilder.generateCodeKeysForRemoval(accountKey, codeUpdate.getPrior());
      final Hash accountHash = accountKey.addressHash();
      maybeStateUpdater.ifPresent(updater -> updater.removeCode(accountHash, priorCodeHash));
      return;
    }
    final Hash accountHash = accountKey.addressHash();
    final Hash codeHash = Hash.hash(codeUpdate.getUpdated());
    leafBuilder.generateCodeKeyValuesForUpdate(accountKey, codeUpdate.getUpdated(), codeHash);
    if (codeUpdate.getUpdated().isEmpty()) {
      maybeStateUpdater.ifPresent(updater -> updater.removeCode(accountHash, codeHash));
    } else {
      maybeStateUpdater.ifPresent(
          updater -> updater.putCode(accountHash, codeHash, codeUpdate.getUpdated()));
    }
  }

  private void generateStorageValues(
      final Address accountKey,
      final LeafBuilder leafBuilder,
      final Optional<BinTrieWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>> storageAccountUpdate) {
    if (storageAccountUpdate == null || storageAccountUpdate.keySet().isEmpty()) {
      return;
    }
    final Hash updatedAddressHash = accountKey.addressHash();
    // for manicured tries and composting, collect branches here (not implemented)
    for (final Map.Entry<StorageSlotKey, PathBasedValue<UInt256>> storageUpdate :
        storageAccountUpdate.entrySet()) {
      final Hash slotHash = storageUpdate.getKey().getSlotHash();
      if (!storageUpdate.getValue().isUnchanged()) {
        final UInt256 updatedStorage = storageUpdate.getValue().getUpdated();
        if (updatedStorage == null) {
          leafBuilder.generateStorageKeyForRemoval(accountKey, storageUpdate.getKey());
          maybeStateUpdater.ifPresent(
              updater -> updater.removeStorageValueBySlotHash(updatedAddressHash, slotHash));
        } else {
          leafBuilder.generateStorageKeyValueForUpdate(
              accountKey, storageUpdate.getKey(), updatedStorage);
          maybeStateUpdater.ifPresent(
              updater ->
                  updater.putStorageValueBySlotHash(updatedAddressHash, slotHash, updatedStorage));
        }
      }
    }
  }

  private void updateState(
      final Address accountKey,
      final BinaryTrie stateTrie,
      final Optional<BinTrieWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final BinTrieWorldStateUpdateAccumulator worldStateUpdater) {

    final LeafBuilder leafBuilder = new LeafBuilder(new TrieKeyFactory(new StemHasher()));

    generateAccountValues(accountKey, leafBuilder, maybeStateUpdater, worldStateUpdater);

    generateCodeValues(
        accountKey,
        leafBuilder,
        maybeStateUpdater,
        worldStateUpdater.getCodeToUpdate().get(accountKey));

    generateStorageValues(
        accountKey,
        leafBuilder,
        maybeStateUpdater,
        worldStateUpdater.getStorageToUpdate().get(accountKey));

    leafBuilder
        .getKeysForRemoval()
        .forEach(
            key -> {
              System.out.println("remove key " + key);
              stateTrie.remove(key);
            });
    leafBuilder
        .getNonStorageKeyValuesForUpdate()
        .forEach(
            (key, value) -> {
              System.out.println("add key " + key + " leaf value " + value);
              stateTrie.put(key, value);
            });
    leafBuilder
        .getStorageKeyValuesForUpdate()
        .forEach(
            (storageSlotKey, pair) -> {
              var storageAccountUpdate = worldStateUpdater.getStorageToUpdate().get(accountKey);
              if (storageAccountUpdate == null) {
                return;
              }
              System.out.println(
                  "add storage key " + pair.getFirst() + "  value " + pair.getSecond());
              Optional<PathBasedValue<UInt256>> storageUpdate =
                  Optional.ofNullable(storageAccountUpdate.get(storageSlotKey));
              stateTrie
                  .put(pair.getFirst(), pair.getSecond())
                  .ifPresentOrElse(
                      bytes ->
                          storageUpdate.ifPresent(
                              storage -> storage.setPrior(UInt256.fromBytes(bytes))),
                      () -> storageUpdate.ifPresent(storage -> storage.setPrior(null)));
            });
  }

  public Set<Address> getAddressesToPersist(
      final PathBasedWorldStateUpdateAccumulator<?> accumulator) {
    Set<Address> mergedAddresses =
        new HashSet<>(accumulator.getAccountsToUpdate().keySet()); // accountsToUpdate
    mergedAddresses.addAll(accumulator.getCodeToUpdate().keySet()); // codeToUpdate
    mergedAddresses.addAll(accumulator.getStorageToClear()); // storageToClear
    mergedAddresses.addAll(accumulator.getStorageToUpdate().keySet()); // storageToUpdate
    return mergedAddresses;
  }

  @Override
  public MutableWorldState freezeStorage() {
    this.isStorageFrozen = true;
    this.worldStateKeyValueStorage =
        new BinTrieLayeredWorldStateKeyValueStorage(getWorldStateStorage());
    return this;
  }

  @Override
  public Account get(final Address address) {
    return getWorldStateStorage().getAccount(address, accumulator).orElse(null);
  }

  @Override
  public Optional<Bytes> getCode(@Nonnull final Address address, final Hash codeHash) {
    return getWorldStateStorage().getCode(codeHash, address.addressHash());
  }

  @Override
  public UInt256 getStorageValue(final Address address, final UInt256 storageKey) {
    return getStorageValueByStorageSlotKey(address, new StorageSlotKey(storageKey))
        .orElse(UInt256.ZERO);
  }

  @Override
  public Optional<UInt256> getStorageValueByStorageSlotKey(
      final Address address, final StorageSlotKey storageSlotKey) {
    return getWorldStateStorage().getStorageValueByStorageSlotKey(address, storageSlotKey);
  }

  @Override
  public UInt256 getPriorStorageValue(final Address address, final UInt256 storageKey) {
    return getStorageValue(address, storageKey);
  }

  @Override
  public Map<Bytes32, Bytes> getAllAccountStorage(final Address address, final Hash rootHash) {
    return Collections.emptyMap();
  }

  private BinaryTrie createTrie(final NodeLoader nodeLoader) {
    return new BinaryTrie(nodeLoader);
  }

  protected void writeTrieNode(
      final SegmentIdentifier segmentId,
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes location,
      final Bytes value) {
    tx.put(segmentId, location.toArrayUnsafe(), value.toArrayUnsafe());
  }

  protected void removeTrieNode(
      final SegmentIdentifier segmentId,
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes location) {
    tx.remove(segmentId, location.toArrayUnsafe());
  }

  protected Hash hashAndSavePreImage(final Bytes value) {
    // by default do not save has preImages
    return Hash.hash(value);
  }

  @Override
  public Hash frontierRootHash() {
    return calculateRootHash(
        Optional.of(
            new BinTrieWorldStateKeyValueStorage.Updater(
                noOpSegmentedTx,
                noOpTx,
                worldStateKeyValueStorage.getFlatDbStrategy(),
                worldStateKeyValueStorage.getComposedWorldStateStorage())),
        accumulator.copy());
  }

  @Override
  protected Hash getEmptyTrieHash() {
    return Hash.wrap(Bytes32.ZERO);
  }
}
