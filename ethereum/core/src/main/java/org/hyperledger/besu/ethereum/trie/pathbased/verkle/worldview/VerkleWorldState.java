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
package org.hyperledger.besu.ethereum.trie.pathbased.verkle.worldview;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.VERKLE_TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.stateless.adapter.TrieKeyFactory;
import org.hyperledger.besu.ethereum.stateless.adapter.TrieKeyUtils;
import org.hyperledger.besu.ethereum.stateless.hasher.StemHasher;
import org.hyperledger.besu.ethereum.stateless.util.Parameters;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.cache.PathBasedCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.ethereum.trie.pathbased.transition.MigratedDiffValue;
import org.hyperledger.besu.ethereum.trie.pathbased.verkle.LeafBuilder;
import org.hyperledger.besu.ethereum.trie.pathbased.verkle.VerkleAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.verkle.VerkleWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.verkle.cache.preloader.StemPreloader;
import org.hyperledger.besu.ethereum.trie.pathbased.verkle.cache.preloader.VerklePreloader;
import org.hyperledger.besu.ethereum.trie.pathbased.verkle.storage.VerkleLayeredWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.verkle.storage.VerkleWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.verkletrie.VerkleTrie;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"unused", "MismatchedQueryAndUpdateOfCollection", "ModifiedButNotUsed"})
public class VerkleWorldState extends PathBasedWorldState {

  private static final Logger LOG = LoggerFactory.getLogger(VerkleWorldState.class);

  private final VerklePreloader verklePreloader;

  public VerkleWorldState(
      final VerkleWorldStateProvider archive,
      final VerkleWorldStateKeyValueStorage worldStateKeyValueStorage,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig) {
    this(
        worldStateKeyValueStorage,
        archive.getCachedWorldStorageManager(),
        archive.getTrieLogManager(),
        evmConfiguration,
        worldStateConfig);
  }

  public VerkleWorldState(
      final VerkleWorldStateKeyValueStorage worldStateKeyValueStorage,
      final PathBasedCachedWorldStorageManager cachedWorldStorageManager,
      final TrieLogManager trieLogManager,
      final EvmConfiguration evmConfiguration,
      final WorldStateConfig worldStateConfig) {
    super(worldStateKeyValueStorage, cachedWorldStorageManager, trieLogManager, worldStateConfig);
    this.verklePreloader = new VerklePreloader(worldStateKeyValueStorage.getStemPreloader());
    this.setAccumulator(
        new VerkleWorldStateUpdateAccumulator(
            this,
            (addr, value) -> verklePreloader.preLoadAccount(addr),
            verklePreloader::preLoadStorageSlot,
            verklePreloader::preLoadCode,
            evmConfiguration));
  }

  public VerkleWorldState(
      final VerkleWorldStateKeyValueStorage worldStateKeyValueStorage,
      final PathBasedCachedWorldStorageManager cachedWorldStorageManager,
      final TrieLogManager trieLogManager,
      final WorldStateConfig worldStateConfig) {
    super(worldStateKeyValueStorage, cachedWorldStorageManager, trieLogManager, worldStateConfig);
    this.verklePreloader = new VerklePreloader(worldStateKeyValueStorage.getStemPreloader());
  }

  @Override
  public VerkleWorldStateKeyValueStorage getWorldStateStorage() {
    return (VerkleWorldStateKeyValueStorage) worldStateKeyValueStorage;
  }

  @Override
  public void persist(final BlockHeader blockHeader) {
    final Optional<BlockHeader> maybeBlockHeader = Optional.ofNullable(blockHeader);
    LOG.atDebug()
        .setMessage("Persist world state for block {}")
        .addArgument(maybeBlockHeader)
        .log();

    boolean success = false;

    final PathBasedWorldStateKeyValueStorage.Updater stateUpdater =
        worldStateKeyValueStorage.updater();
    Runnable saveTrieLog = () -> {}; // Default action for saving trie log

    try {
      final Hash calculatedRootHash;

      if (blockHeader == null) {
        // No block header: calculate root from current state unless storage is frozen
        calculatedRootHash =
            calculateRootHash(
                isStorageFrozen ? Optional.empty() : Optional.of(stateUpdater), accumulator);
      } else if (!worldStateConfig.isTrieDisabled()) {
        // Normal case: calculate root using the block header
        calculatedRootHash = unsafeRootHashUpdate(blockHeader, stateUpdater);
      } else {
        // Trie is disabled: fallback to block's root hash, assuming trusted context
        calculatedRootHash = unsafeRootHashUpdate(blockHeader, stateUpdater);
      }

      // if we are persisted with a block header, and the prior state is the parent
      // then persist the TrieLog for that transition.
      // If specified but not a direct descendant simply store the new block hash.
      if (blockHeader != null) {
        verifyWorldStateRoot(calculatedRootHash, blockHeader);
        final PathBasedWorldStateUpdateAccumulator<?> localCopy = accumulator.copy();
        saveTrieLog =
            () -> {
              trieLogManager.saveTrieLog(localCopy, calculatedRootHash, blockHeader, this);
              // not save a frozen state in the cache
              if (!isStorageFrozen) {
                cachedWorldStorageManager.addCachedLayer(blockHeader, calculatedRootHash, this);
              }
            };
        stateUpdater.saveWorldState(blockHeader.getHash(), calculatedRootHash);
        worldStateBlockHash = blockHeader.getHash();
        worldStateRootHash = calculatedRootHash;
      } else {
        stateUpdater.saveWorldState(Hash.ZERO, calculatedRootHash);
        worldStateBlockHash = Hash.ZERO;
        worldStateRootHash = calculatedRootHash;
      }
      success = true;
    } finally {
      if (success) {
        stateUpdater.commit();
        accumulator.reset();
        saveTrieLog.run();
      } else {
        stateUpdater.rollback();
        accumulator.reset();
      }
    }
  }

  @Override
  protected Hash calculateRootHash(
      final Optional<PathBasedWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final PathBasedWorldStateUpdateAccumulator<?> worldStateUpdater) {
    return internalCalculateRootHash(
        maybeStateUpdater.map(VerkleWorldStateKeyValueStorage.Updater.class::cast),
        (VerkleWorldStateUpdateAccumulator) worldStateUpdater);
  }

  protected Hash internalCalculateRootHash(
      final Optional<VerkleWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final VerkleWorldStateUpdateAccumulator worldStateUpdater) {

    final VerkleTrie stateTrie =
        createTrie(
            (location, hash) -> worldStateKeyValueStorage.getStateTrieNode(location),
            worldStateRootHash);

    final StemPreloader stemPreloader = verklePreloader.stemPreloader();

    // For each address that needs to be updated in the state, generate all the necessary leaf keys
    // (basic data leaf, code, code hash, storage slots, etc.). Then, preload the stems in an
    // optimized
    // and parallelized manner. These stems will later be used to update the trie. Some stems may
    // have
    // already been generated during block processing, so they will not be regenerated here.
    final Set<Address> addressesToPersist = getAddressesToPersist(worldStateUpdater);
    addressesToPersist.parallelStream()
        .forEach(
            accountKey -> {
              final PathBasedValue<VerkleAccount> accountUpdate =
                  worldStateUpdater.getAccountsToUpdate().get(accountKey);
              // Collect all leaf keys (for accounts, storage, and code) for the account
              final List<Bytes32> leafKeys = new ArrayList<>();

              if (accountUpdate != null && !accountUpdate.isUnchanged()) {
                leafKeys.add(TrieKeyUtils.getAccountKeyTrieIndex());
                if (accountUpdate.getPrior() == null) {
                  leafKeys.add(Parameters.CODE_HASH_LEAF_KEY);
                }
              }

              // Process storage updates if needed
              final StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>>
                  storageAccountUpdate = worldStateUpdater.getStorageToUpdate().get(accountKey);
              if (storageAccountUpdate != null) {
                final List<Bytes32> storageSlotKeys =
                    storageAccountUpdate.keySet().stream()
                        .map(
                            storageSlotKey ->
                                storageSlotKey
                                    .getSlotKey()
                                    .orElseThrow(
                                        () -> new IllegalStateException("Slot key is missing")))
                        .map(Bytes32::wrap) // Correct way to call static method wrap()
                        .collect(Collectors.toList());

                if (!storageSlotKeys.isEmpty()) {
                  leafKeys.addAll(TrieKeyUtils.getStorageKeyTrieIndexes(storageSlotKeys));
                }
              }

              // Process code updates if needed
              final PathBasedValue<Bytes> codeUpdate =
                  worldStateUpdater.getCodeToUpdate().get(accountKey);
              if (codeUpdate != null && !codeUpdate.isUnchanged()) {
                final Bytes previousCode = codeUpdate.getPrior();
                final Bytes updatedCode = codeUpdate.getUpdated();

                // Ensure code is not empty and only update if the code has actually changed
                if (!(codeIsEmpty(previousCode) && codeIsEmpty(updatedCode))) {
                  leafKeys.add(Parameters.CODE_HASH_LEAF_KEY);
                  leafKeys.addAll(
                      TrieKeyUtils.getCodeChunkKeyTrieIndexes(
                          updatedCode == null ? previousCode : updatedCode));
                }
              }
              stemPreloader.preloadStems(accountKey, leafKeys);
            });

    int updateCount = 0;
    for (final Address accountKey : addressesToPersist) {
      updateCount +=
          updateState(
              accountKey,
              stateTrie,
              maybeStateUpdater,
              stemPreloader.getStemHasherByAddress(accountKey),
              worldStateUpdater);
    }

    System.out.println("Total updates: " + updateCount);

    LOG.info("Starting commit...");
    long startTime = System.currentTimeMillis();

    maybeStateUpdater.ifPresent(
        verkleUpdater ->
            stateTrie.commit(
                (location, hash, value) -> {
                  if (value == null) {
                    removeTrieNode(
                        VERKLE_TRIE_BRANCH_STORAGE,
                        verkleUpdater.getWorldStateTransaction(),
                        location);
                  } else {
                    writeTrieNode(
                        VERKLE_TRIE_BRANCH_STORAGE,
                        verkleUpdater.getWorldStateTransaction(),
                        location,
                        value);
                  }
                }));

    // LOG.info(stateTrie.toDotTree());
    final Bytes32 rootHash = stateTrie.getRootHash();
    long endTime = System.currentTimeMillis();
    LOG.info("Commit completed in {}", (endTime - startTime) + " : " + rootHash);

    return Hash.wrap(rootHash);
  }

  private static boolean codeIsEmpty(final Bytes value) {
    return value == null || value.isEmpty();
  }

  private boolean processAccountUpdates(
      final Address accountAddress,
      final LeafBuilder leafBuilder,
      final boolean hasStorageUpdate,
      final boolean hasCodeUpdate,
      final Optional<VerkleWorldStateKeyValueStorage.Updater> optionalStateUpdater,
      final VerkleWorldStateUpdateAccumulator worldStateUpdater) {

    // Retrieve the account update for the given account
    var accountUpdate = worldStateUpdater.getAccountsToUpdate().get(accountAddress);
    // Determine if the account update should be skipped based on whether the account is unchanged
    // and if there are no storage or code updates
    boolean shouldSkipAccount =
        accountUpdate == null
            || (accountUpdate.isUnchanged() && !hasStorageUpdate && !hasCodeUpdate);

    /*System.out.println(
        "add basic key "
            + accountAddress
            + " "
            + (accountUpdate != null)
            + " "
            + new TrieKeyFactory(new StemPreloader().getStemHasherByAddress(accountAddress))
                .basicDataKey(accountAddress)
            + " "
            + shouldSkipAccount);*/

    // System.out.println("skip "+accountKey+" "+skipAccount+" "+((accountUpdate !=
    // null)?accountUpdate.getClass().toString():"null"));
    // Handling the migration of the account if it is a migrated diff (this is only necessary during
    // the transition period)
    // Handle migration for the account if it's a migrated diff (used during the transition period)
    if (accountUpdate instanceof MigratedDiffValue<VerkleAccount> migratedAccount) {
      final PathBasedValue<VerkleAccount> replacement =
          new PathBasedValue<>(
              shouldSkipAccount ? migratedAccount.getUpdated() : migratedAccount.getPrior(),
              migratedAccount.getUpdated(),
              migratedAccount.isLastStepCleared());
      worldStateUpdater.getAccountsToUpdate().put(accountAddress, replacement);
    }

    if (shouldSkipAccount) {
      return false;
    }

    // If the updated account is null, it means the account is being removed
    if (accountUpdate.getUpdated() == null) {
      // Generate removal keys for the account and code hash
      leafBuilder.generateAccountKeyForRemoval(accountAddress);
      leafBuilder.generateCodeHashKeyForRemoval(accountAddress);

      // Hash and save the pre-image for the account key, and remove the account info from the state
      final Hash accountHash = hashAndSavePreImage(accountAddress);
      optionalStateUpdater.ifPresent(
          stateUpdater -> stateUpdater.removeAccountInfoState(accountHash));
      return true;
    }

    // Handle updates for both account and associated code together
    handleCoupledCodeAccountUpdates(accountAddress, leafBuilder, accountUpdate, worldStateUpdater);

    // Get the updated account data and generate the key-value update for the account
    final VerkleAccount updatedAccount = accountUpdate.getUpdated();
    leafBuilder.generateAccountKeyValueForUpdate(
        accountAddress, updatedAccount.getNonce(), updatedAccount.getBalance());

    // If the state updater is available, update the account information in the state
    optionalStateUpdater.ifPresent(
        stateUpdater ->
            stateUpdater.putAccountInfoState(
                hashAndSavePreImage(accountAddress), updatedAccount.serializeAccount()));

    return true;
  }

  private void handleCoupledCodeAccountUpdates(
      final Address accountKey,
      final LeafBuilder leafBuilder,
      final PathBasedValue<VerkleAccount> accountUpdate,
      final VerkleWorldStateUpdateAccumulator worldStateUpdater) {
    // Retrieve the prior and updated account details from the account update object
    final VerkleAccount priorAccount = accountUpdate.getPrior();
    final VerkleAccount updatedAccount = accountUpdate.getUpdated();
    // If the account is being created (no prior account), we need to add the code hash and code
    // size for the new account
    if (priorAccount == null) {
      // Add code hash key-value for the new account
      leafBuilder.generateCodeHashKeyValueForUpdate(accountKey, updatedAccount.getCodeHash());
      // Add code size key-value for the new account
      leafBuilder.generateCodeSizeKeyValueForUpdate(accountKey, updatedAccount.getCodeSize().get());
      return;
    }
    // If the account already exists (priorAccount is not null), check if the code has changed
    Optional<Bytes> currentCode =
        worldStateUpdater.getCode(accountKey, updatedAccount.getCodeHash());
    // If the current code exists, update the code size for the account in the world state
    currentCode.ifPresent(
        code -> leafBuilder.generateCodeSizeKeyValueForUpdate(accountKey, code.size()));
  }

  private boolean processCodeUpdates(
      final Address accountAddress,
      final LeafBuilder leafBuilder,
      final Optional<VerkleWorldStateKeyValueStorage.Updater> optionalStateUpdater,
      final PathBasedValue<Bytes> codeUpdate) {

    // If there is no update, or if the code hasn't changed, or if both prior and updated code are
    // empty, return false
    if (codeUpdate == null
        || codeUpdate.isUnchanged()
        || (codeIsEmpty(codeUpdate.getPrior()) && codeIsEmpty(codeUpdate.getUpdated()))) {
      return false;
    }

    // This means the code is being removed (rollback to previous state)
    if (codeUpdate.getUpdated() == null) {
      final Hash priorCodeHash = Hash.hash(codeUpdate.getPrior());
      // Remove the code and its associated keys from the state
      leafBuilder.generateCodeKeysForRemoval(accountAddress, codeUpdate.getPrior());
      final Hash accountAddressHash = accountAddress.addressHash();
      optionalStateUpdater.ifPresent(
          stateUpdater -> stateUpdater.removeCode(accountAddressHash, priorCodeHash));
      return true;
    }

    // If the updated code is not null, update the code in the state
    final Hash accountAddressHash = accountAddress.addressHash();
    final Hash codeHash = Hash.hash(codeUpdate.getUpdated());
    leafBuilder.generateCodeKeyValuesForUpdate(accountAddress, codeUpdate.getUpdated(), codeHash);

    // The code is empty, so we store it as empty in the state
    if (codeUpdate.getUpdated().isEmpty()) {
      optionalStateUpdater.ifPresent(
          stateUpdater -> stateUpdater.removeCode(accountAddressHash, codeHash));
    } else {
      // Otherwise, add the updated code to the state
      optionalStateUpdater.ifPresent(
          stateUpdater ->
              stateUpdater.putCode(accountAddressHash, codeHash, codeUpdate.getUpdated()));
    }

    return true;
  }

  private boolean generateStorageValues(
      final Address accountAddress,
      final LeafBuilder leafBuilder,
      final Optional<VerkleWorldStateKeyValueStorage.Updater> optionalStateUpdater,
      final VerkleWorldStateUpdateAccumulator worldStateAccumulator) {

    // Get the storage updates for the given account
    final StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>> storageUpdates =
        worldStateAccumulator.getStorageToUpdate().get(accountAddress);

    // If no storage updates are available, return false
    if (storageUpdates == null || storageUpdates.isEmpty()) {
      return false;
    }

    boolean isStorageUpdated = false;
    final Hash accountAddressHash = accountAddress.addressHash();

    // Iterate through each storage update
    final Iterator<Map.Entry<StorageSlotKey, PathBasedValue<UInt256>>> storageIterator =
        storageUpdates.entrySet().iterator();
    while (storageIterator.hasNext()) {
      Map.Entry<StorageSlotKey, PathBasedValue<UInt256>> storageEntry = storageIterator.next();
      final Hash storageSlotHash = storageEntry.getKey().getSlotHash();
      final PathBasedValue<UInt256> storageData = storageEntry.getValue();

      // Handle migration of the storage if it's a migrated diff
      if (storageData instanceof MigratedDiffValue<UInt256> migratedData) {
        final PathBasedValue<UInt256> updatedStorageData =
            new PathBasedValue<>(
                storageData.isUnchanged() ? migratedData.getUpdated() : migratedData.getPrior(),
                migratedData.getUpdated(),
                migratedData.isLastStepCleared());
        storageUpdates.put(storageEntry.getKey(), updatedStorageData);
      }

      // Skip if storage hasn't changed
      if (storageData.isUnchanged()) {
        continue;
      }

      // Indicate that storage has been updated
      isStorageUpdated = true;

      // Handle removal or update of storage
      final UInt256 newStorageValue = storageData.getUpdated();
      if (newStorageValue == null) {
        leafBuilder.generateStorageKeyForRemoval(accountAddress, storageEntry.getKey());
        optionalStateUpdater.ifPresent(
            stateUpdater ->
                stateUpdater.removeStorageValueBySlotHash(accountAddressHash, storageSlotHash));
      } else {
        leafBuilder.generateStorageKeyValueForUpdate(
            accountAddress, storageEntry.getKey(), newStorageValue);
        optionalStateUpdater.ifPresent(
            stateUpdater ->
                stateUpdater.putStorageValueBySlotHash(
                    accountAddressHash, storageSlotHash, newStorageValue));
      }
    }

    return isStorageUpdated;
  }

  private int updateState(
      final Address accountKey,
      final VerkleTrie stateTrie,
      final Optional<VerkleWorldStateKeyValueStorage.Updater> maybeStateUpdater,
      final StemHasher stemHasher,
      final VerkleWorldStateUpdateAccumulator worldStateUpdater) {

    final LeafBuilder leafBuilder = new LeafBuilder(new TrieKeyFactory(stemHasher));

    final boolean hasCodeUpdate =
        processCodeUpdates(
            accountKey,
            leafBuilder,
            maybeStateUpdater,
            worldStateUpdater.getCodeToUpdate().get(accountKey));

    final boolean hasStorageUpdate =
        generateStorageValues(accountKey, leafBuilder, maybeStateUpdater, worldStateUpdater);

    //System.out.println("has ?" + accountKey + " " + hasStorageUpdate + " " + hasCodeUpdate);
    processAccountUpdates(
        accountKey,
        leafBuilder,
        hasStorageUpdate,
        hasCodeUpdate,
        maybeStateUpdater,
        worldStateUpdater);

    leafBuilder
        .getKeysForRemoval()
        .forEach(
            key -> {
              //System.out.println("remove key " + key);
              stateTrie.remove(key);
            });
    leafBuilder
        .getNonStorageKeyValuesForUpdate()
        .forEach(
            (key, value) -> {
              //System.out.println("add key " + key + " leaf value " + value);
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
              //System.out.println(
              //    "add storage key " + pair.getFirst() + "  value " + pair.getSecond());

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

    return leafBuilder.getKeysForRemoval().size()
        + leafBuilder.getNonStorageKeyValuesForUpdate().size()
        + leafBuilder.getStorageKeyValuesForUpdate().size();
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
        new VerkleLayeredWorldStateKeyValueStorage(getWorldStateStorage());
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

  private VerkleTrie createTrie(final NodeLoader nodeLoader, final Hash worldStateRootHash) {
    return new VerkleTrie(nodeLoader);
  }

  protected void removeTrieNode(
      final SegmentIdentifier segmentId,
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes location) {
    tx.remove(segmentId, location.toArrayUnsafe());
  }

  protected void writeTrieNode(
      final SegmentIdentifier segmentId,
      final SegmentedKeyValueStorageTransaction tx,
      final Bytes location,
      final Bytes value) {
    tx.put(segmentId, location.toArrayUnsafe(), value.toArrayUnsafe());
  }

  protected Hash hashAndSavePreImage(final Bytes value) {
    // by default do not save has preImages
    return Hash.hash(value);
  }

  @Override
  public Hash frontierRootHash() {
    return calculateRootHash(
        Optional.of(
            new VerkleWorldStateKeyValueStorage.Updater(
                noOpSegmentedTx, noOpTx, worldStateKeyValueStorage.getFlatDbStrategy())),
        accumulator.copy());
  }

  @Override
  protected Hash getEmptyTrieHash() {
    return Hash.wrap(Bytes32.ZERO);
  }

  public VerklePreloader getVerklePreloader() {
    return verklePreloader;
  }

  @Override
  public VerkleWorldStateUpdateAccumulator getAccumulator() {
    return (VerkleWorldStateUpdateAccumulator) super.getAccumulator();
  }
}
