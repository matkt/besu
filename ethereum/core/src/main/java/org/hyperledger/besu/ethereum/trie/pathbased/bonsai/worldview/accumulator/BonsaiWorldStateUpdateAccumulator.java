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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.MptAccountValue;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.MptTrieFactory;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BinaryStorageRootStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.MptStorageRootStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.StorageRootStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldView;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.AccountConsumingMap;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.Consumer;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.AbstractWorldUpdater;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogAccumulator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class BonsaiWorldStateUpdateAccumulator
    extends AbstractWorldUpdater<BonsaiWorldView, BonsaiAccount>
    implements BonsaiWorldView, TrieLogAccumulator {
  private static final Logger LOG =
      LoggerFactory.getLogger(BonsaiWorldStateUpdateAccumulator.class);
  protected final Consumer<BonsaiValue<BonsaiAccount>> accountPreloader;
  protected final Consumer<StorageSlotKey> storagePreloader;

  private final AccountConsumingMap<BonsaiValue<BonsaiAccount>> accountsToUpdate;
  private final Map<Address, BonsaiValue<Bytes>> codeToUpdate = new ConcurrentHashMap<>();
  private final Set<Address> storageToClear = Collections.synchronizedSet(new HashSet<>());
  protected final EvmConfiguration evmConfiguration;
  private final BonsaiCodeCache codeCache;

  // storage sub mapped by _hashed_ key.  This is because in self_destruct calls we need to
  // enumerate the old storage and delete it.  Those are trie stored by hashed key by spec and the
  // alternative was to keep a giant pre-image cache of the entire trie.
  private final Map<Address, StorageConsumingMap<StorageSlotKey, BonsaiValue<UInt256>>>
      storageToUpdate = new ConcurrentHashMap<>();

  private final Map<UInt256, Hash> storageKeyHashLookup = new ConcurrentHashMap<>();

  /** Code hashes newly introduced by the block currently flowing through the accumulator. */
  private final Set<Hash> introducedCodeHashes = Collections.synchronizedSet(new HashSet<>());

  protected boolean isAccumulatorStateChanged;

  public BonsaiWorldStateUpdateAccumulator(
      final BonsaiWorldView world,
      final Consumer<BonsaiValue<BonsaiAccount>> accountPreloader,
      final Consumer<StorageSlotKey> storagePreloader,
      final EvmConfiguration evmConfiguration,
      final BonsaiCodeCache codeCache) {
    super(world, evmConfiguration);
    this.accountsToUpdate = new AccountConsumingMap<>(new ConcurrentHashMap<>(), accountPreloader);
    this.accountPreloader = accountPreloader;
    this.storagePreloader = storagePreloader;
    this.isAccumulatorStateChanged = false;
    this.evmConfiguration = evmConfiguration;
    this.codeCache = codeCache;
  }

  public void cloneFromUpdater(final BonsaiWorldStateUpdateAccumulator source) {
    accountsToUpdate.putAll(source.getAccountsToUpdate());
    codeToUpdate.putAll(source.codeToUpdate);
    storageToClear.addAll(source.storageToClear);
    storageToUpdate.putAll(source.storageToUpdate);
    updatedAccounts.putAll(source.updatedAccounts);
    deletedAccounts.addAll(source.deletedAccounts);
    introducedCodeHashes.addAll(source.introducedCodeHashes);
    this.isAccumulatorStateChanged = true;
  }

  /**
   * Integrates prior state changes from an external source into the current state. This method
   * retrieves state modifications from the specified source and adds them to the current state's
   * list of modifications. It does not remove any existing elements in the current state's
   * modification list. If a modification has been made in both the current state and the source,
   * the modification from the source will be taken. This approach ensures that the source's state
   * changes are prioritized and overrides any conflicting changes in the current state.
   *
   * @param source The source accumulator whose changes take priority
   */
  public void importStateChangesFromSource(final BonsaiWorldStateUpdateAccumulator source) {
    importFrom(source, ImportMode.UPSERT);
    storageToClear.addAll(source.storageToClear);
  }

  /**
   * Imports unchanged (prior-only) state data from an external source. Only data not already
   * present in this accumulator is imported — existing entries are never overwritten. Both prior
   * and updated values are set to the source's prior value (i.e. read-only snapshot).
   *
   * @param source The source accumulator to import prior state from
   */
  public void importPriorStateFromSource(final BonsaiWorldStateUpdateAccumulator source) {
    importFrom(source, ImportMode.INSERT);
  }

  public void importStateChangesFromPartialView(final PartialBlockAccessView partialView) {
    importStateChangesFromPartialView(partialView, false);
  }

  /**
   * Imports writes from a {@link PartialBlockAccessView} produced by parallel BAL execution.
   * Account and storage changes are inserted directly as {@link BonsaiValue} entries so {@link
   * #commit()} does not need to re-read prior values from the database.
   *
   * @param partialView partial access list for a single transaction
   * @param clearEmptyAccounts when true, delete accounts that become empty after applying writes
   */
  public void importStateChangesFromPartialView(
      final PartialBlockAccessView partialView, final boolean clearEmptyAccounts) {
    for (final PartialBlockAccessView.AccountChanges accountChanges :
        partialView.accountChanges()) {
      final Address address = accountChanges.getAddress();
      final boolean hasHeaderChange =
          accountChanges.getPostBalance().isPresent()
              || accountChanges.getNonceChange().isPresent()
              || accountChanges.getNewCode().isPresent();
      final boolean hasStorageChange = !accountChanges.getStorageChanges().isEmpty();

      if (!hasHeaderChange && !hasStorageChange) {
        continue;
      }

      MutableAccount accountValue = getOrCreate(address);

      boolean shouldCheckForEmptyAccount = false;

      if (accountChanges.getPostBalance().isPresent()) {
        final Wei balance = accountChanges.getPostBalance().get();
        accountValue.setBalance(balance);
        shouldCheckForEmptyAccount = clearEmptyAccounts && balance.isZero();
      }

      if (accountChanges.getNonceChange().isPresent()) {
        final long nonce = accountChanges.getNonceChange().get();
        accountValue.setNonce(nonce);
        shouldCheckForEmptyAccount |= clearEmptyAccounts && nonce == 0L;
      }

      if (accountChanges.getNewCode().isPresent()) {
        final Bytes code = accountChanges.getNewCode().get();
        accountValue.setCode(code);
        shouldCheckForEmptyAccount |= clearEmptyAccounts && code.isEmpty();
      }

      if (hasStorageChange) {
        final StorageConsumingMap<StorageSlotKey, BonsaiValue<UInt256>> pendingStorageUpdates =
            storageToUpdate.computeIfAbsent(
                address,
                k ->
                    new StorageConsumingMap<>(
                        address, new ConcurrentHashMap<>(), storagePreloader));
        for (final PartialBlockAccessView.SlotChange slotChange :
            accountChanges.getStorageChanges()) {
          final StorageSlotKey slotKey = slotChange.slot();
          final UInt256 prior =
              slotChange.previousValue() != null ? slotChange.previousValue() : UInt256.ZERO;
          final UInt256 updated =
              slotChange.newValue() != null ? slotChange.newValue() : UInt256.ZERO;

          final BonsaiValue<UInt256> pendingValue = pendingStorageUpdates.get(slotKey);
          if (pendingValue == null) {
            pendingStorageUpdates.put(slotKey, new BonsaiValue<>(prior, updated));
          } else {
            pendingValue.setUpdated(updated);
          }
        }
      }

      if (shouldCheckForEmptyAccount && accountValue.isEmpty()) {
        deleteAccount(address);
      }
    }
    this.isAccumulatorStateChanged = true;
  }

  private enum ImportMode {
    /** Insert new entries and update existing ones with values from the source. */
    UPSERT,
    /** Insert new entries only, existing entries are left untouched. */
    INSERT
  }

  private void importFrom(final BonsaiWorldStateUpdateAccumulator source, final ImportMode mode) {
    final boolean priorOnly = mode == ImportMode.INSERT;

    source
        .getAccountsToUpdate()
        .forEach(
            (address, srcValue) -> {
              final BonsaiAccount copyPrior =
                  srcValue.getPrior() != null
                      ? copyAccount(srcValue.getPrior(), this, false)
                      : null;
              final BonsaiAccount copyUpdated =
                  priorOnly
                      ? (srcValue.getPrior() != null
                          ? copyAccount(srcValue.getPrior(), this, true)
                          : null)
                      : (srcValue.getUpdated() != null
                          ? copyAccount(srcValue.getUpdated(), this, true)
                          : null);
              final BonsaiValue<BonsaiAccount> newValue =
                  priorOnly
                      ? new BonsaiValue<>(copyPrior, copyUpdated)
                      : new BonsaiValue<>(copyPrior, copyUpdated, srcValue.isLastStepCleared());
              if (priorOnly) {
                accountsToUpdate.putIfAbsent(address, newValue);
              } else {
                accountsToUpdate.put(address, newValue);
              }
            });

    source
        .getCodeToUpdate()
        .forEach(
            (address, srcValue) -> {
              final Bytes prior = srcValue.getPrior();
              final Bytes updated = priorOnly ? prior : srcValue.getUpdated();
              final BonsaiValue<Bytes> newValue =
                  priorOnly
                      ? new BonsaiValue<>(prior, updated)
                      : new BonsaiValue<>(prior, updated, srcValue.isLastStepCleared());
              if (priorOnly) {
                codeToUpdate.putIfAbsent(address, newValue);
              } else {
                codeToUpdate.put(address, newValue);
              }
            });

    source
        .getStorageToUpdate()
        .forEach(
            (address, slots) -> {
              final StorageConsumingMap<StorageSlotKey, BonsaiValue<UInt256>> targetSlots =
                  storageToUpdate.computeIfAbsent(
                      address,
                      k ->
                          new StorageConsumingMap<>(
                              address, new ConcurrentHashMap<>(), storagePreloader));
              slots.forEach(
                  (slotKey, srcSlot) -> {
                    final UInt256 slotPrior = srcSlot.getPrior();
                    final UInt256 slotUpdated = priorOnly ? slotPrior : srcSlot.getUpdated();
                    final BonsaiValue<UInt256> newSlotValue =
                        priorOnly
                            ? new BonsaiValue<>(slotPrior, slotUpdated)
                            : new BonsaiValue<>(
                                slotPrior, slotUpdated, srcSlot.isLastStepCleared());
                    if (priorOnly) {
                      targetSlots.putIfAbsent(slotKey, newSlotValue);
                    } else {
                      targetSlots.put(slotKey, newSlotValue);
                    }
                  });
            });

    storageKeyHashLookup.putAll(source.storageKeyHashLookup);
    this.isAccumulatorStateChanged = true;
  }

  protected Consumer<BonsaiValue<BonsaiAccount>> getAccountPreloader() {
    return accountPreloader;
  }

  public Consumer<StorageSlotKey> getStoragePreloader() {
    return storagePreloader;
  }

  public EvmConfiguration getEvmConfiguration() {
    return evmConfiguration;
  }

  @Override
  public Account get(final Address address) {
    return super.get(address);
  }

  @Override
  protected UpdateTrackingAccount<BonsaiAccount> track(
      final UpdateTrackingAccount<BonsaiAccount> account) {
    return super.track(account);
  }

  @Override
  public MutableAccount getAccount(final Address address) {
    return super.getAccount(address);
  }

  @Override
  public MutableAccount createAccount(final Address address, final long nonce, final Wei balance) {
    BonsaiValue<BonsaiAccount> pathBasedValue = accountsToUpdate.get(address);

    if (pathBasedValue == null) {
      pathBasedValue = new BonsaiValue<>(null, null);
      accountsToUpdate.put(address, pathBasedValue);
    } else if (pathBasedValue.getUpdated() != null) {
      if (pathBasedValue.getUpdated().isEmpty()) {
        return track(new UpdateTrackingAccount<>(pathBasedValue.getUpdated()));
      } else {
        throw new IllegalStateException("Cannot create an account when one already exists");
      }
    }

    final BonsaiAccount newAccount =
        createAccount(
            this,
            address,
            hashAndSaveAccountPreImage(address),
            nonce,
            balance,
            storageRootStrategy(),
            Hash.EMPTY,
            true);
    pathBasedValue.setUpdated(newAccount);
    return track(new UpdateTrackingAccount<>(newAccount));
  }

  /**
   * Returns the {@link StorageRootStrategy} for the active storage format, used when constructing
   * new {@link BonsaiAccount} instances. MPT → a fresh {@link MptStorageRootStrategy} holding
   * {@link Hash#EMPTY_TRIE_HASH} (the initial root for a new account); BINARY → the stateless
   * {@link BinaryStorageRootStrategy} singleton. This is the single place that maps the world
   * state's {@link DataStorageFormat} onto a strategy — {@code BonsaiAccount} itself is
   * format-agnostic.
   */
  protected StorageRootStrategy storageRootStrategy() {
    return StorageRootStrategy.forFormat(
        wrappedWorldView().getWorldStateStorage().getDataStorageFormat());
  }

  @Override
  public Map<Address, BonsaiValue<BonsaiAccount>> getAccountsToUpdate() {
    return accountsToUpdate;
  }

  @Override
  public Map<Address, BonsaiValue<Bytes>> getCodeToUpdate() {
    return codeToUpdate;
  }

  public Set<Address> getStorageToClear() {
    return storageToClear;
  }

  @Override
  public Map<Address, StorageConsumingMap<StorageSlotKey, BonsaiValue<UInt256>>>
      getStorageToUpdate() {
    return storageToUpdate;
  }

  @Override
  protected BonsaiAccount getForMutation(final Address address) {
    return loadAccount(address, BonsaiValue::getUpdated);
  }

  protected Optional<BonsaiAccount> loadAccountFromParentAccumulator(
      final Address address,
      final Function<BonsaiValue<BonsaiAccount>, BonsaiAccount> accountFunction) {
    if (wrappedWorldView() instanceof BonsaiWorldStateUpdateAccumulator parentAccumulator) {
      return Optional.of(parentAccumulator.loadAccount(address, accountFunction));
    }
    return Optional.empty();
  }

  protected void onAccountValueLoaded(
      final Address address, final BonsaiValue<BonsaiAccount> accountValue) {}

  protected void onCodeValueLoaded(final Address address, final BonsaiValue<Bytes> codeValue) {}

  protected void onStorageValueLoaded(
      final Address address,
      final StorageSlotKey storageSlotKey,
      final BonsaiValue<UInt256> storageValue) {}

  public BonsaiAccount loadAccount(
      final Address address,
      final Function<BonsaiValue<BonsaiAccount>, BonsaiAccount> accountFunction) {
    try {
      final BonsaiValue<BonsaiAccount> pathBasedValue = accountsToUpdate.get(address);
      if (pathBasedValue == null) {
        final Optional<BonsaiAccount> fromParent =
            loadAccountFromParentAccumulator(address, accountFunction);
        if (fromParent.isPresent()) {
          return fromParent.get();
        }
        final Account account = wrappedWorldView().get(address);
        if (account instanceof BonsaiAccount pathBasedAccount) {
          final BonsaiAccount updatedAccount = copyAccount(pathBasedAccount, this, true);
          final BonsaiValue<BonsaiAccount> accountValue =
              new BonsaiValue<>(pathBasedAccount, updatedAccount);
          onAccountValueLoaded(address, accountValue);
          accountsToUpdate.put(address, accountValue);
          return accountFunction.apply(accountValue);
        }
        final BonsaiValue<BonsaiAccount> accountValue = new BonsaiValue<>(null, null);
        onAccountValueLoaded(address, accountValue);
        if (accountValue.getUpdated() == null) {
          accountsToUpdate.put(address, new BonsaiValue<>(null, null));
          return null;
        }
        accountsToUpdate.put(address, accountValue);
        return accountFunction.apply(accountValue);
      }
      return accountFunction.apply(pathBasedValue);
    } catch (MerkleTrieException e) {
      // need to throw to trigger the heal
      throw new MerkleTrieException(
          e.getMessage(), Optional.of(address), e.getHash(), e.getLocation());
    }
  }

  @Override
  public Collection<? extends Account> getTouchedAccounts() {
    return getUpdatedAccounts();
  }

  @Override
  public Collection<Address> getDeletedAccountAddresses() {
    return getDeletedAccounts();
  }

  @Override
  public void commit() {
    this.isAccumulatorStateChanged = true;

    for (final Address deletedAddress : getDeletedAccounts()) {
      final BonsaiValue<BonsaiAccount> accountValue =
          accountsToUpdate.computeIfAbsent(
              deletedAddress,
              __ -> loadAccountFromParent(deletedAddress, new BonsaiValue<>(null, null, true)));
      storageToClear.add(deletedAddress);
      final BonsaiValue<Bytes> codeValue = codeToUpdate.get(deletedAddress);
      if (codeValue != null) {
        codeValue.setUpdated(null).setCleared();
      } else {
        wrappedWorldView()
            .getCode(
                deletedAddress,
                Optional.ofNullable(accountValue)
                    .map(BonsaiValue::getPrior)
                    .map(BonsaiAccount::getCodeHash)
                    .orElse(Hash.EMPTY))
            .ifPresent(
                deletedCode ->
                    codeToUpdate.put(deletedAddress, new BonsaiValue<>(deletedCode, null, true)));
      }

      // mark all updated storage as to be cleared
      final Map<StorageSlotKey, BonsaiValue<UInt256>> deletedStorageUpdates =
          storageToUpdate.computeIfAbsent(
              deletedAddress,
              k ->
                  new StorageConsumingMap<>(
                      deletedAddress, new ConcurrentHashMap<>(), storagePreloader));
      final Iterator<Map.Entry<StorageSlotKey, BonsaiValue<UInt256>>> iter =
          deletedStorageUpdates.entrySet().iterator();
      while (iter.hasNext()) {
        final Map.Entry<StorageSlotKey, BonsaiValue<UInt256>> updateEntry = iter.next();
        final BonsaiValue<UInt256> updatedSlot = updateEntry.getValue();
        if (updatedSlot.getPrior() == null || updatedSlot.getPrior().isZero()) {
          iter.remove();
        } else {
          updatedSlot.setUpdated(null).setCleared();
        }
      }

      final BonsaiAccount originalValue = accountValue.getPrior();
      if (originalValue != null) {
        // Enumerate and delete addresses not updated. This deletion enumeration is MPT-only:
        // binary-trie accounts carry no per-account MPT storage trie (their storage lives in the
        // flat DB), and the binaryTrie fork removes SELFDESTRUCT, so binary accounts never reach
        // this deletion path. Guard on hasStorageRoot() so MptTrieFactory is not invoked for
        // binary accounts (which would have no storage root to enumerate).
        if (originalValue.hasStorageRoot()) {
          final Hash storageRoot = originalValue.getStorageRoot();
          MptTrieFactory.getAllAccountStorage(
                  (BonsaiWorldState) wrappedWorldView(), deletedAddress, storageRoot)
              .forEach(
                  (keyHash, entryValue) -> {
                    final StorageSlotKey storageSlotKey =
                        new StorageSlotKey(Hash.wrap(keyHash), Optional.empty());
                    if (!deletedStorageUpdates.containsKey(storageSlotKey)) {
                      final UInt256 value = UInt256.fromBytes(RLP.decodeOne(entryValue));
                      deletedStorageUpdates.put(
                          storageSlotKey, new BonsaiValue<>(value, null, true));
                    }
                  });
        }
      }
      if (deletedStorageUpdates.isEmpty()) {
        storageToUpdate.remove(deletedAddress);
      }
      accountValue.setUpdated(null);
    }

    getUpdatedAccounts().parallelStream()
        .forEach(
            tracked -> {
              final Address updatedAddress = tracked.getAddress();
              final BonsaiAccount updatedAccount;
              final BonsaiValue<BonsaiAccount> updatedAccountValue =
                  accountsToUpdate.get(updatedAddress);
              final Map<StorageSlotKey, BonsaiValue<UInt256>> pendingStorageUpdates =
                  storageToUpdate.computeIfAbsent(
                      updatedAddress,
                      k ->
                          new StorageConsumingMap<>(
                              updatedAddress, new ConcurrentHashMap<>(), storagePreloader));

              if (tracked.getWrappedAccount() == null) {
                updatedAccount = createAccount(this, tracked);
                tracked.setWrappedAccount(updatedAccount);
                if (updatedAccountValue == null) {
                  accountsToUpdate.put(updatedAddress, new BonsaiValue<>(null, updatedAccount));
                  codeToUpdate.put(
                      updatedAddress, new BonsaiValue<>(null, updatedAccount.getCode()));
                } else {
                  updatedAccountValue.setUpdated(updatedAccount);
                }
              } else {
                updatedAccount = tracked.getWrappedAccount();
                updatedAccount.setBalance(tracked.getBalance());
                updatedAccount.setNonce(tracked.getNonce());
                if (tracked.codeWasUpdated()) {
                  updatedAccount.setCode(tracked.getCode());
                }
                if (tracked.getStorageWasCleared()) {
                  updatedAccount.clearStorage();
                }
                tracked.getUpdatedStorage().forEach(updatedAccount::setStorageValue);
              }

              if (tracked.codeWasUpdated()) {
                final BonsaiValue<Bytes> pendingCode =
                    codeToUpdate.computeIfAbsent(
                        updatedAddress,
                        addr ->
                            new BonsaiValue<>(
                                wrappedWorldView()
                                    .getCode(
                                        addr,
                                        Optional.ofNullable(updatedAccountValue)
                                            .map(BonsaiValue::getPrior)
                                            .map(BonsaiAccount::getCodeHash)
                                            .orElse(Hash.EMPTY))
                                    .orElse(null),
                                null));
                pendingCode.setUpdated(updatedAccount.getCode());
              }

              if (tracked.getStorageWasCleared()) {
                storageToClear.add(updatedAddress);
                pendingStorageUpdates.clear();
              }

              // parallel stream here may cause database corruption
              updatedAccount
                  .getUpdatedStorage()
                  .entrySet()
                  .forEach(
                      storageUpdate -> {
                        final UInt256 keyUInt = storageUpdate.getKey();
                        final StorageSlotKey slotKey =
                            new StorageSlotKey(
                                hashAndSaveSlotPreImage(keyUInt), Optional.of(keyUInt));
                        final UInt256 value = storageUpdate.getValue();
                        final BonsaiValue<UInt256> pendingValue =
                            pendingStorageUpdates.get(slotKey);
                        if (pendingValue == null) {
                          pendingStorageUpdates.put(
                              slotKey,
                              new BonsaiValue<>(
                                  updatedAccount.getOriginalStorageValue(keyUInt), value));
                        } else {
                          pendingValue.setUpdated(value);
                        }
                      });

              updatedAccount.getUpdatedStorage().clear();

              if (pendingStorageUpdates.isEmpty()) {
                storageToUpdate.remove(updatedAddress);
              }

              if (tracked.getStorageWasCleared()) {
                tracked.setStorageWasCleared(false); // storage already cleared for this transaction
              }
            });
  }

  @Override
  public Optional<Bytes> getCode(final Address address, final Hash codeHash) {
    final BonsaiValue<Bytes> localCode = codeToUpdate.get(address);
    if (localCode == null) {
      final Supplier<Bytes> loader =
          Suppliers.memoize(() -> wrappedWorldView().getCode(address, codeHash).orElse(null));
      final BonsaiValue<Bytes> codeValue = BonsaiValue.withLazy(loader, loader);
      onCodeValueLoaded(address, codeValue);
      codeToUpdate.put(address, codeValue);
      return Optional.ofNullable(codeValue.getUpdated());
    } else {
      return Optional.ofNullable(localCode.getUpdated());
    }
  }

  @Override
  public UInt256 getStorageValue(final Address address, final UInt256 slotKey) {
    StorageSlotKey storageSlotKey =
        new StorageSlotKey(hashAndSaveSlotPreImage(slotKey), Optional.of(slotKey));
    return getStorageValueByStorageSlotKey(address, storageSlotKey).orElse(UInt256.ZERO);
  }

  @Override
  public Optional<UInt256> getStorageValueByStorageSlotKey(
      final Address address, final StorageSlotKey storageSlotKey) {
    final Map<StorageSlotKey, BonsaiValue<UInt256>> localAccountStorage =
        storageToUpdate.get(address);
    if (localAccountStorage != null) {
      final BonsaiValue<UInt256> value = localAccountStorage.get(storageSlotKey);
      if (value != null) {
        return Optional.ofNullable(value.getUpdated());
      }
    }
    try {
      final Supplier<UInt256> loader =
          Suppliers.memoize(
              () ->
                  (wrappedWorldView() instanceof BonsaiWorldState worldState)
                      ? worldState
                          .getStorageValueByStorageSlotKey(address, storageSlotKey)
                          .orElse(null)
                      : wrappedWorldView()
                          .getStorageValueByStorageSlotKey(address, storageSlotKey)
                          .orElse(null));
      final BonsaiValue<UInt256> storageValue = BonsaiValue.withLazy(loader, loader);
      onStorageValueLoaded(address, storageSlotKey, storageValue);

      storageToUpdate
          .computeIfAbsent(
              address,
              key ->
                  new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), storagePreloader))
          .put(storageSlotKey, storageValue);

      return Optional.ofNullable(storageValue.getUpdated());
    } catch (MerkleTrieException e) {
      // need to throw to trigger the heal
      throw new MerkleTrieException(
          e.getMessage(), Optional.of(address), e.getHash(), e.getLocation());
    }
  }

  @Override
  public UInt256 getPriorStorageValue(final Address address, final UInt256 storageKey) {
    // TODO maybe log the read into the trie layer?
    StorageSlotKey storageSlotKey =
        new StorageSlotKey(hashAndSaveSlotPreImage(storageKey), Optional.of(storageKey));
    final Map<StorageSlotKey, BonsaiValue<UInt256>> localAccountStorage =
        storageToUpdate.get(address);
    if (localAccountStorage != null) {
      final BonsaiValue<UInt256> value = localAccountStorage.get(storageSlotKey);
      if (value != null) {
        if (value.isLastStepCleared()) {
          return UInt256.ZERO;
        }
        final UInt256 updated = value.getUpdated();
        if (updated != null) {
          return updated;
        }
        final UInt256 original = value.getPrior();
        if (original != null) {
          return original;
        }
      }
    }
    if (storageToClear.contains(address)) {
      return UInt256.ZERO;
    }
    return getStorageValue(address, storageKey);
  }

  /**
   * Marks the boundary of a transaction by clearing tracking collections.
   *
   * <p>These tracking collections store changes made during the transaction. After committing the
   * transaction, they become unnecessary and can be safely cleared.
   *
   * <p>Note: If the transaction is not committed before this method is called, any uncommitted
   * changes will be lost.
   */
  @Override
  public void markTransactionBoundary() {
    getUpdatedAccounts().clear();
    getDeletedAccounts().clear();
  }

  @Override
  public boolean isModifyingHeadWorldState() {
    return true;
  }

  @Override
  public BonsaiWorldStateKeyValueStorage getWorldStateStorage() {
    return wrappedWorldView().getWorldStateStorage();
  }

  public void rollForward(final TrieLog layer) {
    layer
        .getAccountChanges()
        .forEach(
            (address, change) ->
                rollAccountChange(address, change.getPrior(), change.getUpdated()));
    layer
        .getCodeChanges()
        .forEach(
            (address, change) -> rollCodeChange(address, change.getPrior(), change.getUpdated()));
    layer
        .getStorageChanges()
        .forEach(
            (address, storage) ->
                storage.forEach(
                    (storageSlotKey, value) ->
                        rollStorageChange(
                            address, storageSlotKey, value.getPrior(), value.getUpdated())));
  }

  public void rollBack(final TrieLog layer) {
    layer
        .getAccountChanges()
        .forEach(
            (address, change) ->
                rollAccountChange(address, change.getUpdated(), change.getPrior()));
    layer
        .getCodeChanges()
        .forEach(
            (address, change) -> rollCodeChange(address, change.getUpdated(), change.getPrior()));
    layer
        .getStorageChanges()
        .forEach(
            (address, storage) ->
                storage.forEach(
                    (storageSlotKey, value) ->
                        rollStorageChange(
                            address, storageSlotKey, value.getUpdated(), value.getPrior())));
    introducedCodeHashes.addAll(layer.getIntroducedCodeHashes());
  }

  /**
   * Returns the mutable set of code hashes newly introduced by the block currently flowing through
   * the accumulator. The binary committer records into this set during the forward commit (flat-DB
   * presence check) and reads it during rollback to drop CODE_ZONE chunks; {@code
   * PbtTrieLogFactory.create} copies it into the trie-log layer for persistence.
   *
   * @return the live, mutable set.
   */
  public Set<Hash> getIntroducedCodeHashes() {
    return introducedCodeHashes;
  }

  private void rollAccountChange(
      final Address address,
      final AccountValue expectedValue,
      final AccountValue replacementValue) {
    if (Objects.equals(expectedValue, replacementValue)) {
      // non-change, a cached read.
      return;
    }
    BonsaiValue<BonsaiAccount> accountValue = accountsToUpdate.get(address);
    if (accountValue == null) {
      accountValue = loadAccountFromParent(address, accountValue);
    }
    if (accountValue == null) {
      if (expectedValue == null && replacementValue != null) {
        accountsToUpdate.put(
            address, new BonsaiValue<>(null, createAccount(this, address, replacementValue, true)));
      } else {
        throw new IllegalStateException(
            String.format(
                "Expected to update account, but the account does not exist. Address=%s", address));
      }
    } else {
      if (expectedValue == null) {
        if (accountValue.getUpdated() != null) {
          throw new IllegalStateException(
              String.format(
                  "Expected to create account, but the account exists.  Address=%s", address));
        }
      } else {
        assertCloseEnoughForDiffing(
            accountValue.getUpdated(),
            expectedValue,
            "Address=" + address + " Prior Value in Rolling Change");
      }
      if (replacementValue == null) {
        if (accountValue.getPrior() == null) {
          // TODO: should we remove from the parent accumulated change also?  only if it is a
          // private copy
          accountsToUpdate.remove(address);
        } else {
          accountValue.setUpdated(null);
        }
      } else {
        accountValue.setUpdated(createAccount(wrappedWorldView(), address, replacementValue, true));
      }
    }
  }

  private BonsaiValue<BonsaiAccount> loadAccountFromParent(
      final Address address, final BonsaiValue<BonsaiAccount> defaultValue) {
    try {
      final Account parentAccount = wrappedWorldView().get(address);
      if (parentAccount instanceof BonsaiAccount account) {
        final BonsaiValue<BonsaiAccount> loadedAccountValue =
            new BonsaiValue<>(copyAccount(account), account);
        accountsToUpdate.put(address, loadedAccountValue);
        return loadedAccountValue;
      } else {
        return defaultValue;
      }
    } catch (MerkleTrieException e) {
      // need to throw to trigger the heal
      throw new MerkleTrieException(
          e.getMessage(), Optional.of(address), e.getHash(), e.getLocation());
    }
  }

  private void rollCodeChange(
      final Address address, final Bytes expectedCode, final Bytes replacementCode) {
    if (Objects.equals(expectedCode, replacementCode)) {
      // non-change, a cached read.
      return;
    }
    BonsaiValue<Bytes> codeValue = codeToUpdate.get(address);
    if (codeValue == null) {
      final Bytes storedCode =
          wrappedWorldView()
              .getCode(
                  address, Optional.ofNullable(expectedCode).map(Hash::hash).orElse(Hash.EMPTY))
              .orElse(Bytes.EMPTY);
      if (!storedCode.isEmpty()) {
        codeValue = new BonsaiValue<>(storedCode, storedCode);
        codeToUpdate.put(address, codeValue);
      }
    }

    if (codeValue == null) {
      if ((expectedCode == null || expectedCode.isEmpty()) && replacementCode != null) {
        codeToUpdate.put(address, new BonsaiValue<>(null, replacementCode));
      } else {
        throw new IllegalStateException(
            String.format(
                "Expected to update code, but the code does not exist.  Address=%s", address));
      }
    } else {
      final Bytes existingCode = codeValue.getUpdated();
      if ((expectedCode == null || expectedCode.isEmpty())
          && existingCode != null
          && !existingCode.isEmpty()) {
        LOG.warn("At Address={}, expected to create code, but code exists. Overwriting.", address);
      } else if (!Objects.equals(expectedCode, existingCode)) {
        throw new IllegalStateException(
            String.format(
                "Old value of code does not match expected value.  Address=%s ExpectedHash=%s ActualHash=%s",
                address,
                expectedCode == null ? "null" : Hash.hash(expectedCode),
                Hash.hash(codeValue.getUpdated())));
      }
      if (replacementCode == null && codeValue.getPrior() == null) {
        codeToUpdate.remove(address);
      } else {
        codeValue.setUpdated(replacementCode);
      }
    }
  }

  private Map<StorageSlotKey, BonsaiValue<UInt256>> maybeCreateStorageMap(
      final Map<StorageSlotKey, BonsaiValue<UInt256>> storageMap, final Address address) {
    if (storageMap == null) {
      final StorageConsumingMap<StorageSlotKey, BonsaiValue<UInt256>> newMap =
          new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), storagePreloader);
      storageToUpdate.put(address, newMap);
      return newMap;
    } else {
      return storageMap;
    }
  }

  private void rollStorageChange(
      final Address address,
      final StorageSlotKey storageSlotKey,
      final UInt256 expectedValue,
      final UInt256 replacementValue) {
    if (Objects.equals(expectedValue, replacementValue)) {
      // non-change, a cached read.
      return;
    }
    if (replacementValue == null && expectedValue != null && expectedValue.isZero()) {
      // corner case on deletes, non-change
      return;
    }
    final Map<StorageSlotKey, BonsaiValue<UInt256>> storageMap = storageToUpdate.get(address);
    BonsaiValue<UInt256> slotValue = storageMap == null ? null : storageMap.get(storageSlotKey);
    if (slotValue == null) {
      final Optional<UInt256> storageValue =
          wrappedWorldView().getStorageValueByStorageSlotKey(address, storageSlotKey);
      if (storageValue.isPresent()) {
        slotValue = new BonsaiValue<>(storageValue.get(), storageValue.get());
        storageToUpdate
            .computeIfAbsent(
                address,
                k ->
                    new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), storagePreloader))
            .put(storageSlotKey, slotValue);
      }
    }
    if (slotValue == null) {
      if ((expectedValue == null || expectedValue.isZero()) && replacementValue != null) {
        maybeCreateStorageMap(storageMap, address)
            .put(storageSlotKey, new BonsaiValue<>(null, replacementValue));
      } else {
        throw new IllegalStateException(
            String.format(
                "Expected to update storage value, but the slot does not exist. Account=%s SlotKey=%s",
                address, storageSlotKey));
      }
    } else {
      final UInt256 existingSlotValue = slotValue.getUpdated();
      if ((expectedValue == null || expectedValue.isZero())
          && existingSlotValue != null
          && !existingSlotValue.isZero()) {
        throw new IllegalStateException(
            String.format(
                "Expected to create slot, but the slot exists. Account=%s SlotKey=%s expectedValue=%s existingValue=%s",
                address, storageSlotKey, expectedValue, existingSlotValue));
      }
      if (!isSlotEquals(expectedValue, existingSlotValue)) {
        throw new IllegalStateException(
            String.format(
                "Old value of slot does not match expected value. Account=%s SlotKey=%s Expected=%s Actual=%s",
                address,
                storageSlotKey,
                expectedValue == null ? "null" : expectedValue.toShortHexString(),
                existingSlotValue == null ? "null" : existingSlotValue.toShortHexString()));
      }
      if (replacementValue == null && slotValue.getPrior() == null) {
        final Map<StorageSlotKey, BonsaiValue<UInt256>> thisStorageUpdate =
            maybeCreateStorageMap(storageMap, address);
        thisStorageUpdate.remove(storageSlotKey);
        if (thisStorageUpdate.isEmpty()) {
          storageToUpdate.remove(address);
        }
      } else {
        slotValue.setUpdated(replacementValue);
      }
    }
  }

  private boolean isSlotEquals(final UInt256 expectedValue, final UInt256 existingSlotValue) {
    final UInt256 sanitizedExpectedValue = (expectedValue == null) ? UInt256.ZERO : expectedValue;
    final UInt256 sanitizedExistingSlotValue =
        (existingSlotValue == null) ? UInt256.ZERO : existingSlotValue;
    return Objects.equals(sanitizedExpectedValue, sanitizedExistingSlotValue);
  }

  public boolean isAccumulatorStateChanged() {
    return isAccumulatorStateChanged;
  }

  public void resetAccumulatorStateChanged() {
    isAccumulatorStateChanged = false;
  }

  /**
   * Reverts all changes that have not yet been committed.
   *
   * <p>This method calls the `reset` method of the superclass, which cancels all changes that have
   * not yet been committed. This effectively reverts the state to the last committed state.
   */
  @Override
  public void revert() {
    super.reset();
  }

  /**
   * Resets the accumulator by clearing all changes, including those that have been committed.
   *
   * <p>This method clears all internal maps and data structures that track changes. This includes
   * clearing the storage to clear, storage to update, code to update, accounts to update, and other
   * related data structures. This effectively removes all changes, even those that have been
   * committed in the accumulator.
   */
  @Override
  public void reset() {
    storageToClear.clear();
    storageToUpdate.clear();
    codeToUpdate.clear();
    accountsToUpdate.clear();
    introducedCodeHashes.clear();
    resetAccumulatorStateChanged();
    updatedAccounts.clear();
    deletedAccounts.clear();
    storageKeyHashLookup.clear();
  }

  protected Hash hashAndSaveAccountPreImage(final Address address) {
    // no need to save account preimage by default
    return Hash.hash(address.getBytes());
  }

  protected Hash hashAndSaveSlotPreImage(final UInt256 slotKey) {
    Hash hash = storageKeyHashLookup.get(slotKey);
    if (hash == null) {
      hash = Hash.hash(slotKey);
      storageKeyHashLookup.put(slotKey, hash);
    }
    return hash;
  }

  public BonsaiWorldStateUpdateAccumulator copy() {
    final BonsaiWorldStateUpdateAccumulator copy =
        new BonsaiWorldStateUpdateAccumulator(
            wrappedWorldView(),
            getAccountPreloader(),
            getStoragePreloader(),
            getEvmConfiguration(),
            codeCache);
    copy.cloneFromUpdater(this);
    return copy;
  }

  protected BonsaiAccount copyAccount(final BonsaiAccount account) {
    return new BonsaiAccount(account);
  }

  protected BonsaiAccount copyAccount(
      final BonsaiAccount toCopy, final BonsaiWorldView context, final boolean mutable) {
    return new BonsaiAccount(toCopy, context, mutable);
  }

  protected BonsaiAccount createAccount(
      final BonsaiWorldView context,
      final Address address,
      final AccountValue stateTrieAccount,
      final boolean mutable) {
    // Seed the MPT strategy with the AccountValue's root; for binary the strategy is stateless.
    final StorageRootStrategy strategy = storageRootStrategy();
    if (strategy.hasStorageRoot()) {
      strategy.setStorageRoot(
          stateTrieAccount instanceof MptAccountValue mpt
              ? mpt.getStorageRoot()
              : Hash.EMPTY_TRIE_HASH);
    }
    return new BonsaiAccount(
        context,
        address,
        address.addressHash(),
        stateTrieAccount.getNonce(),
        stateTrieAccount.getBalance(),
        strategy,
        stateTrieAccount.getCodeHash(),
        mutable,
        codeCache);
  }

  protected BonsaiAccount createAccount(
      final BonsaiWorldView context,
      final Address address,
      final Hash addressHash,
      final long nonce,
      final Wei balance,
      final StorageRootStrategy storageRootStrategy,
      final Hash codeHash,
      final boolean mutable) {
    return new BonsaiAccount(
        context,
        address,
        addressHash,
        nonce,
        balance,
        storageRootStrategy,
        codeHash,
        mutable,
        codeCache);
  }

  protected BonsaiAccount createAccount(
      final BonsaiWorldView context, final UpdateTrackingAccount<BonsaiAccount> tracked) {
    return new BonsaiAccount(context, tracked, storageRootStrategy(), codeCache);
  }

  protected void assertCloseEnoughForDiffing(
      final BonsaiAccount source, final AccountValue account, final String context) {
    BonsaiAccount.assertCloseEnoughForDiffing(source, account, context);
  }

  @Override
  public BonsaiCodeCache codeCache() {
    return codeCache;
  }
}
