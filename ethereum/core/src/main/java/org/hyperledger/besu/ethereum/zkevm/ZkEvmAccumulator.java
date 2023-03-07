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
 *
 */

package org.hyperledger.besu.ethereum.zkevm;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.bonsai.BonsaiPersistedWorldState;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.EvmAccount;
import org.hyperledger.besu.evm.worldstate.AbstractWorldUpdater;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;
import org.hyperledger.besu.evm.worldstate.WrappedEvmAccount;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

public class ZkEvmAccumulator extends AbstractWorldUpdater<ZkWorldView, ZkAccount>
    implements ZkWorldView {

  private final Map<Address, ZkValue<ZkAccount>> accountsToUpdate = new ConcurrentHashMap<>();
  private final Map<Address, ZkValue<Bytes>> codeToUpdate = new ConcurrentHashMap<>();
  private final Set<Address> storageToClear = Collections.synchronizedSet(new HashSet<>());

  private final Map<Address, Map<Hash, ZkValue<UInt256>>> storageToUpdate =
      new ConcurrentHashMap<>();

  private final Set<Bytes> readZeroAccount = Collections.synchronizedSet(new HashSet<>());
  private final Set<Bytes> readZeroSlot = Collections.synchronizedSet(new HashSet<>());

  ZkEvmAccumulator(final ZkWorldView world) {
    super(world);
  }

  public ZkEvmAccumulator copy() {
    final ZkEvmAccumulator copy = new ZkEvmAccumulator(wrappedWorldView());
    copy.cloneFromUpdater(this);
    return copy;
  }

  void cloneFromUpdater(final ZkEvmAccumulator source) {
    accountsToUpdate.putAll(source.getAccountsToUpdate());
    codeToUpdate.putAll(source.codeToUpdate);
    storageToClear.addAll(source.storageToClear);
    storageToUpdate.putAll(source.storageToUpdate);
    updatedAccounts.putAll(source.updatedAccounts);
    deletedAccounts.addAll(source.deletedAccounts);
    readZeroAccount.addAll(source.readZeroAccount);
    readZeroSlot.addAll(source.readZeroSlot);
  }

  @Override
  public Account get(final Address address) {
    return super.getAccount(address);
  }

  @Override
  protected UpdateTrackingAccount<ZkAccount> track(final UpdateTrackingAccount<ZkAccount> account) {
    return super.track(account);
  }

  @Override
  public EvmAccount getAccount(final Address address) {
    return super.getAccount(address);
  }

  @Override
  public EvmAccount createAccount(final Address address, final long nonce, final Wei balance) {
    ZkValue<ZkAccount> zkValue = accountsToUpdate.get(address);
    if (zkValue == null) {
      zkValue = new ZkValue<>(null, null);
      accountsToUpdate.put(address, zkValue);
    } else if (zkValue.getUpdated() != null) {
      throw new IllegalStateException("Cannot create an account when one already exists");
    }
    final ZkAccount newAccount =
        new ZkAccount(
            this,
            address,
            Hash.hash(address),
            nonce,
            balance,
            Hash.EMPTY_TRIE_HASH,
            Hash.EMPTY,
            Hash.EMPTY, // TODO put empty mimcHash,
            0,
            true);
    zkValue.setUpdated(newAccount);
    return new WrappedEvmAccount(track(new UpdateTrackingAccount<>(newAccount)));
  }

  Map<Address, ZkValue<ZkAccount>> getAccountsToUpdate() {
    return accountsToUpdate;
  }

  Map<Address, ZkValue<Bytes>> getCodeToUpdate() {
    return codeToUpdate;
  }

  public Set<Address> getStorageToClear() {
    return storageToClear;
  }

  Map<Address, Map<Hash, ZkValue<UInt256>>> getStorageToUpdate() {
    return storageToUpdate;
  }

  @Override
  protected ZkAccount getForMutation(final Address address) {
    return loadAccount(address, ZkValue::getUpdated);
  }

  protected ZkAccount loadAccount(
      final Address address, final Function<ZkValue<ZkAccount>, ZkAccount> bonsaiAccountFunction) {
    try {
      final ZkValue<ZkAccount> zkValue = accountsToUpdate.get(address);
      if (zkValue == null) {
        if (!readZeroAccount.contains(address)) {
          final Account account = wrappedWorldView().get(address);
          if (account instanceof ZkAccount) {
            final ZkAccount mutableAccount = new ZkAccount((ZkAccount) account, this, true);
            accountsToUpdate.put(address, new ZkValue<>((ZkAccount) account, mutableAccount));
            return mutableAccount;
          }
          readZeroAccount.add(address);
        }
        return null;
      } else {
        return bonsaiAccountFunction.apply(zkValue);
      }
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
  public void revert() {
    super.reset();
  }

  @Override
  public void commit() {
    for (final Address deletedAddress : getDeletedAccounts()) {
      final ZkValue<ZkAccount> accountValue =
          accountsToUpdate.computeIfAbsent(
              deletedAddress,
              __ -> loadAccountFromParent(deletedAddress, new ZkValue<>(null, null)));
      storageToClear.add(deletedAddress);
      final ZkValue<Bytes> codeValue = codeToUpdate.get(deletedAddress);
      if (codeValue != null) {
        codeValue.setUpdated(null);
      } else {
        wrappedWorldView()
            .getCode(
                deletedAddress,
                Optional.ofNullable(accountValue)
                    .map(ZkValue::getPrior)
                    .map(ZkAccount::getCodeHash)
                    .orElse(Hash.EMPTY))
            .ifPresent(
                deletedCode -> codeToUpdate.put(deletedAddress, new ZkValue<>(deletedCode, null)));
      }

      // mark all updated storage as to be cleared
      final Map<Hash, ZkValue<UInt256>> deletedStorageUpdates =
          storageToUpdate.computeIfAbsent(deletedAddress, k -> new ConcurrentHashMap<>());
      final Iterator<Map.Entry<Hash, ZkValue<UInt256>>> iter =
          deletedStorageUpdates.entrySet().iterator();
      while (iter.hasNext()) {
        final Map.Entry<Hash, ZkValue<UInt256>> updateEntry = iter.next();
        final ZkValue<UInt256> updatedSlot = updateEntry.getValue();
        if (updatedSlot.getPrior() == null || updatedSlot.getPrior().isZero()) {
          iter.remove();
        } else {
          updatedSlot.setUpdated(null);
        }
      }

      final ZkAccount originalValue = accountValue.getPrior();
      if (originalValue != null) {
        // Enumerate and delete addresses not updated
        wrappedWorldView()
            .getAllAccountStorage(deletedAddress, originalValue.getStorageRoot())
            .forEach(
                (keyHash, entryValue) -> {
                  final Hash slotHash = Hash.wrap(keyHash);
                  if (!deletedStorageUpdates.containsKey(slotHash)) {
                    final UInt256 value = UInt256.fromBytes(RLP.decodeOne(entryValue));
                    deletedStorageUpdates.put(slotHash, new ZkValue<>(value, null, true));
                  }
                });
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
              final ZkAccount updatedAccount;
              final ZkValue<ZkAccount> updatedAccountValue = accountsToUpdate.get(updatedAddress);
              if (tracked.getWrappedAccount() == null) {
                updatedAccount = new ZkAccount(this, tracked);
                tracked.setWrappedAccount(updatedAccount);
                if (updatedAccountValue == null) {
                  accountsToUpdate.put(updatedAddress, new ZkValue<>(null, updatedAccount));
                  codeToUpdate.put(updatedAddress, new ZkValue<>(null, updatedAccount.getCode()));
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
                final ZkValue<Bytes> pendingCode =
                    codeToUpdate.computeIfAbsent(
                        updatedAddress,
                        addr ->
                            new ZkValue<>(
                                wrappedWorldView()
                                    .getCode(
                                        addr,
                                        Optional.ofNullable(updatedAccountValue)
                                            .map(ZkValue::getPrior)
                                            .map(ZkAccount::getCodeHash)
                                            .orElse(Hash.EMPTY))
                                    .orElse(null),
                                null));
                pendingCode.setUpdated(updatedAccount.getCode());
              }
              // This is especially to avoid unnecessary computation for withdrawals
              if (updatedAccount.getUpdatedStorage().isEmpty()) return;
              final Map<Hash, ZkValue<UInt256>> pendingStorageUpdates =
                  storageToUpdate.computeIfAbsent(updatedAddress, __ -> new ConcurrentHashMap<>());
              if (tracked.getStorageWasCleared()) {
                storageToClear.add(updatedAddress);
                pendingStorageUpdates.clear();
              }

              final TreeSet<Map.Entry<UInt256, UInt256>> entries =
                  new TreeSet<>(Map.Entry.comparingByKey());
              entries.addAll(updatedAccount.getUpdatedStorage().entrySet());

              // parallel stream here may cause database corruption
              entries.forEach(
                  storageUpdate -> {
                    final UInt256 keyUInt = storageUpdate.getKey();
                    final Hash slotHash = Hash.hash(keyUInt);
                    final UInt256 value = storageUpdate.getValue();
                    final ZkValue<UInt256> pendingValue = pendingStorageUpdates.get(slotHash);
                    if (pendingValue == null) {
                      pendingStorageUpdates.put(
                          slotHash,
                          new ZkValue<>(updatedAccount.getOriginalStorageValue(keyUInt), value));
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
    final ZkValue<Bytes> localCode = codeToUpdate.get(address);
    if (localCode == null) {
      final Optional<Bytes> code = wrappedWorldView().getCode(address, codeHash);
      if (code.isEmpty() && !codeHash.equals(Hash.EMPTY)) {
        throw new MerkleTrieException(
            "invalid account code", Optional.of(address), codeHash, Bytes.EMPTY);
      }
      return code;
    } else {
      return Optional.ofNullable(localCode.getUpdated());
    }
  }

  @Override
  public UInt256 getStorageValue(final Address address, final UInt256 storageKey) {
    // TODO maybe log the read into the trie layer?
    final Hash slotHashBytes = Hash.hash(storageKey);
    return getStorageValueBySlotHash(address, slotHashBytes).orElse(UInt256.ZERO);
  }

  @Override
  public Optional<UInt256> getStorageValueBySlotHash(final Address address, final Hash slotHash) {
    final Map<Hash, ZkValue<UInt256>> localAccountStorage = storageToUpdate.get(address);
    if (localAccountStorage != null) {
      final ZkValue<UInt256> value = localAccountStorage.get(slotHash);
      if (value != null) {
        return Optional.ofNullable(value.getUpdated());
      }
    }
    final Bytes slot = Bytes.concatenate(Hash.hash(address), slotHash);
    if (readZeroSlot.contains(slot)) {
      return Optional.empty();
    } else {
      try {
        final Optional<UInt256> valueUInt =
            (wrappedWorldView() instanceof BonsaiPersistedWorldState)
                ? ((BonsaiPersistedWorldState) wrappedWorldView())
                    .getStorageValueBySlotHash(
                        () ->
                            Optional.ofNullable(loadAccount(address, ZkValue::getPrior))
                                .map(ZkAccount::getStorageRoot),
                        address,
                        slotHash)
                : wrappedWorldView().getStorageValueBySlotHash(address, slotHash);
        valueUInt.ifPresentOrElse(
            v ->
                storageToUpdate
                    .computeIfAbsent(address, key -> new ConcurrentHashMap<>())
                    .put(slotHash, new ZkValue<>(v, v)),
            () -> {
              readZeroSlot.add(Bytes.concatenate(Hash.hash(address), slotHash));
            });
        return valueUInt;
      } catch (MerkleTrieException e) {
        // need to throw to trigger the heal
        throw new MerkleTrieException(
            e.getMessage(), Optional.of(address), e.getHash(), e.getLocation());
      }
    }
  }

  @Override
  public UInt256 getPriorStorageValue(final Address address, final UInt256 storageKey) {
    // TODO maybe log the read into the trie layer?
    final Map<Hash, ZkValue<UInt256>> localAccountStorage = storageToUpdate.get(address);
    if (localAccountStorage != null) {
      final Hash slotHash = Hash.hash(storageKey);
      final ZkValue<UInt256> value = localAccountStorage.get(slotHash);
      if (value != null) {
        if (value.isCleared()) {
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

  @Override
  public Map<Bytes32, Bytes> getAllAccountStorage(final Address address, final Hash rootHash) {
    final Map<Bytes32, Bytes> results = wrappedWorldView().getAllAccountStorage(address, rootHash);
    storageToUpdate.get(address).forEach((key, value) -> results.put(key, value.getUpdated()));
    return results;
  }

  private ZkValue<ZkAccount> loadAccountFromParent(
      final Address address, final ZkValue<ZkAccount> defaultValue) {
    try {
      final Account parentAccount = wrappedWorldView().get(address);
      if (parentAccount instanceof ZkAccount) {
        final ZkAccount account = (ZkAccount) parentAccount;
        final ZkValue<ZkAccount> loadedAccountValue =
            new ZkValue<>(new ZkAccount(account), account);
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

  @Override
  public void reset() {
    storageToClear.clear();
    storageToUpdate.clear();
    codeToUpdate.clear();
    accountsToUpdate.clear();
    readZeroSlot.clear();
    super.reset();
  }

  public boolean isDirty() {
    return !(accountsToUpdate.isEmpty()
        && updatedAccounts.isEmpty()
        && deletedAccounts.isEmpty()
        && storageToUpdate.isEmpty()
        && storageToClear.isEmpty()
        && codeToUpdate.isEmpty());
  }
}
