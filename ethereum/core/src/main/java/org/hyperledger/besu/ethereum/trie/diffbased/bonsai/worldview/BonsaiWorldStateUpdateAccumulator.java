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
package org.hyperledger.besu.ethereum.trie.diffbased.bonsai.worldview;

import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.diffbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.diffbased.common.DiffBasedAccount;
import org.hyperledger.besu.ethereum.trie.diffbased.common.DiffBasedValue;
import org.hyperledger.besu.ethereum.trie.diffbased.common.worldview.DiffBasedWorldState;
import org.hyperledger.besu.ethereum.trie.diffbased.common.worldview.DiffBasedWorldView;
import org.hyperledger.besu.ethereum.trie.diffbased.common.worldview.accumulator.DiffBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.diffbased.common.worldview.accumulator.preload.Consumer;
import org.hyperledger.besu.ethereum.trie.diffbased.common.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class BonsaiWorldStateUpdateAccumulator
    extends DiffBasedWorldStateUpdateAccumulator<BonsaiAccount> {


  private final Map<Address, Map<StorageSlotKey, Optional<UInt256>>>
          storageCache = new ConcurrentHashMap<>();

  public BonsaiWorldStateUpdateAccumulator(
      final DiffBasedWorldView world,
      final Consumer<DiffBasedValue<BonsaiAccount>> accountPreloader,
      final Consumer<StorageSlotKey> storagePreloader,
      final EvmConfiguration evmConfiguration) {
    super(world, accountPreloader, storagePreloader, evmConfiguration);
  }

  @Override
  public DiffBasedWorldStateUpdateAccumulator<BonsaiAccount> copy() {
    final BonsaiWorldStateUpdateAccumulator copy =
        new BonsaiWorldStateUpdateAccumulator(
            wrappedWorldView(),
            getAccountPreloader(),
            getStoragePreloader(),
            getEvmConfiguration());
    copy.cloneFromUpdater(this);
    return copy;
  }

  @Override
  protected BonsaiAccount copyAccount(final BonsaiAccount account) {
    return new BonsaiAccount(account);
  }

  @Override
  protected BonsaiAccount copyAccount(
      final BonsaiAccount toCopy, final DiffBasedWorldView context, final boolean mutable) {
    return new BonsaiAccount(toCopy, context, mutable);
  }

  @Override
  protected BonsaiAccount createAccount(
      final DiffBasedWorldView context,
      final Address address,
      final AccountValue stateTrieAccount,
      final boolean mutable) {
    return new BonsaiAccount(context, address, stateTrieAccount, mutable);
  }

  @Override
  protected BonsaiAccount createAccount(
      final DiffBasedWorldView context,
      final Address address,
      final Hash addressHash,
      final long nonce,
      final Wei balance,
      final Hash storageRoot,
      final Hash codeHash,
      final boolean mutable) {
    return new BonsaiAccount(
        context, address, addressHash, nonce, balance, storageRoot, codeHash, mutable);
  }

  @Override
  protected BonsaiAccount createAccount(
      final DiffBasedWorldView context, final UpdateTrackingAccount<BonsaiAccount> tracked) {
    return new BonsaiAccount(context, tracked);
  }

  @Override
  protected void assertCloseEnoughForDiffing(
      final BonsaiAccount source, final AccountValue account, final String context) {
    BonsaiAccount.assertCloseEnoughForDiffing(source, account, context);
  }

  private void preloadAccount(final BonsaiAccount account){
    CompletableFuture.runAsync(() -> {
      if(!account.getStorageRoot().equals(Hash.EMPTY_TRIE_HASH)){
        for (int i = 0; i < 256; i++) {
          cacheStorageValueByStorageSlotKeyWithCache(account.getAddress(), new StorageSlotKey(UInt256.valueOf(i)));
        }
      }
    });
  }

  @Override
  public BonsaiAccount loadAccount(
          final Address address, final Function<DiffBasedValue<BonsaiAccount>, BonsaiAccount> accountFunction) {
    try {
      final DiffBasedValue<BonsaiAccount> diffBasedValue = accountsToUpdate.get(address);
      if (diffBasedValue == null) {
        final Account account;
        if (wrappedWorldView() instanceof DiffBasedWorldStateUpdateAccumulator) {
          final DiffBasedWorldStateUpdateAccumulator<BonsaiAccount> worldStateUpdateAccumulator =
                  (DiffBasedWorldStateUpdateAccumulator<BonsaiAccount>) wrappedWorldView();
          account = worldStateUpdateAccumulator.loadAccount(address, accountFunction);
        } else {
          account = wrappedWorldView().get(address);
        }
        if (account instanceof DiffBasedAccount diffBasedAccount) {
          BonsaiAccount mutableAccount = copyAccount((BonsaiAccount) diffBasedAccount, this, true);
          accountsToUpdate.put(
                  address, new DiffBasedValue<>((BonsaiAccount) diffBasedAccount, mutableAccount));
          preloadAccount(mutableAccount);
          return mutableAccount;
        } else {
          // add the empty read in accountsToUpdate
          accountsToUpdate.put(address, new DiffBasedValue<>(null, null));
          return null;
        }
      } else {
        return accountFunction.apply(diffBasedValue);
      }
    } catch (MerkleTrieException e) {
      // need to throw to trigger the heal
      throw new MerkleTrieException(
              e.getMessage(), Optional.of(address), e.getHash(), e.getLocation());
    }
  }

  @Override
  public Optional<UInt256> getStorageValueByStorageSlotKey(
          final Address address, final StorageSlotKey storageSlotKey) {
    final Map<StorageSlotKey, DiffBasedValue<UInt256>> localAccountStorage =
            storageToUpdate.get(address);
    if (localAccountStorage != null) {
      final DiffBasedValue<UInt256> value = localAccountStorage.get(storageSlotKey);
      if (value != null) {
        return Optional.ofNullable(value.getUpdated());
      }
    }
    try {
      final Optional<UInt256> valueUInt;
      if( (wrappedWorldView() instanceof DiffBasedWorldState worldState)) {
        Map<StorageSlotKey, Optional<UInt256>> storageCacheForAccount = storageCache.get(address);
        if (storageCacheForAccount != null) {
          Optional<UInt256> cachedSlot = storageCacheForAccount.get(storageSlotKey);
          if (cachedSlot != null) {
            valueUInt = cachedSlot;
          } else {
            valueUInt = worldState.getStorageValueByStorageSlotKey(address, storageSlotKey);
          }
        } else {
          valueUInt = worldState.getStorageValueByStorageSlotKey(address, storageSlotKey);
        }
      }else {
        valueUInt = wrappedWorldView().getStorageValueByStorageSlotKey(address, storageSlotKey);
      }
      storageToUpdate
              .computeIfAbsent(
                      address,
                      key ->
                              new StorageConsumingMap<>(address, new ConcurrentHashMap<>(), storagePreloader))
              .put(
                      storageSlotKey, new DiffBasedValue<>(valueUInt.orElse(null), valueUInt.orElse(null)));
      return valueUInt;
    } catch (MerkleTrieException e) {
      // need to throw to trigger the heal
      throw new MerkleTrieException(
              e.getMessage(), Optional.of(address), e.getHash(), e.getLocation());
    }
  }

  private void cacheStorageValueByStorageSlotKeyWithCache(
          final Address address, final StorageSlotKey storageSlotKey) {
    try {
      final Optional<UInt256> valueUInt =
              (wrappedWorldView() instanceof DiffBasedWorldState worldState)
                      ? worldState.getStorageValueByStorageSlotKey(address, storageSlotKey)
                      : wrappedWorldView().getStorageValueByStorageSlotKey(address, storageSlotKey);
      storageCache
              .computeIfAbsent(
                      address,
                      key ->
                              new ConcurrentHashMap<>())
              .put(
                      storageSlotKey, valueUInt);
    } catch (MerkleTrieException e) {
      // need to throw to trigger the heal
      throw new MerkleTrieException(
              e.getMessage(), Optional.of(address), e.getHash(), e.getLocation());
    }
  }
}
