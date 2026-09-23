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
package org.hyperledger.besu.evm.worldstate;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.ModificationNotAllowedException;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.AccountStorageEntry;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.CodeCache;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link MutableAccount} that tracks updates made to the account since the
 * creation of the updater this is linked to.
 *
 * <p>Note that in practice this only track the modified value of the nonce and balance, but doesn't
 * remind if those were modified or not (the reason being that any modification of an account imply
 * the underlying trie node will have to be updated, and so knowing if the nonce and balance where
 * updated or not doesn't matter, we just need their new value).
 *
 * @param <A> the type parameter
 */
public class UpdateTrackingAccount<A extends Account> implements MutableAccount {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateTrackingAccount.class);

  private final Address address;
  private final Hash addressHash;

  @Nullable private A account; // null if this is a new account.
  @Nullable private CodeCache codeCache;

  private boolean immutable;

  private long nonce;
  private Wei balance;

  @Nullable private Code updatedCode; // Null if the underlying code has not been updated.
  private final Code oldCode;
  @Nullable private Hash updatedCodeHash;
  private final Hash oldCodeHash;

  // Only contains updated storage entries, but may contain entry with a value of 0 to signify
  // deletion.
  private final NavigableMap<UInt256, UInt256> updatedStorage;
  private boolean storageWasCleared = false;
  private boolean transactionBoundary = false;

  /**
   * Instantiates a new Update tracking account.
   *
   * @param address the address
   */
  UpdateTrackingAccount(final Address address) {
    this(address, null);
  }

  /**
   * Instantiates a new Update tracking account for a newly created address, optionally wiring the
   * shared analyzed-code cache so jump-dest results are retained across txs.
   *
   * @param address the address
   * @param codeCache shared code cache, or {@code null}
   */
  UpdateTrackingAccount(final Address address, final CodeCache codeCache) {
    checkNotNull(address);
    this.address = address;
    this.addressHash = this.address.addressHash();
    this.account = null;
    this.codeCache = codeCache;

    this.nonce = 0;
    this.balance = Wei.ZERO;

    this.updatedCode = Code.EMPTY_CODE;
    this.oldCode = Code.EMPTY_CODE;
    this.oldCodeHash = Hash.EMPTY;
    this.updatedStorage = new TreeMap<>();
  }

  /**
   * Instantiates a new Update tracking account.
   *
   * @param account the account
   */
  public UpdateTrackingAccount(final A account) {
    checkNotNull(account);

    this.address = account.getAddress();
    this.addressHash =
        (account instanceof UpdateTrackingAccount)
            ? ((UpdateTrackingAccount<?>) account).addressHash
            : account.getAddressHash();
    this.account = account;

    this.nonce = account.getNonce();
    this.balance = account.getBalance();

    this.oldCode = account.getCode();
    this.oldCodeHash = account.getCodeHash();

    this.updatedStorage = new TreeMap<>();

    // Prefer the account's CodeCache when present (Bonsai exposes the KV analyzed-code cache).
    final CodeCache accountCodeCache = account.getCodeCache();
    if (accountCodeCache != null) {
      this.codeCache = accountCodeCache;
    }
  }

  /**
   * The original account over which this tracks updates.
   *
   * @return The original account over which this tracks updates, or {@code null} if this is a newly
   *     created account.
   */
  public A getWrappedAccount() {
    return account;
  }

  /**
   * Sets wrapped account.
   *
   * @param account the account
   */
  public void setWrappedAccount(final A account) {
    if (this.account == null) {
      this.account = account;
      storageWasCleared = false;
    } else {
      throw new IllegalStateException("Already tracking a wrapped account");
    }
  }

  /**
   * Whether the code of the account was modified.
   *
   * @return {@code true} if the code was updated.
   */
  public boolean codeWasUpdated() {
    return updatedCode != null;
  }

  @Override
  public CodeCache getCodeCache() {
    return codeCache;
  }

  /**
   * A map of the storage entries that were modified.
   *
   * @return a map containing all entries that have been modified. This <b>may</b> contain entries
   *     with a value of 0 to signify deletion.
   */
  @Override
  public Map<UInt256, UInt256> getUpdatedStorage() {
    return updatedStorage;
  }

  @Override
  public Address getAddress() {
    return address;
  }

  @Override
  public Hash getAddressHash() {
    return addressHash;
  }

  @Override
  public long getNonce() {
    return nonce;
  }

  @Override
  public void setNonce(final long value) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    this.nonce = value;
  }

  @Override
  public Wei getBalance() {
    return balance;
  }

  @Override
  public void setBalance(final Wei value) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    this.balance = value;
  }

  @Override
  public Code getCode() {
    // Note that we set code for new account, so it's only null if account isn't.
    return updatedCode == null ? oldCode : updatedCode;
  }

  @Override
  public Hash getCodeHash() {
    if (updatedCode == null) {
      // Note that we set code for new account, so it's only null if account isn't.
      return oldCodeHash;
    } else {
      // Cache the hash of updated code to avoid DOS attacks which repeatedly request hash
      // of updated code and cause us to regenerate it.
      if (updatedCodeHash == null) {
        updatedCodeHash = updatedCode.getCodeHash();
      }
      return updatedCodeHash;
    }
  }

  @Override
  public boolean hasCode() {
    // Note that we set code for new account, so it's only null if account isn't.
    return updatedCode == null ? oldCode.getSize() > 0 : updatedCode.getSize() > 0;
  }

  @Override
  public void setCode(final Code code) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    this.updatedCode = code == null ? Code.EMPTY_CODE : code;
    this.updatedCodeHash = null;
  }

  @Override
  public Code getOrCreateCachedCode() {
    if (codeCache == null && account != null) {
      codeCache = account.getCodeCache();
    }
    if (codeCache == null) {
      final Code code = getCode();
      final boolean analyzedBefore = code.getJumpDestBitMask() != null;
      final long t0 = System.nanoTime();
      code.ensureJumpDestAnalyzed();
      final long jumpDestNs = System.nanoTime() - t0;
      if (!analyzedBefore && code.getSize() > 0) {
        LOG.info(
            "Code jumpDest (tx, no CodeCache): thread={} jumpDestUs={} codeHash={}",
            Thread.currentThread().getName(),
            jumpDestNs / 1_000,
            getCodeHash());
      }
      return code;
    }

    // if the code already exists in the cache, return it
    final Code cachedCode = codeCache.getIfPresent(getCodeHash());
    if (cachedCode != null) {
      LOG.debug(
          "Code cache HIT (tx): thread={} codeHash={}",
          Thread.currentThread().getName(),
          getCodeHash());
      return cachedCode;
    }

    // if the code is not in the cache, put the current Code instance into the cache
    final Code newCode = getCode();
    final boolean analyzedBefore = newCode.getJumpDestBitMask() != null;
    final long t0 = System.nanoTime();
    newCode.ensureJumpDestAnalyzed();
    final long jumpDestNs = System.nanoTime() - t0;
    codeCache.put(getCodeHash(), newCode);
    LOG.info(
        "Code cache MISS (tx): thread={} jumpDestUs={} analyzedBeforePut={} codeHash={}",
        Thread.currentThread().getName(),
        jumpDestNs / 1_000,
        analyzedBefore,
        getCodeHash());

    return newCode;
  }

  /** Mark transaction boundary. */
  void markTransactionBoundary() {
    this.transactionBoundary = true;
  }

  @Override
  public UInt256 getStorageValue(final UInt256 key) {
    final UInt256 value = updatedStorage.get(key);
    if (value != null) {
      return value;
    }
    if (storageWasCleared) {
      return UInt256.ZERO;
    }

    // We haven't updated the key-value yet, so either it's a new account, and it doesn't have the
    // key, or we should query the underlying storage for its existing value (which might be 0).
    return account == null ? UInt256.ZERO : account.getStorageValue(key);
  }

  @Override
  public UInt256 getOriginalStorageValue(final UInt256 key) {
    if (transactionBoundary) {
      return getStorageValue(key);
    } else if (storageWasCleared || account == null) {
      return UInt256.ZERO;
    } else {
      return account.getOriginalStorageValue(key);
    }
  }

  @Override
  public NavigableMap<Bytes32, AccountStorageEntry> storageEntriesFrom(
      final Bytes32 startKeyHash, final int limit) {
    final NavigableMap<Bytes32, AccountStorageEntry> entries;
    if (account != null) {
      entries = account.storageEntriesFrom(startKeyHash, limit);
    } else {
      entries = new TreeMap<>();
    }
    updatedStorage.entrySet().stream()
        .map(entry -> AccountStorageEntry.forKeyAndValue(entry.getKey(), entry.getValue()))
        .filter(entry -> entry.getKeyHash().compareTo(startKeyHash) >= 0)
        .forEach(entry -> entries.put(entry.getKeyHash(), entry));

    while (entries.size() > limit) {
      entries.remove(entries.lastKey());
    }
    return entries;
  }

  @Override
  public void setStorageValue(final UInt256 key, final UInt256 value) {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    updatedStorage.put(key, value);
  }

  @Override
  public void clearStorage() {
    if (immutable) {
      throw new ModificationNotAllowedException();
    }
    storageWasCleared = true;
    updatedStorage.clear();
  }

  @Override
  public void becomeImmutable() {
    immutable = true;
  }

  /**
   * Gets storage was cleared.
   *
   * @return boolean if storage was cleared
   */
  public boolean getStorageWasCleared() {
    return storageWasCleared;
  }

  /**
   * Sets storage was cleared.
   *
   * @param storageWasCleared the storage was cleared
   */
  public void setStorageWasCleared(final boolean storageWasCleared) {
    this.storageWasCleared = storageWasCleared;
  }

  @Override
  public String toString() {
    String storage = updatedStorage.isEmpty() ? "[not updated]" : updatedStorage.toString();
    if (updatedStorage.isEmpty() && storageWasCleared) {
      storage = "[cleared]";
    }
    return String.format(
        "%s -> {nonce: %s, balance:%s, code:%s, storage:%s }",
        address, nonce, balance, updatedCode == null ? "[not updated]" : updatedCode, storage);
  }
}
