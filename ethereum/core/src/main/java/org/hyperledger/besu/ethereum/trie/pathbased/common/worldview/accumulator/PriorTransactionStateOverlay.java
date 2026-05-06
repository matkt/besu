/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedAccount;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Read-only view of state mutations from prior transactions in the same block (from a {@link
 * BlockAccessList}), up to but not including {@code balIndex}. Used to merge prior writes lazily on
 * first read in {@link PathBasedWorldStateUpdateAccumulator}.
 */
public final class PriorTransactionStateOverlay {

  private final Map<Address, AccountEntry> byAddress;

  private PriorTransactionStateOverlay(final Map<Address, AccountEntry> byAddress) {
    this.byAddress = byAddress;
  }

  /**
   * Builds an overlay: for each account in the access list, collects the latest balance, nonce,
   * code, and per-slot storage writes from transactions with {@code txIndex < balIndex}.
   */
  public static PriorTransactionStateOverlay fromBlockAccessList(
      final BlockAccessList blockAccessList, final long balIndex) {
    final Map<Address, AccountEntry> map = new HashMap<>();
    for (final BlockAccessList.AccountChanges accountChanges : blockAccessList.accountChanges()) {
      final Address address = accountChanges.address();
      final Optional<Wei> balance =
          Optional.ofNullable(findLatestBalanceChange(accountChanges.balanceChanges(), balIndex))
              .map(BlockAccessList.BalanceChange::postBalance);
      final Optional<Long> nonce =
          Optional.ofNullable(findLatestNonceChange(accountChanges.nonceChanges(), balIndex))
              .map(BlockAccessList.NonceChange::newNonce);
      final Optional<Bytes> code =
          Optional.ofNullable(findLatestCodeChange(accountChanges.codeChanges(), balIndex))
              .map(BlockAccessList.CodeChange::newCode);

      final Map<StorageSlotKey, UInt256> storage = new HashMap<>();
      for (final BlockAccessList.SlotChanges slotChanges : accountChanges.storageChanges()) {
        final BlockAccessList.StorageChange latest =
            findLatestStorageChange(slotChanges.changes(), balIndex);
        if (latest != null) {
          final UInt256 value = latest.newValue() != null ? latest.newValue() : UInt256.ZERO;
          storage.put(slotChanges.slot(), value);
        }
      }

      if (balance.isPresent() || nonce.isPresent() || code.isPresent() || !storage.isEmpty()) {
        map.put(
            address,
            new AccountEntry(
                balance, nonce, code, Collections.unmodifiableMap(new HashMap<>(storage))));
      }
    }
    return new PriorTransactionStateOverlay(Collections.unmodifiableMap(map));
  }

  public Optional<AccountEntry> accountEntry(final Address address) {
    return Optional.ofNullable(byAddress.get(address));
  }

  public Optional<Bytes> overlayCode(final Address address) {
    return accountEntry(address).flatMap(AccountEntry::code);
  }

  /**
   * Effective slot value after prior in-block writes: overlay value if present, otherwise {@code
   * worldValue}.
   */
  public UInt256 effectiveStorage(
      final Address address, final StorageSlotKey storageSlotKey, final UInt256 worldValue) {
    return accountEntry(address)
        .flatMap(e -> Optional.ofNullable(e.storage().get(storageSlotKey)))
        .orElse(worldValue);
  }

  public boolean hasStorageOverride(final Address address, final StorageSlotKey storageSlotKey) {
    return accountEntry(address).map(e -> e.storage().containsKey(storageSlotKey)).orElse(false);
  }

  /** Applies balance, nonce, and code from the overlay onto {@code mutable}. */
  public static <A extends PathBasedAccount> void applyAccountHeaderOverlay(
      final AccountEntry entry, final A mutable) {
    entry.balance().ifPresent(mutable::setBalance);
    entry.nonce().ifPresent(mutable::setNonce);
    entry.code().ifPresent(mutable::setCode);
  }

  public record AccountEntry(
      Optional<Wei> balance,
      Optional<Long> nonce,
      Optional<Bytes> code,
      Map<StorageSlotKey, UInt256> storage) {}

  private static BlockAccessList.BalanceChange findLatestBalanceChange(
      final Collection<BlockAccessList.BalanceChange> changes, final long maxIndex) {
    BlockAccessList.BalanceChange latest = null;
    long latestIndex = -1L;
    for (final BlockAccessList.BalanceChange change : changes) {
      final long txIndex = change.txIndex();
      if (txIndex < maxIndex && txIndex > latestIndex) {
        latest = change;
        latestIndex = txIndex;
      }
    }
    return latest;
  }

  private static BlockAccessList.NonceChange findLatestNonceChange(
      final Collection<BlockAccessList.NonceChange> changes, final long maxIndex) {
    BlockAccessList.NonceChange latest = null;
    long latestIndex = -1L;
    for (final BlockAccessList.NonceChange change : changes) {
      final long txIndex = change.txIndex();
      if (txIndex < maxIndex && txIndex > latestIndex) {
        latest = change;
        latestIndex = txIndex;
      }
    }
    return latest;
  }

  private static BlockAccessList.CodeChange findLatestCodeChange(
      final Collection<BlockAccessList.CodeChange> changes, final long maxIndex) {
    BlockAccessList.CodeChange latest = null;
    long latestIndex = -1L;
    for (final BlockAccessList.CodeChange change : changes) {
      final long txIndex = change.txIndex();
      if (txIndex < maxIndex && txIndex > latestIndex) {
        latest = change;
        latestIndex = txIndex;
      }
    }
    return latest;
  }

  private static BlockAccessList.StorageChange findLatestStorageChange(
      final Collection<BlockAccessList.StorageChange> changes, final long maxIndex) {
    BlockAccessList.StorageChange latest = null;
    long latestIndex = -1L;
    for (final BlockAccessList.StorageChange change : changes) {
      final long txIndex = change.txIndex();
      if (txIndex < maxIndex && txIndex > latestIndex) {
        latest = change;
        latestIndex = txIndex;
      }
    }
    return latest;
  }
}
