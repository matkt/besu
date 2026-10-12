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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListChanges.AccountFinalChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListChanges.StorageFinalChange;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Net state change of a sequence of blocks, folded from their block access lists. Only post values
 * are kept, the last write wins.
 */
public final class StateDiff {

  private final TreeMap<Bytes32, AccountChange> accounts = new TreeMap<>();
  private long slotCount;

  /** Folds the next block, which must follow the blocks already folded. */
  public void add(final BlockAccessList blockAccessList) {
    for (final AccountFinalChanges changes :
        BlockAccessListChanges.latestChanges(blockAccessList)) {
      if (!changes.hasAnyChange()) {
        continue;
      }
      final Bytes32 accountHash = Bytes32.wrap(changes.address().addressHash().getBytes());
      final AccountChange account =
          accounts.computeIfAbsent(accountHash, h -> new AccountChange(changes.address(), h));
      changes.nonce().ifPresent(nonce -> account.nonce = nonce);
      changes.balance().ifPresent(balance -> account.balance = balance);
      changes.code().ifPresent(code -> account.code = code);
      for (final StorageFinalChange slot : changes.storageChanges()) {
        if (account.storage.put(Bytes32.wrap(slot.slot().getSlotHash().getBytes()), slot.value())
            == null) {
          slotCount++;
        }
      }
    }
  }

  /** Returns the changed accounts, sorted by account hash. */
  public NavigableMap<Bytes32, AccountChange> accounts() {
    return Collections.unmodifiableNavigableMap(accounts);
  }

  /** Returns the change of an account, if any. */
  public Optional<AccountChange> account(final Bytes32 accountHash) {
    return Optional.ofNullable(accounts.get(accountHash));
  }

  /** Returns whether no state changed. */
  public boolean isEmpty() {
    return accounts.isEmpty();
  }

  /** Returns the number of changed storage slots. */
  public long slotCount() {
    return slotCount;
  }

  /** Final changes of one account; a null field means unchanged. */
  public static final class AccountChange {
    private final Address address;
    private final Bytes32 accountHash;
    private Long nonce;
    private Wei balance;
    private Bytes code;
    private final TreeMap<Bytes32, UInt256> storage = new TreeMap<>();

    AccountChange(final Address address, final Bytes32 accountHash) {
      this.address = address;
      this.accountHash = accountHash;
    }

    public Address address() {
      return address;
    }

    public Bytes32 accountHash() {
      return accountHash;
    }

    public Optional<Long> nonce() {
      return Optional.ofNullable(nonce);
    }

    public Optional<Wei> balance() {
      return Optional.ofNullable(balance);
    }

    public Optional<Bytes> code() {
      return Optional.ofNullable(code);
    }

    /** Returns the final value of each written slot, keyed by slot hash. */
    public NavigableMap<Bytes32, UInt256> storage() {
      return Collections.unmodifiableNavigableMap(storage);
    }

    /** Returns whether a field of the account itself changed. */
    public boolean changesAccountFields() {
      return nonce != null || balance != null || code != null;
    }
  }
}
