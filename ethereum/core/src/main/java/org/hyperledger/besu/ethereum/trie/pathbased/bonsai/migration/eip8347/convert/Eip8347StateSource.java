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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.convert;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.worldstate.WorldState;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Read-only view of the anchor MPT state used by {@link Eip8347SnapshotGenerator}: nonce, balance,
 * code and storage per preimage address. Production uses {@link #of(WorldState)}; tests stub it.
 * Called from a single thread, so implementations need not be thread-safe.
 */
@FunctionalInterface
public interface Eip8347StateSource {

  /**
   * Account fields needed to emit EIP-8297 leaves for one address.
   *
   * @param nonce account nonce
   * @param balance account balance
   * @param code account code (empty for EOAs; may be an EIP-7702 delegation indicator)
   */
  record AccountView(long nonce, Wei balance, Bytes code) {}

  /** Returns the account at {@code address}, or empty if absent from the anchor state. */
  Optional<AccountView> getAccount(Address address);

  /** Storage value at {@code slotKey} (32-byte big-endian slot), or zero if unset. */
  default UInt256 getStorage(final Address address, final Bytes32 slotKey) {
    return UInt256.ZERO;
  }

  /**
   * View over a Besu {@link WorldState}. The generator reads an account and then its slots, so the
   * last resolved account is kept to avoid one world-state lookup per slot.
   */
  static Eip8347StateSource of(final WorldState worldState) {
    Objects.requireNonNull(worldState, "worldState");
    return new Eip8347StateSource() {
      private Address cachedAddress;
      private Account cachedAccount;

      @Override
      public Optional<AccountView> getAccount(final Address address) {
        return Optional.ofNullable(resolve(address))
            .map(a -> new AccountView(a.getNonce(), a.getBalance(), a.getCode()));
      }

      @Override
      public UInt256 getStorage(final Address address, final Bytes32 slotKey) {
        final Account account = resolve(address);
        return account == null ? UInt256.ZERO : account.getStorageValue(UInt256.fromBytes(slotKey));
      }

      private Account resolve(final Address address) {
        if (!address.equals(cachedAddress)) {
          cachedAddress = address;
          cachedAccount = worldState.get(address);
        }
        return cachedAccount;
      }
    };
  }
}
