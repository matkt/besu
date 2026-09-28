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
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.worldstate.WorldState;

import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * {@link Eip8347StateSource} backed by a Besu {@link WorldState} at the convert anchor.
 *
 * <p>{@link Eip8347SnapshotGenerator} looks up account fields then storage slots for the same
 * address; this adapter caches the last resolved {@link Account} so slot reads do not repeat {@code
 * worldState.get(address)}.
 */
public final class Eip8347WorldStateSource implements Eip8347StateSource {

  private final WorldState worldState;
  private Address cachedAddress;
  private Account cachedAccount;

  public Eip8347WorldStateSource(final WorldState worldState) {
    this.worldState = Objects.requireNonNull(worldState, "worldState");
  }

  @Override
  public Optional<AccountView> getAccount(final Address address) {
    final Account account = resolve(address);
    if (account == null) {
      return Optional.empty();
    }
    return Optional.of(
        new AccountView(account.getNonce(), account.getBalance(), account.getCode()));
  }

  @Override
  public UInt256 getStorage(final Address address, final Bytes32 slotKey) {
    final Account account = resolve(address);
    if (account == null) {
      return UInt256.ZERO;
    }
    return account.getStorageValue(UInt256.fromBytes(slotKey));
  }

  private Account resolve(final Address address) {
    if (Objects.equals(address, cachedAddress)) {
      return cachedAccount;
    }
    cachedAddress = address;
    cachedAccount = worldState.get(address);
    return cachedAccount;
  }
}
