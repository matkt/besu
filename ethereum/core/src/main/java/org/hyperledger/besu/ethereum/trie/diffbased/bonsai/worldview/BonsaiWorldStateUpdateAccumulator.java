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
import org.hyperledger.besu.ethereum.trie.diffbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.diffbased.common.DiffBasedValue;
import org.hyperledger.besu.ethereum.trie.diffbased.common.worldview.DiffBasedWorldView;
import org.hyperledger.besu.ethereum.trie.diffbased.common.worldview.accumulator.DiffBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.diffbased.common.worldview.accumulator.preload.Consumer;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;

import java.util.concurrent.CompletableFuture;

public class BonsaiWorldStateUpdateAccumulator
    extends DiffBasedWorldStateUpdateAccumulator<BonsaiAccount> {

  final boolean isAccountPreloadEnabled;

  public BonsaiWorldStateUpdateAccumulator(
      final DiffBasedWorldView world,
      final Consumer<DiffBasedValue<BonsaiAccount>> accountPreloader,
      final Consumer<StorageSlotKey> storagePreloader,
      final EvmConfiguration evmConfiguration) {
    super(world, accountPreloader, storagePreloader, evmConfiguration);
    isAccountPreloadEnabled = false;
  }

  public BonsaiWorldStateUpdateAccumulator(final DiffBasedWorldView world, final Consumer<DiffBasedValue<BonsaiAccount>> accountPreloader,
                                           final Consumer<StorageSlotKey> storagePreloader,
                                           final EvmConfiguration evmConfiguration,
                                           final boolean isAccountPreloadEnabled) {
    super(world, accountPreloader, storagePreloader, evmConfiguration);
    this.isAccountPreloadEnabled = isAccountPreloadEnabled;
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

  @Override
  public Account get(final Address address) {
    final Account account = super.get(address);
    preloadAccount(account);
    return account;
  }

  @Override
  public MutableAccount getAccount(final Address address) {
    final MutableAccount account = super.getAccount(address);
    preloadAccount(account);
    return account;
  }

  private void preloadAccount(final Account account){
    CompletableFuture.runAsync(() -> {
      if(account instanceof BonsaiAccount bonsaiAccount){
        if(!bonsaiAccount.getStorageRoot().equals(Hash.EMPTY_TRIE_HASH)){
          for (int i = 0; i < 256; i++) {
            getStorageValueByStorageSlotKey(account.getAddress(), new StorageSlotKey(UInt256.valueOf(i)));
          }
        }
      }
    });
  }
}
