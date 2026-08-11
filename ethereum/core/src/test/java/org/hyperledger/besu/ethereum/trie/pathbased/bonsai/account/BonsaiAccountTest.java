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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class BonsaiAccountTest {

  @Mock BonsaiWorldState bonsaiWorldState;

  @Test
  void shouldCopyTrackedBonsaiAccountCorrectly() {
    final BonsaiAccount trackedAccount =
        new BonsaiAccount(
            bonsaiWorldState,
            Address.ZERO,
            Hash.hash(Address.ZERO.getBytes()),
            0,
            Wei.ONE,
            new MptStorageRootStrategy(Hash.EMPTY_TRIE_HASH),
            Hash.EMPTY,
            true,
            new BonsaiCodeCache());
    trackedAccount.setCode(Bytes.of(1));
    final UpdateTrackingAccount<BonsaiAccount> bonsaiAccountUpdateTrackingAccount =
        new UpdateTrackingAccount<>(trackedAccount);
    bonsaiAccountUpdateTrackingAccount.setStorageValue(UInt256.ONE, UInt256.ONE);

    final BonsaiAccount expectedAccount = new BonsaiAccount(trackedAccount, bonsaiWorldState, true);
    expectedAccount.setStorageValue(UInt256.ONE, UInt256.ONE);
    assertThat(
            new BonsaiAccount(
                bonsaiWorldState,
                bonsaiAccountUpdateTrackingAccount,
                new MptStorageRootStrategy(Hash.EMPTY_TRIE_HASH),
                trackedAccount.getCodeCache()))
        .isEqualToComparingFieldByField(expectedAccount);
  }

  @Test
  void shouldCopyBonsaiAccountCorrectly() {
    final BonsaiAccount account =
        new BonsaiAccount(
            bonsaiWorldState,
            Address.ZERO,
            Hash.hash(Address.ZERO.getBytes()),
            0,
            Wei.ONE,
            new MptStorageRootStrategy(Hash.EMPTY_TRIE_HASH),
            Hash.EMPTY,
            true,
            new BonsaiCodeCache());
    account.setCode(Bytes.of(1));
    account.setStorageValue(UInt256.ONE, UInt256.ONE);
    assertThat(new BonsaiAccount(account, bonsaiWorldState, true))
        .isEqualToComparingFieldByField(account);
  }

  @Test
  void binaryAccountDelegatesToBinaryStorageRootStrategy() {
    final BonsaiAccount account =
        new BonsaiAccount(
            bonsaiWorldState,
            Address.ZERO,
            Hash.hash(Address.ZERO.getBytes()),
            1L,
            Wei.ONE,
            BinaryStorageRootStrategy.INSTANCE,
            Hash.EMPTY,
            true,
            new BonsaiCodeCache());

    assertThat(account.hasStorageRoot()).isFalse();
    // MPT-specific accessors throw for binary accounts (no storage root).
    assertThatThrownBy(account::getStorageRoot)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("not applicable for binary trie accounts");
    assertThatThrownBy(account::isStorageEmpty)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("not applicable for binary trie accounts");
    // setStorageRoot throws for binary (the strategy owns the state and rejects MPT roots).
    assertThatThrownBy(() -> account.setStorageRoot(Hash.EMPTY_TRIE_HASH))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void mptAccountDelegatesToMptStorageRootStrategy() {
    final Hash storageRoot = Hash.hash(Bytes.of(1, 2, 3));
    final BonsaiAccount account =
        new BonsaiAccount(
            bonsaiWorldState,
            Address.ZERO,
            Hash.hash(Address.ZERO.getBytes()),
            1L,
            Wei.ONE,
            new MptStorageRootStrategy(storageRoot),
            Hash.EMPTY,
            true,
            new BonsaiCodeCache());

    assertThat(account.hasStorageRoot()).isTrue();
    assertThat(account.getStorageRoot()).isEqualTo(storageRoot);
    assertThat(account.isStorageEmpty()).isFalse();

    // setStorageRoot updates the held root (used by the MPT committers).
    final Hash patched = Hash.hash(Bytes.of(4, 5, 6));
    account.setStorageRoot(patched);
    assertThat(account.getStorageRoot()).isEqualTo(patched);

    // isStorageEmpty compares against EMPTY_TRIE_HASH.
    account.setStorageRoot(Hash.EMPTY_TRIE_HASH);
    assertThat(account.isStorageEmpty()).isTrue();
  }
}
