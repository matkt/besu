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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.BalStateRootCommitter;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.StateRootComputations;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.BinaryTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.evm.account.Account;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Binary BAL root: replays BAL changes onto the partitioned binary trie via {@link
 * BinaryTrieWriter}.
 */
public final class BinaryBalEngine implements BalStateRootCommitter.Engine {

  public static final BinaryBalEngine INSTANCE = new BinaryBalEngine();

  private BinaryBalEngine() {}

  @Override
  public boolean useBalOverlay() {
    return false;
  }

  @Override
  public BalStateRootCommitter.Result compute(
      final BonsaiWorldState parent,
      final BlockAccessListAccountLookup accountLookup,
      final boolean storageFrozen) {
    return new Computation(parent, accountLookup, storageFrozen).execute();
  }

  private static final class Computation {

    private final BonsaiWorldState worldState;
    private final BlockAccessListAccountLookup accountLookup;
    private final BinaryTrieWriter writer;
    private final Set<Hash> introducedCodeHashes = new HashSet<>();

    Computation(
        final BonsaiWorldState worldState,
        final BlockAccessListAccountLookup accountLookup,
        final boolean storageFrozen) {
      this.worldState = worldState;
      this.accountLookup = accountLookup;
      this.writer =
          new BinaryTrieWriter(
              worldState,
              storageFrozen,
              introducedCodeHashes,
              BinaryTrieFactory.createStateTrie(worldState));
    }

    BalStateRootCommitter.Result execute() {
      for (final BlockAccessList.AccountChanges changes : accountLookup.accountChanges()) {
        if (!changes.hasAnyChange()) {
          continue;
        }
        final Address address = changes.address();
        final BonsaiAccount priorAccount = priorAccount(address);
        final Bytes priorCode = priorCode(address, priorAccount);
        applyAccount(changes, priorAccount, priorCode);
        applyCode(changes, priorCode);
        applyStorage(changes);
      }

      final Hash root = writer.commit();
      System.out.println("Used BAL PBT for  " + root);
      return new BalStateRootCommitter.Result(
          StateRootComputations.pathBased(root, writer.writes()),
          Map.of(),
          Set.copyOf(introducedCodeHashes));
    }

    private void applyAccount(
        final BlockAccessList.AccountChanges changes,
        final BonsaiAccount priorAccount,
        final Bytes priorCode) {
      final boolean headerChanged =
          !changes.nonceChanges().isEmpty()
              || !changes.balanceChanges().isEmpty()
              || !changes.codeChanges().isEmpty();
      if (!headerChanged && priorAccount != null) {
        return;
      }

      final Bytes updatedCode =
          changes.codeChanges().isEmpty()
              ? priorCode
              : nullToEmpty(changes.codeChanges().getLast().newCode());
      final long newNonce =
          changes.nonceChanges().isEmpty()
              ? (priorAccount != null ? priorAccount.getNonce() : 0L)
              : changes.nonceChanges().getLast().newNonce();
      final Wei newBalance =
          changes.balanceChanges().isEmpty()
              ? (priorAccount != null ? priorAccount.getBalance() : Wei.ZERO)
              : changes.balanceChanges().getLast().postBalance();
      final Hash newCodeHash = updatedCode.isEmpty() ? Hash.EMPTY : Hash.hash(updatedCode);

      writer.putAccountHeader(
          changes.address(),
          priorAccount != null,
          priorAccount != null ? priorAccount.getCodeHash() : Hash.EMPTY,
          priorCode,
          newNonce,
          newBalance,
          updatedCode,
          newCodeHash,
          RLP.encode(new BinaryTrieAccountValue(newNonce, newBalance, newCodeHash)::writeTo));
    }

    private void applyCode(final BlockAccessList.AccountChanges changes, final Bytes priorCode) {
      if (changes.codeChanges().isEmpty()) {
        return;
      }
      writer.putCode(
          changes.address(), priorCode, nullToEmpty(changes.codeChanges().getLast().newCode()));
    }

    private void applyStorage(final BlockAccessList.AccountChanges changes) {
      for (final BlockAccessList.SlotChanges slotChanges : changes.storageChanges()) {
        final UInt256 rawValue = slotChanges.changes().getLast().newValue();
        writer.putStorageValue(
            changes.address(), slotChanges.slot(), rawValue == null ? UInt256.ZERO : rawValue);
      }
    }

    private BonsaiAccount priorAccount(final Address address) {
      final Account account = worldState.get(address);
      return account instanceof BonsaiAccount bonsaiAccount ? bonsaiAccount : null;
    }

    private Bytes priorCode(final Address address, final BonsaiAccount priorAccount) {
      if (priorAccount == null || Hash.EMPTY.equals(priorAccount.getCodeHash())) {
        return Bytes.EMPTY;
      }
      return worldState.getCode(address, priorAccount.getCodeHash()).orElse(Bytes.EMPTY);
    }

    private static Bytes nullToEmpty(final Bytes value) {
      return value == null ? Bytes.EMPTY : value;
    }
  }
}
