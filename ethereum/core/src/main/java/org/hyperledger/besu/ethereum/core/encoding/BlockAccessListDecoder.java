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
package org.hyperledger.besu.ethereum.core.encoding;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotRead;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public final class BlockAccessListDecoder {

  /**
   * Accounts from which a list is decoded in parallel: a block of thousands of transfers lists tens
   * of thousands of accounts, decoded before anything else of its payload, and their entries are
   * independent.
   */
  private static final int PARALLEL_DECODING_MIN_ACCOUNTS = 1024;

  /** Accounts decoded by each task of a parallel decoding. */
  private static final int ACCOUNTS_PER_TASK = 512;

  private BlockAccessListDecoder() {}

  public static BlockAccessList decode(final RLPInput in) {
    // delimiting the accounts only reads their length, decoding them is the work
    final List<RLPInput> accountInputs = new ArrayList<>();
    in.enterList();
    while (!in.isEndOfCurrentList()) {
      accountInputs.add(in.readAsRlp());
    }
    in.leaveList();

    final List<AccountChanges> accounts =
        accountInputs.size() < PARALLEL_DECODING_MIN_ACCOUNTS
            ? decodeAccounts(accountInputs)
            : decodeAccountsInParallel(accountInputs);
    return new BlockAccessList(accounts, Optional.of(in.raw()));
  }

  private static List<AccountChanges> decodeAccounts(final List<RLPInput> accountInputs) {
    final List<AccountChanges> accounts = new ArrayList<>(accountInputs.size());
    for (final RLPInput accountInput : accountInputs) {
      accounts.add(decodeAccount(accountInput));
    }
    return accounts;
  }

  /** Decodes lots of accounts in parallel, keeping their order, and fails like decoding them. */
  private static List<AccountChanges> decodeAccountsInParallel(final List<RLPInput> accountInputs) {
    final List<CompletableFuture<List<AccountChanges>>> lots = new ArrayList<>();
    for (int start = 0; start < accountInputs.size(); start += ACCOUNTS_PER_TASK) {
      final List<RLPInput> lot =
          accountInputs.subList(start, Math.min(start + ACCOUNTS_PER_TASK, accountInputs.size()));
      lots.add(CompletableFuture.supplyAsync(() -> decodeAccounts(lot), ForkJoinPool.commonPool()));
    }
    final List<AccountChanges> accounts = new ArrayList<>(accountInputs.size());
    try {
      for (final CompletableFuture<List<AccountChanges>> lot : lots) {
        accounts.addAll(lot.join());
      }
    } catch (final CompletionException e) {
      if (e.getCause() instanceof RuntimeException cause) {
        throw cause;
      }
      throw e;
    }
    return accounts;
  }

  private static AccountChanges decodeAccount(final RLPInput acctIn) {
    acctIn.enterList();

    Address address = Address.readFrom(acctIn);

    List<SlotChanges> slotChanges =
        acctIn.readList(
            scIn -> {
              scIn.enterList();
              StorageSlotKey slot = new StorageSlotKey(scIn.readUInt256Scalar());
              List<StorageChange> changes =
                  scIn.readList(
                      changeIn -> {
                        changeIn.enterList();
                        long txIndex = changeIn.readUnsignedIntScalar();
                        UInt256 newVal = changeIn.readUInt256Scalar();
                        changeIn.leaveList();
                        return new StorageChange(txIndex, newVal);
                      });
              // An empty change list is well-formed RLP. The EIP-7928 "at least one storage
              // change" rule is left to MainnetBlockAccessListValidator, so the block comes back
              // INVALID instead of engine_newPayload failing on invalid params.
              scIn.leaveList();
              return new SlotChanges(slot, changes);
            });

    List<SlotRead> reads =
        acctIn.readList(r -> new SlotRead(new StorageSlotKey(r.readUInt256Scalar())));

    List<BalanceChange> balances =
        acctIn.readList(
            bcIn -> {
              bcIn.enterList();
              long txIndex = bcIn.readUnsignedIntScalar();
              Wei postBalance = Wei.of(bcIn.readUInt256Scalar());
              bcIn.leaveList();
              return new BalanceChange(txIndex, postBalance);
            });

    List<NonceChange> nonces =
        acctIn.readList(
            ncIn -> {
              ncIn.enterList();
              long txIndex = ncIn.readUnsignedIntScalar();
              long newNonce = ncIn.readLongScalar();
              ncIn.leaveList();
              return new NonceChange(txIndex, newNonce);
            });

    List<CodeChange> codes =
        acctIn.readList(
            ccIn -> {
              ccIn.enterList();
              long txIndex = ccIn.readUnsignedIntScalar();
              Bytes newCode = ccIn.readBytes();
              ccIn.leaveList();
              return new CodeChange(txIndex, newCode);
            });

    acctIn.leaveList();

    return new AccountChanges(address, slotChanges, reads, balances, nonces, codes);
  }
}
