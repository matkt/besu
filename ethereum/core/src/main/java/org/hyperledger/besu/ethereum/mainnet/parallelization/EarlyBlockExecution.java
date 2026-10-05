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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.MiningBeneficiaryCalculator;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The transactions of an upcoming block, run in the background before the block is validated and
 * processed: they are handed over one at a time as the payload is decoded. The block processing
 * then takes their results, provided the block is the one they ran for: same execution context in
 * the header, same beneficiary, the same instances of all its transactions and the same block
 * access list instance.
 *
 * <p>Once cancelled, it starts no transaction any more (those already running finish, their results
 * are dropped) and the block processing does not take it.
 */
public final class EarlyBlockExecution {

  private final ParallelBlockTransactionProcessor processor;
  private final ProcessableBlockHeader blockHeader;
  private final Address miningBeneficiary;
  private final Optional<BlockAccessList> blockAccessList;
  private final Transaction[] submitted;
  private final AtomicReference<EarlyBlockExecution> owner;
  private volatile boolean cancelled;

  EarlyBlockExecution(
      final ParallelBlockTransactionProcessor processor,
      final ProcessableBlockHeader blockHeader,
      final Address miningBeneficiary,
      final Optional<BlockAccessList> blockAccessList,
      final int transactionCount,
      final AtomicReference<EarlyBlockExecution> owner) {
    this.processor = processor;
    this.blockHeader = blockHeader;
    this.miningBeneficiary = miningBeneficiary;
    this.blockAccessList = blockAccessList;
    this.submitted = new Transaction[transactionCount];
    this.owner = owner;
  }

  /**
   * Starts running the transaction at {@code index} of the block.
   *
   * @param index the index of the transaction in the block
   * @param transaction the transaction
   */
  public void submit(final int index, final Transaction transaction) {
    if (cancelled || index < 0 || index >= submitted.length) {
      return;
    }
    submitted[index] = transaction;
    processor.submit(index, transaction);
  }

  /** Stops running transactions for this block, e.g. once it is processed or rejected. */
  public void cancel() {
    cancelled = true;
    owner.compareAndSet(this, null);
    processor.abort();
  }

  boolean isCancelled() {
    return cancelled;
  }

  ParallelBlockTransactionProcessor processor() {
    return processor;
  }

  /**
   * Whether the transactions ran for this very block: the header carries the execution context they
   * ran with and the same beneficiary, every transaction of the block was handed over (the same
   * instance at each index), and the block access list is the same instance.
   */
  // the very instances decoded from the payload reach the block processing: compare identities
  @SuppressWarnings("ReferenceEquality")
  boolean isFor(
      final BlockHeader header,
      final List<Transaction> transactions,
      final Optional<BlockAccessList> maybeBlockAccessList,
      final MiningBeneficiaryCalculator miningBeneficiaryCalculator) {
    if (cancelled
        || transactions.size() != submitted.length
        || maybeBlockAccessList.isPresent() != blockAccessList.isPresent()
        || (blockAccessList.isPresent() && maybeBlockAccessList.get() != blockAccessList.get())
        || !sameExecutionContext(header, blockHeader)
        || !miningBeneficiaryCalculator.calculateBeneficiary(header).equals(miningBeneficiary)) {
      return false;
    }
    for (int i = 0; i < submitted.length; i++) {
      if (submitted[i] != transactions.get(i)) {
        return false;
      }
    }
    return true;
  }

  /** The header fields a transaction execution reads. */
  private static boolean sameExecutionContext(
      final ProcessableBlockHeader header, final ProcessableBlockHeader ranWith) {
    return header.getParentHash().equals(ranWith.getParentHash())
        && header.getCoinbase().equals(ranWith.getCoinbase())
        && header.getNumber() == ranWith.getNumber()
        && header.getTimestamp() == ranWith.getTimestamp()
        && header.getGasLimit() == ranWith.getGasLimit()
        && header.getBaseFee().equals(ranWith.getBaseFee())
        && header.getDifficulty().equals(ranWith.getDifficulty())
        && header.getMixHashOrPrevRandao().equals(ranWith.getMixHashOrPrevRandao())
        && header.getParentBeaconBlockRoot().equals(ranWith.getParentBeaconBlockRoot())
        && Objects.equals(header.getOptionalSlotNumber(), ranWith.getOptionalSlotNumber());
  }
}
