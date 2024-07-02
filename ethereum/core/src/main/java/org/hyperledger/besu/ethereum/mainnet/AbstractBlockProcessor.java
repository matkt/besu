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
package org.hyperledger.besu.ethereum.mainnet;

import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Deposit;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.privacy.storage.PrivateMetadataUpdater;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.diffbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.diffbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.diffbased.bonsai.worldview.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.diffbased.common.worldview.accumulator.DiffBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.vm.BlockHashLookup;
import org.hyperledger.besu.ethereum.vm.CachingBlockHashLookup;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldState;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("SuspiciousMethodCalls")
public abstract class AbstractBlockProcessor implements BlockProcessor {

  @FunctionalInterface
  public interface TransactionReceiptFactory {

    TransactionReceipt create(
        TransactionType transactionType,
        TransactionProcessingResult result,
        WorldState worldState,
        long gasUsed);
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBlockProcessor.class);

  static final int MAX_GENERATION = 6;

  protected final MainnetTransactionProcessor transactionProcessor;

  protected final AbstractBlockProcessor.TransactionReceiptFactory transactionReceiptFactory;

  final Wei blockReward;

  protected final boolean skipZeroBlockRewards;
  private final ProtocolSchedule protocolSchedule;

  protected final MiningBeneficiaryCalculator miningBeneficiaryCalculator;

  protected AbstractBlockProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final TransactionReceiptFactory transactionReceiptFactory,
      final Wei blockReward,
      final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
      final boolean skipZeroBlockRewards,
      final ProtocolSchedule protocolSchedule) {
    this.transactionProcessor = transactionProcessor;
    this.transactionReceiptFactory = transactionReceiptFactory;
    this.blockReward = blockReward;
    this.miningBeneficiaryCalculator = miningBeneficiaryCalculator;
    this.skipZeroBlockRewards = skipZeroBlockRewards;
    this.protocolSchedule = protocolSchedule;
  }

  @SuppressWarnings({"unchecked", "ReassignedVariable"})
  @Override
  public BlockProcessingResult processBlock(
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final BlockHeader blockHeader,
      final List<Transaction> transactions,
      final List<BlockHeader> ommers,
      final Optional<List<Withdrawal>> maybeWithdrawals,
      final Optional<List<Deposit>> maybeDeposits,
      final PrivateMetadataUpdater privateMetadataUpdater) {
    final List<TransactionReceipt> receipts = new ArrayList<>();
    long currentGasUsed = 0;
    long currentBlobGasUsed = 0;

    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(blockHeader);

    if (blockHeader.getParentBeaconBlockRoot().isPresent()) {
      final WorldUpdater updater = worldState.updater();
      ParentBeaconBlockRootHelper.storeParentBeaconBlockRoot(
          updater, blockHeader.getTimestamp(), blockHeader.getParentBeaconBlockRoot().get());
    }

    final BlockHashLookup blockHashLookup = new CachingBlockHashLookup(blockHeader, blockchain);
    final Address miningBeneficiary = miningBeneficiaryCalculator.calculateBeneficiary(blockHeader);

    TransactionConflictChecker transactionConflictChecker = new TransactionConflictChecker();
    transactionConflictChecker.findParallelTransactions(miningBeneficiary, transactions);

    Optional<BlockHeader> maybeParentHeader =
        blockchain.getBlockHeader(blockHeader.getParentHash());

    Wei blobGasPrice =
        maybeParentHeader
            .map(
                parentHeader ->
                    protocolSpec
                        .getFeeMarket()
                        .blobGasPricePerGas(
                            calculateExcessBlobGasForParent(protocolSpec, parentHeader)))
            .orElse(Wei.ZERO);

    transactionConflictChecker.getParallelizedTransactions().parallelStream()
        .forEach(
            transaction -> {
              BonsaiWorldState roundWorldState =
                  new BonsaiWorldState((BonsaiWorldState) worldState);
              WorldUpdater roundWorldStateUpdater = roundWorldState.updater();

              final TransactionProcessingResult result =
                  transactionProcessor.processTransaction(
                      roundWorldStateUpdater,
                      blockHeader,
                      transaction.transaction(),
                      miningBeneficiary,
                      OperationTracer.NO_TRACING,
                      blockHashLookup,
                      true,
                      TransactionValidationParams.processingBlock(),
                      privateMetadataUpdater,
                      blobGasPrice);
              roundWorldStateUpdater.commit();
              transactionConflictChecker.saveParallelizedTransactionProcessingResult(
                  transaction, roundWorldState.getAccumulator(), result);
            });

    int confirmedParallelizedTransaction = 0;
    try {
      final BonsaiWorldStateUpdateAccumulator blockUpdater =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();
      for (int i = 0; i < transactions.size(); i++) {
        Transaction transaction = transactions.get(i);
        final TransactionProcessingResult transactionProcessingResult;

        final DiffBasedWorldStateUpdateAccumulator<BonsaiAccount> transactionAccumulator =
            (DiffBasedWorldStateUpdateAccumulator<BonsaiAccount>)
                transactionConflictChecker.getAccumulatorByTransaction().get((long) i);
        if (transactionAccumulator != null
            && !transactionConflictChecker.checkConflicts(
                miningBeneficiary,
                new TransactionConflictChecker.TransactionWithLocation(i, transaction),
                transactionAccumulator,
                blockUpdater)) {
          blockUpdater.cloneFromUpdater(
              (DiffBasedWorldStateUpdateAccumulator<BonsaiAccount>)
                  transactionConflictChecker.getAccumulatorByTransaction().get((long) i));
          transactionProcessingResult =
              transactionConflictChecker.getResultByTransaction().get((long) i);
          confirmedParallelizedTransaction++;
        } else {
          transactionProcessingResult =
              transactionProcessor.processTransaction(
                  blockUpdater,
                  blockHeader,
                  transaction,
                  miningBeneficiary,
                  OperationTracer.NO_TRACING,
                  blockHashLookup,
                  true,
                  TransactionValidationParams.processingBlock(),
                  privateMetadataUpdater,
                  blobGasPrice);
        }
        final var coinbase = blockUpdater.getOrCreate(miningBeneficiary);
        if (transactionProcessingResult.getMiningBenef() != null) {
          coinbase.incrementBalance(transactionProcessingResult.getMiningBenef());
        }
        blockUpdater.commit();

        currentGasUsed += transaction.getGasLimit() - transactionProcessingResult.getGasRemaining();
        if (transaction.getVersionedHashes().isPresent()) {
          currentBlobGasUsed +=
              (transaction.getVersionedHashes().get().size()
                  * CancunGasCalculator.BLOB_GAS_PER_BLOB);
        }
        final TransactionReceipt transactionReceipt =
            transactionReceiptFactory.create(
                transaction.getType(), transactionProcessingResult, worldState, currentGasUsed);
        receipts.add(transactionReceipt);
      }
      System.out.println("confirmedParallelizedTransaction " + confirmedParallelizedTransaction);
    } catch (Throwable e) {
      e.printStackTrace();
      System.out.println("error " + e);
    }

    if (blockHeader.getBlobGasUsed().isPresent()
        && currentBlobGasUsed != blockHeader.getBlobGasUsed().get()) {
      String errorMessage =
          String.format(
              "block did not consume expected blob gas: header %d, transactions %d",
              blockHeader.getBlobGasUsed().get(), currentBlobGasUsed);
      LOG.error(errorMessage);
      return new BlockProcessingResult(Optional.empty(), errorMessage);
    }
    final Optional<WithdrawalsProcessor> maybeWithdrawalsProcessor =
        protocolSpec.getWithdrawalsProcessor();
    if (maybeWithdrawalsProcessor.isPresent() && maybeWithdrawals.isPresent()) {
      try {
        maybeWithdrawalsProcessor
            .get()
            .processWithdrawals(maybeWithdrawals.get(), worldState.updater());
      } catch (final Exception e) {
        LOG.error("failed processing withdrawals", e);
        return new BlockProcessingResult(Optional.empty(), e);
      }
    }

    final WithdrawalRequestValidator exitsValidator = protocolSpec.getWithdrawalRequestValidator();
    if (exitsValidator.allowWithdrawalRequests()) {
      // Performing system-call logic
      WithdrawalRequestContractHelper.popWithdrawalRequestsFromQueue(worldState);
    }

    if (!rewardCoinbase(worldState, blockHeader, ommers, skipZeroBlockRewards)) {
      // no need to log, rewardCoinbase logs the error.
      if (worldState instanceof BonsaiWorldState) {
        ((BonsaiWorldStateUpdateAccumulator) worldState.updater()).reset();
      }
      return new BlockProcessingResult(Optional.empty(), "ommer too old");
    }

    System.out.println(worldState.updater().get(miningBeneficiary).getBalance());
    try {
      worldState.persist(blockHeader);
    } catch (MerkleTrieException e) {
      LOG.trace("Merkle trie exception during Transaction processing ", e);
      if (worldState instanceof BonsaiWorldState) {
        ((BonsaiWorldStateUpdateAccumulator) worldState.updater()).reset();
      }
      throw e;
    } catch (Exception e) {
      LOG.error("failed persisting block", e);
      return new BlockProcessingResult(Optional.empty(), e);
    }
    return new BlockProcessingResult(Optional.of(new BlockProcessingOutputs(worldState, receipts)));
  }

  protected boolean hasAvailableBlockBudget(
      final BlockHeader blockHeader, final Transaction transaction, final long currentGasUsed) {
    final long remainingGasBudget = blockHeader.getGasLimit() - currentGasUsed;
    if (Long.compareUnsigned(transaction.getGasLimit(), remainingGasBudget) > 0) {
      LOG.info(
          "Block processing error: transaction gas limit {} exceeds available block budget"
              + " remaining {}. Block {} Transaction {}",
          transaction.getGasLimit(),
          remainingGasBudget,
          blockHeader.getHash().toHexString(),
          transaction.getHash().toHexString());
      return false;
    }

    return true;
  }

  protected MiningBeneficiaryCalculator getMiningBeneficiaryCalculator() {
    return miningBeneficiaryCalculator;
  }

  abstract boolean rewardCoinbase(
      final MutableWorldState worldState,
      final BlockHeader header,
      final List<BlockHeader> ommers,
      final boolean skipZeroBlockRewards);
}
