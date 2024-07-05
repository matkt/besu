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
import static org.hyperledger.besu.evm.operation.BlockHashOperation.BlockHashLookup;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.parallelization.PreloadConcurrentTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.requests.RequestProcessorCoordinator;
import org.hyperledger.besu.ethereum.privacy.storage.PrivateMetadataUpdater;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.diffbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.diffbased.bonsai.worldview.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.vm.CachingBlockHashLookup;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldState;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  @Override
  public BlockProcessingResult processBlock(
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final BlockHeader blockHeader,
      final List<Transaction> transactions,
      final List<BlockHeader> ommers,
      final Optional<List<Withdrawal>> maybeWithdrawals,
      final PrivateMetadataUpdater privateMetadataUpdater) {
    final List<TransactionReceipt> receipts = new ArrayList<>();
    long currentGasUsed = 0;
    long currentBlobGasUsed = 0;

    final PreloadConcurrentTransactionProcessor preloadConcurrentTransactionProcessor =
        new PreloadConcurrentTransactionProcessor(transactionProcessor);

    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(blockHeader);

    protocolSpec.getBlockHashProcessor().processBlockHashes(blockchain, worldState, blockHeader);
    final BlockHashLookup blockHashLookup = new CachingBlockHashLookup(blockHeader, blockchain);
    final Address miningBeneficiary = miningBeneficiaryCalculator.calculateBeneficiary(blockHeader);

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

    /*
     * The runAsyncPreloadBlock method is specifically working with BONSAI.
     * It facilitates the parallel execution of transactions in the background through an optimistic strategy.
     * Being non-blocking, it permits reading and modifying a majority of slots and accounts while also
     * allowing transactions to be executed optimistically. This approach preloads most accounts and slots
     * into the cache, and also reducing the need to replay transactions in the absence of collisions.
     */
    preloadConcurrentTransactionProcessor.runAsyncPreloadBlock(
        worldState,
        blockHeader,
        transactions,
        miningBeneficiary,
        blockHashLookup,
        blobGasPrice,
        privateMetadataUpdater);

    for (int i = 0; i < transactions.size(); i++) {
      final Transaction transaction = transactions.get(i);
      if (!hasAvailableBlockBudget(blockHeader, transaction, currentGasUsed)) {
        return new BlockProcessingResult(Optional.empty(), "provided gas insufficient");
      }
      final WorldUpdater blockUpdater = worldState.updater();

      /*
       * This applyPreloadBlockResult is designed to fetch the results of transactions processed by background threads.
       * If the transactions were successfully executed in the background, their results are applied to the block.
       * In the absence of collisions, the entire result is applied without re-executing the transaction.
       * However, in the event of a conflict, only the data we read are added to the block cache,
       * and the transaction is replayed. This approach minimizes the amount of data that needs to be
       * fetched from the disk. If a transaction was not processed in time in the background,
       * it is executed in the usual manner.
       */
      TransactionProcessingResult transactionProcessingResult =
          preloadConcurrentTransactionProcessor
              .applyPreloadBlockResult(worldState, miningBeneficiary, transaction, i)
              .orElse(null);

      if (transactionProcessingResult == null) {
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
      if (transactionProcessingResult.isInvalid()) {
        String errorMessage =
            MessageFormat.format(
                "Block processing error: transaction invalid {0}. Block {1} Transaction {2}",
                transactionProcessingResult.getValidationResult().getErrorMessage(),
                blockHeader.getHash().toHexString(),
                transaction.getHash().toHexString());
        LOG.info(errorMessage);
        if (worldState instanceof BonsaiWorldState) {
          ((BonsaiWorldStateUpdateAccumulator) blockUpdater).reset();
        }
        return new BlockProcessingResult(Optional.empty(), errorMessage);
      }

      blockUpdater.commit();

      currentGasUsed += transaction.getGasLimit() - transactionProcessingResult.getGasRemaining();
      if (transaction.getVersionedHashes().isPresent()) {
        currentBlobGasUsed +=
            (transaction.getVersionedHashes().get().size() * CancunGasCalculator.BLOB_GAS_PER_BLOB);
      }

      final TransactionReceipt transactionReceipt =
          transactionReceiptFactory.create(
              transaction.getType(), transactionProcessingResult, worldState, currentGasUsed);
      receipts.add(transactionReceipt);
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
    System.out.println(
        "conflictingButCachedTransaction result "
            + preloadConcurrentTransactionProcessor.getConflictingButCachedTransaction());
    System.out.println(
        "confirmedParallelizedTransaction result "
            + preloadConcurrentTransactionProcessor.getConfirmedParallelizedTransaction());

    // EIP-7685: process EL requests
    final Optional<RequestProcessorCoordinator> requestProcessor =
        protocolSpec.getRequestProcessorCoordinator();
    Optional<List<Request>> maybeRequests = Optional.empty();
    if (requestProcessor.isPresent()) {
      maybeRequests = requestProcessor.get().process(worldState, receipts);
    }

    if (!rewardCoinbase(worldState, blockHeader, ommers, skipZeroBlockRewards)) {
      // no need to log, rewardCoinbase logs the error.
      if (worldState instanceof BonsaiWorldState) {
        ((BonsaiWorldStateUpdateAccumulator) worldState.updater()).reset();
      }
      return new BlockProcessingResult(Optional.empty(), "ommer too old");
    }

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

    return new BlockProcessingResult(
        Optional.of(new BlockProcessingOutputs(worldState, receipts, maybeRequests)));
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
