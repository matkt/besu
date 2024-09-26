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
package org.hyperledger.besu.ethereum.blockcreation.txselection;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.GasLimitCalculator;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.MiningParameters;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.eth.transactions.layered.SenderPendingTransactions;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.blockhash.BlockHashProcessor;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelizedConcurrentTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelizedPreProcessingContext;
import org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelizedTransactionContext;
import org.hyperledger.besu.ethereum.mainnet.parallelization.PreprocessingContext;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.diffbased.common.worldview.DiffBasedWorldState;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.txselection.PluginTransactionSelector;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Responsible for extracting transactions from PendingTransactions and determining if the
 * transaction is suitable for inclusion in the block defined by the provided
 * ProcessableBlockHeader.
 *
 * <p>If a transaction is suitable for inclusion, the world state must be updated, and a receipt
 * generated.
 *
 * <p>The output from this class's execution will be:
 *
 * <ul>
 *   <li>A list of transactions to include in the block being constructed.
 *   <li>A list of receipts for inclusion in the block.
 *   <li>The root hash of the world state at the completion of transaction execution.
 *   <li>The amount of gas consumed when executing all transactions.
 *   <li>A list of transactions evaluated but not included in the block being constructed.
 * </ul>
 *
 * Once "used" this class must be discarded and another created. This class contains state which is
 * not cleared between executions of buildTransactionListForBlock().
 */
public class ParallelizedBlockTransactionSelector extends BlockTransactionSelector {

  private final Optional<Counter> confirmedParallelizedTransactionCounter;
  private final Optional<Counter> conflictingButCachedTransactionCounter;

  public ParallelizedBlockTransactionSelector(
      final MiningParameters miningParameters,
      final MainnetTransactionProcessor transactionProcessor,
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final TransactionPool transactionPool,
      final ProcessableBlockHeader processableBlockHeader,
      final AbstractBlockProcessor.TransactionReceiptFactory transactionReceiptFactory,
      final Supplier<Boolean> isCancelled,
      final Address miningBeneficiary,
      final Wei blobGasPrice,
      final FeeMarket feeMarket,
      final GasCalculator gasCalculator,
      final GasLimitCalculator gasLimitCalculator,
      final BlockHashProcessor blockHashProcessor,
      final PluginTransactionSelector pluginTransactionSelector,
      final EthScheduler ethScheduler) {
    super(
        miningParameters,
        transactionProcessor,
        blockchain,
        worldState,
        transactionPool,
        processableBlockHeader,
        transactionReceiptFactory,
        isCancelled,
        miningBeneficiary,
        blobGasPrice,
        feeMarket,
        gasCalculator,
        gasLimitCalculator,
        blockHashProcessor,
        pluginTransactionSelector,
        ethScheduler);

    this.confirmedParallelizedTransactionCounter =
        Optional.of(
            transactionPool
                .getMetrics()
                .getMetricsSystem() // TODO CHANGE THIS WAY TO RETRIEVE METRICS SYSTEM
                .createCounter(
                    BesuMetricCategory.TRANSACTION_POOL,
                    "parallelized_transactions_selection_counter",
                    "Counter for the number of parallelized transactions during block creation"));

    this.conflictingButCachedTransactionCounter =
        Optional.of(
            transactionPool
                .getMetrics()
                .getMetricsSystem()
                .createCounter(
                    BesuMetricCategory.TRANSACTION_POOL,
                    "conflicted_transactions_selection_counter",
                    "Counter for the number of conflicted transactions during block creation"));
  }

  @Override
  protected Optional<PreprocessingContext> runBlockPreProcessing(
      final List<SenderPendingTransactions> candidateTransactions) {
    if ((worldState instanceof DiffBasedWorldState)) {
      ParallelizedConcurrentTransactionProcessor parallelizedConcurrentTransactionProcessor =
          new ParallelizedConcurrentTransactionProcessor(transactionProcessor);
      // runAsyncBlock, if activated, facilitates the  non-blocking parallel execution of
      // transactions in the background through an optimistic strategy.
      CompletableFuture.runAsync(
          () -> {
            final List<Transaction> firstTransactionForSenders =
                candidateTransactions.stream()
                    .map(SenderPendingTransactions::pendingTransactions)
                    .map(List::getFirst)
                    .filter(
                        tx ->
                            evaluatePreProcessing(createTransactionEvaluationContext(tx))
                                .selected())
                    .map(PendingTransaction::getTransaction)
                    .toList();

            System.out.println("amount of transactions " + firstTransactionForSenders.size());

            parallelizedConcurrentTransactionProcessor.runAsyncBlockSelection(
                worldState,
                miningParameters,
                blockSelectionContext.pendingBlockHeader(),
                firstTransactionForSenders,
                blockSelectionContext.miningBeneficiary(),
                blockSelectionContext.blockHashLookup(),
                blockSelectionContext.blobGasPrice(),
                null);
          },
          ParallelizedConcurrentTransactionProcessor.executor);

      return Optional.of(
          new ParallelizedPreProcessingContext(parallelizedConcurrentTransactionProcessor));
    }
    return Optional.empty();
  }

  @Override
  protected TransactionProcessingResult processTransaction(
      final Optional<PreprocessingContext> preProcessingContext,
      final MutableWorldState worldState,
      final WorldUpdater txWorldStateUpdater,
      final PendingTransaction pendingTransaction) {
    {
      TransactionProcessingResult transactionProcessingResult = null;

      if (preProcessingContext.isPresent()) {
        final ParallelizedPreProcessingContext parallelizedPreProcessingContext =
            (ParallelizedPreProcessingContext) preProcessingContext.get();
        Optional<ParallelizedTransactionContext> parallelizedTransactionContext =
            parallelizedPreProcessingContext
                .getParallelizedConcurrentTransactionProcessor()
                .applyParallelizedTransactionResult(
                    worldState,
                    blockSelectionContext.miningBeneficiary(),
                    pendingTransaction.getTransaction(),
                    confirmedParallelizedTransactionCounter,
                    conflictingButCachedTransactionCounter);
        if (parallelizedTransactionContext.isPresent()) {
          transactionProcessingResult =
              parallelizedTransactionContext.get().transactionProcessingResult();
          pluginTransactionSelector.mergeExternalSelector(
              parallelizedTransactionContext.get().getPluginTransactionSelector());
        }
      }

      if (transactionProcessingResult == null) {
        return super.processTransaction(
            preProcessingContext, worldState, txWorldStateUpdater, pendingTransaction);
      } else {
        System.out.println("found already finished transaction");
        return transactionProcessingResult;
      }
    }
  }
}
