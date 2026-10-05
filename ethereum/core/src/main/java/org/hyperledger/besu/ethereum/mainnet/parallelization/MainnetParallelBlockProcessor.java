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

import static org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator.calculateExcessBlobGasForParent;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.MiningBeneficiaryCalculator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpecBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListFactory;
import org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch.BalPrefetch;
import org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch.BalPrefetcher;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainnetParallelBlockProcessor extends MainnetBlockProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(MainnetParallelBlockProcessor.class);

  private final Optional<Counter> confirmedParallelizedTransactionCounter;
  private final Optional<Counter> conflictingButCachedTransactionCounter;

  private static final Executor executor = BlockProcessingExecutors.cpuExecutor();

  private final Optional<BalPrefetcher> maybePrefetcher;

  /** Block access list whose state is being prefetched ahead of the processing of its block. */
  private final AtomicReference<BlockAccessList> prefetchedAhead = new AtomicReference<>();

  /** Transactions of an upcoming block running ahead of the processing of the block. */
  private final AtomicReference<EarlyBlockExecution> startedEarly = new AtomicReference<>();

  private final ProtocolSchedule protocolSchedule;

  public MainnetParallelBlockProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final TransactionReceiptFactory transactionReceiptFactory,
      final Wei blockReward,
      final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
      final boolean skipZeroBlockRewards,
      final ProtocolSchedule protocolSchedule,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    super(
        transactionProcessor,
        transactionReceiptFactory,
        blockReward,
        miningBeneficiaryCalculator,
        skipZeroBlockRewards,
        protocolSchedule,
        balConfiguration,
        metricsSystem);
    this.confirmedParallelizedTransactionCounter =
        Optional.of(
            metricsSystem.createCounter(
                BesuMetricCategory.BLOCK_PROCESSING,
                "parallelized_transactions_counter",
                "Counter for the number of parallelized transactions during block processing"));

    this.protocolSchedule = protocolSchedule;
    this.maybePrefetcher = BalPrefetcher.fromConfiguration(balConfiguration);
    this.conflictingButCachedTransactionCounter =
        Optional.of(
            metricsSystem.createCounter(
                BesuMetricCategory.BLOCK_PROCESSING,
                "conflicted_transactions_counter",
                "Counter for the number of conflicted transactions during block processing"));
  }

  /**
   * Starts prefetching the state of a block from its access list as soon as the payload is
   * received, so that it is mostly in cache when the block is executed. Processing that block (the
   * same access list instance) then does not prefetch it again.
   */
  @Override
  public Optional<BalPrefetch> prefetchBlockAccessList(
      final ProtocolContext protocolContext,
      final BlockHeader parentHeader,
      final BlockAccessList blockAccessList) {
    return maybePrefetcher.map(
        prefetcher -> {
          prefetchedAhead.set(blockAccessList);
          return prefetcher.prefetch(protocolContext, parentHeader, blockAccessList);
        });
  }

  /**
   * Starts running the transactions of an upcoming block before it is validated and processed: with
   * its block access list (as {@link BalConcurrentTransactionProcessor} does) when the
   * configuration uses it, optimistically otherwise. Processing that block takes their results if
   * they ran for it (see {@link EarlyBlockExecution}); the prefetch of the block access list is
   * started separately ({@link #prefetchBlockAccessList}).
   */
  @Override
  public Optional<EarlyBlockExecution> startBlockExecution(
      final ProtocolContext protocolContext,
      final BlockHeader parentHeader,
      final ProcessableBlockHeader blockHeader,
      final Optional<BlockAccessList> blockAccessList,
      final int transactionCount) {
    if (!(protocolContext.getWorldStateArchive() instanceof PathBasedWorldStateProvider)) {
      return Optional.empty();
    }
    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(blockHeader);
    final ParallelBlockTransactionProcessor processor =
        balConfiguration.isPerfectParallelizationEnabled() && blockAccessList.isPresent()
            ? new BalConcurrentTransactionProcessor(
                transactionProcessor, blockAccessList.get(), Optional.empty())
            : new OptimisticConcurrentTransactionProcessor(transactionProcessor);
    // a payload's beneficiary is its fee recipient; the block processing checks it is the one
    final Address miningBeneficiary = blockHeader.getCoinbase();
    processor.start(
        protocolContext,
        blockHeader,
        transactionCount,
        miningBeneficiary,
        protocolSpec
            .getPreExecutionProcessor()
            .createBlockHashLookup(protocolContext.getBlockchain(), blockHeader),
        protocolSpec
            .getFeeMarket()
            .blobGasPricePerGas(calculateExcessBlobGasForParent(protocolSpec, parentHeader)),
        executor,
        protocolSpec
            .getBlockAccessListFactory()
            .map(BlockAccessListFactory::newBlockAccessListBuilder),
        Optional.of(parentHeader));
    final EarlyBlockExecution execution =
        new EarlyBlockExecution(
            processor,
            blockHeader,
            miningBeneficiary,
            blockAccessList,
            transactionCount,
            startedEarly);
    final EarlyBlockExecution previous = startedEarly.getAndSet(execution);
    if (previous != null) {
      previous.cancel();
    }
    return Optional.of(execution);
  }

  @Override
  protected TransactionProcessingResult getTransactionProcessingResult(
      final Optional<PreprocessingContext> preProcessingContext,
      final BlockProcessingContext blockProcessingContext,
      final WorldUpdater transactionUpdater,
      final Wei blobGasPrice,
      final Address miningBeneficiary,
      final Transaction transaction,
      final int location,
      final BlockHashLookup blockHashLookup,
      final Optional<AccessLocationTracker> accessLocationTracker) {
    return preProcessingContext
        .flatMap(
            ctx ->
                ctx.processor()
                    .getProcessingResult(
                        blockProcessingContext.getWorldState(),
                        miningBeneficiary,
                        transaction,
                        location,
                        confirmedParallelizedTransactionCounter,
                        conflictingButCachedTransactionCounter))
        .orElseGet(
            () ->
                super.getTransactionProcessingResult(
                    preProcessingContext,
                    blockProcessingContext,
                    transactionUpdater,
                    blobGasPrice,
                    miningBeneficiary,
                    transaction,
                    location,
                    blockHashLookup,
                    accessLocationTracker));
  }

  @Override
  public BlockProcessingResult processBlock(
      final ProtocolContext protocolContext,
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final Block block) {
    return processBlock(protocolContext, blockchain, worldState, block, Optional.empty());
  }

  @Override
  public BlockProcessingResult processBlock(
      final ProtocolContext protocolContext,
      final Blockchain blockchain,
      final MutableWorldState worldState,
      final Block block,
      final Optional<BlockAccessList> blockAccessList) {
    final boolean isPrefetchedAhead =
        blockAccessList.isPresent() && prefetchedAhead.compareAndSet(blockAccessList.get(), null);
    final BlockProcessingResult blockProcessingResult =
        super.processBlock(
            protocolContext,
            blockchain,
            worldState,
            block,
            blockAccessList,
            new ParallelTransactionPreprocessing(
                transactionProcessor,
                executor,
                balConfiguration,
                isPrefetchedAhead ? Optional.empty() : maybePrefetcher,
                takeStartedEarly(block, blockAccessList)));
    if (blockProcessingResult.isFailed()) {
      // Fallback to non-parallel processing if there is a block processing exception .
      LOG.info(
          "Parallel transaction processing failure. Falling back to non-parallel processing for block #{} ({})",
          block.getHeader().getNumber(),
          block.getHash());
      if (worldState instanceof BonsaiWorldState) {
        ((BonsaiWorldStateUpdateAccumulator) worldState.updater()).reset();
      }
      return super.processBlock(protocolContext, blockchain, worldState, block, blockAccessList);
    }
    return blockProcessingResult;
  }

  @VisibleForTesting
  boolean hasStartedEarly() {
    return startedEarly.get() != null;
  }

  /**
   * The processor of the transactions started early for this very block, if any. One started for
   * another block is left alone: it is cancelled once its payload is handled.
   */
  private Optional<ParallelBlockTransactionProcessor> takeStartedEarly(
      final Block block, final Optional<BlockAccessList> blockAccessList) {
    final EarlyBlockExecution execution = startedEarly.get();
    if (execution != null
        && execution.isFor(
            block.getHeader(),
            block.getBody().getTransactions(),
            blockAccessList,
            miningBeneficiaryCalculator)
        && startedEarly.compareAndSet(execution, null)) {
      return Optional.of(execution.processor());
    }
    return Optional.empty();
  }

  public static class ParallelBlockProcessorBuilder
      implements ProtocolSpecBuilder.BlockProcessorBuilder {

    final MetricsSystem metricsSystem;

    public ParallelBlockProcessorBuilder(final MetricsSystem metricsSystem) {
      this.metricsSystem = metricsSystem;
    }

    @Override
    public BlockProcessor apply(
        final MainnetTransactionProcessor transactionProcessor,
        final TransactionReceiptFactory transactionReceiptFactory,
        final Wei blockReward,
        final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
        final boolean skipZeroBlockRewards,
        final ProtocolSchedule protocolSchedule,
        final BalConfiguration balConfiguration) {
      return new MainnetParallelBlockProcessor(
          transactionProcessor,
          transactionReceiptFactory,
          blockReward,
          miningBeneficiaryCalculator,
          skipZeroBlockRewards,
          protocolSchedule,
          balConfiguration,
          metricsSystem);
    }
  }
}
