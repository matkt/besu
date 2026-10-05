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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_2;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_3;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2_KEYPAIR;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Transactions run by {@link MainnetParallelBlockProcessor#startBlockExecution} before their block
 * is processed: the block processing takes their results only for the block they ran for, and the
 * result is the one of a sequential processing either way.
 */
class EarlyBlockExecutionIntegrationTest extends AbstractParallelBlockProcessorIntegrationTest {

  private static final Wei BASE_FEE = Wei.of(1);

  private Transaction tx1;
  private Transaction tx2;
  private Hash expectedStateRoot;
  private BlockAccessList blockAccessList;

  @Override
  protected String getVariantName() {
    return "early";
  }

  @Override
  protected ParallelTransactionPreprocessing createParallelPreprocessing(
      final MainnetTransactionProcessor transactionProcessor) {
    return new ParallelTransactionPreprocessing(
        transactionProcessor, Runnable::run, BalConfiguration.DEFAULT);
  }

  @BeforeEach
  void processSequentially() {
    tx1 =
        createTransferTransaction(
            0, 1_000_000_000_000_000_000L, 300_000L, 2L, 10L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);
    tx2 =
        createTransferTransaction(
            0, 2_000_000_000_000_000_000L, 300_000L, 3L, 10L, ACCOUNT_3, ACCOUNT_GENESIS_2_KEYPAIR);
    final Hash stateRoot = discoverStateRoot(BASE_FEE, tx1, tx2);
    final ExecutionContextTestFixture ctx = createFreshContext();
    final MutableWorldState worldState = ctx.getStateArchive().getWorldState();
    final BlockProcessingResult result =
        createSequentialProcessor(ctx)
            .processBlock(
                ctx.getProtocolContext(),
                ctx.getBlockchain(),
                worldState,
                createBlock(ctx, stateRoot, BASE_FEE, tx1, tx2));
    assertThat(result.isSuccessful()).isTrue();
    expectedStateRoot = worldState.rootHash();
    blockAccessList = getBlockAccessList(result).orElseThrow();
  }

  @Test
  void blockWithAccessListTakesTheTransactionsStartedEarly() {
    final Run run = new Run();
    run.startAndSubmitAll(run.block.getHeader(), Optional.of(blockAccessList));

    run.process(Optional.of(blockAccessList));

    assertThat(run.processor.hasStartedEarly()).isFalse();
  }

  @Test
  void blockWithoutAccessListTakesTheTransactionsStartedOptimistically() {
    final Run run = new Run();
    run.startAndSubmitAll(run.block.getHeader(), Optional.empty());

    run.process(Optional.empty());

    assertThat(run.processor.hasStartedEarly()).isFalse();
  }

  @Test
  void transactionsNotHandedOverAreRunByTheBlockProcessing() {
    final Run run = new Run();
    final EarlyBlockExecution execution =
        run.start(run.block.getHeader(), Optional.of(blockAccessList));
    execution.submit(0, run.transactions().get(0));

    run.process(Optional.of(blockAccessList));

    assertThat(run.processor.hasStartedEarly()).isFalse();
  }

  @Test
  void blockWithAnotherExecutionContextDoesNotTakeThem() {
    final Run run = new Run();
    final ProcessableBlockHeader otherTimestamp =
        BlockHeaderBuilder.fromHeader(run.block.getHeader())
            .timestamp(run.block.getHeader().getTimestamp() + 1)
            .buildProcessableBlockHeader();
    final EarlyBlockExecution execution =
        run.startAndSubmitAll(otherTimestamp, Optional.of(blockAccessList));

    run.process(Optional.of(blockAccessList));

    assertThat(run.processor.hasStartedEarly()).isTrue();
    execution.cancel();
    assertThat(run.processor.hasStartedEarly()).isFalse();
  }

  @Test
  void blockWithAnotherAccessListInstanceDoesNotTakeThem() {
    final Run run = new Run();
    run.startAndSubmitAll(run.block.getHeader(), Optional.of(blockAccessList));

    run.process(Optional.of(new BlockAccessList(blockAccessList.accountChanges())));

    assertThat(run.processor.hasStartedEarly()).isTrue();
  }

  @Test
  void cancelledExecutionIsNotTakenAndRunsNothingMore() {
    final Run run = new Run();
    final EarlyBlockExecution execution =
        run.start(run.block.getHeader(), Optional.of(blockAccessList));
    execution.cancel();
    // e.g. the payload turned out to be invalid while its transactions were being decoded
    execution.submit(0, run.transactions().get(0));

    run.process(Optional.of(blockAccessList));

    assertThat(run.processor.hasStartedEarly()).isFalse();
    assertThat(execution.isCancelled()).isTrue();
  }

  /** A block processed on a fresh chain by a parallel block processor. */
  private final class Run {
    final ExecutionContextTestFixture ctx = createFreshContext();
    final Block block = createBlock(ctx, expectedStateRoot, BASE_FEE, tx1, tx2);
    final MainnetParallelBlockProcessor processor;

    Run() {
      final ProtocolSpec spec =
          ctx.getProtocolSchedule()
              .getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());
      processor =
          new MainnetParallelBlockProcessor(
              spec.getTransactionProcessor(),
              spec.getTransactionReceiptFactory(),
              Wei.ZERO,
              BlockHeader::getCoinbase,
              true,
              ctx.getProtocolSchedule(),
              BalConfiguration.DEFAULT,
              new NoOpMetricsSystem());
    }

    List<Transaction> transactions() {
      return block.getBody().getTransactions();
    }

    EarlyBlockExecution start(
        final ProcessableBlockHeader executionContext,
        final Optional<BlockAccessList> maybeBlockAccessList) {
      return processor
          .startBlockExecution(
              ctx.getProtocolContext(),
              ctx.getBlockchain().getChainHeadHeader(),
              executionContext,
              maybeBlockAccessList,
              transactions().size())
          .orElseThrow();
    }

    EarlyBlockExecution startAndSubmitAll(
        final ProcessableBlockHeader executionContext,
        final Optional<BlockAccessList> maybeBlockAccessList) {
      final EarlyBlockExecution execution = start(executionContext, maybeBlockAccessList);
      for (int i = 0; i < transactions().size(); i++) {
        execution.submit(i, transactions().get(i));
      }
      return execution;
    }

    void process(final Optional<BlockAccessList> maybeBlockAccessList) {
      final MutableWorldState worldState = ctx.getStateArchive().getWorldState();
      final BlockProcessor blockProcessor = processor;
      final BlockProcessingResult result =
          blockProcessor.processBlock(
              ctx.getProtocolContext(),
              ctx.getBlockchain(),
              worldState,
              block,
              maybeBlockAccessList);
      assertThat(result.isSuccessful()).as(result.errorMessage.orElse("(no message)")).isTrue();
      assertThat(worldState.rootHash()).isEqualTo(expectedStateRoot);
    }
  }
}
