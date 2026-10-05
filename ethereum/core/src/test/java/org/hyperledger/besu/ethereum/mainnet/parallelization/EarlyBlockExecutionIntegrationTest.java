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
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.MINING_BENEFICIARY;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Transactions run by {@link MainnetParallelBlockProcessor#startBlockExecution} before their block
 * is processed: the block processing takes their results only for the block they ran for, and the
 * result is the one of a sequential processing either way.
 */
class EarlyBlockExecutionIntegrationTest extends AbstractParallelBlockProcessorIntegrationTest {

  private static final Wei BASE_FEE = Wei.of(1);

  /** What the tests started: whatever a block did not take is cancelled once the test is done. */
  private final List<EarlyBlockExecution> started = new ArrayList<>();

  private Transaction tx1;
  private Transaction tx2;
  private Expected expected;

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
    expected = processSequentially(this::createFreshContext, tx1, tx2);
  }

  @AfterEach
  void cancelStarted() {
    started.forEach(EarlyBlockExecution::cancel);
  }

  @Test
  void blockWithAccessListTakesTheTransactionsStartedEarly() {
    final Run run = new Run(createFreshContext(), expected, tx1, tx2);
    run.startAndSubmitAll(run.block.getHeader(), Optional.of(expected.blockAccessList()));

    final BlockProcessingResult result = run.process(Optional.of(expected.blockAccessList()));

    assertThat(run.processor.hasStartedEarly()).isFalse();
    assertThat(result.getNbParallelizedTransactions()).contains(2);
  }

  @Test
  void blockWithoutAccessListTakesTheTransactionsStartedOptimistically() {
    final Run run = new Run(createFreshContext(), expected, tx1, tx2);
    run.startAndSubmitAll(run.block.getHeader(), Optional.empty());

    run.process(Optional.empty());

    // how many results are used depends on timing: one not done yet when its turn comes is dropped
    assertThat(run.processor.hasStartedEarly()).isFalse();
  }

  @Test
  void executionContextBuiltOnItsOwnIsTaken() {
    final Run run = new Run(createFreshContext(), expected, tx1, tx2);
    // as from a payload: another instance, with the same fields
    run.startAndSubmitAll(
        fromHeader(run.block.getHeader()).buildProcessableBlockHeader(),
        Optional.of(expected.blockAccessList()));

    run.process(Optional.of(expected.blockAccessList()));

    assertThat(run.processor.hasStartedEarly()).isFalse();
  }

  @Test
  void executionMissingTransactionsIsNotTaken() {
    final Run run = new Run(createFreshContext(), expected, tx1, tx2);
    final EarlyBlockExecution execution =
        run.start(run.block.getHeader(), Optional.of(expected.blockAccessList()));
    execution.submit(0, run.transactions().get(0));

    run.process(Optional.of(expected.blockAccessList()));

    assertThat(run.processor.hasStartedEarly()).isTrue();
  }

  @Test
  void blockWithOtherTransactionInstancesDoesNotTakeThem() {
    final Run run = new Run(createFreshContext(), expected, tx1, tx2);
    final EarlyBlockExecution execution =
        run.start(run.block.getHeader(), Optional.of(expected.blockAccessList()));
    execution.submit(0, run.transactions().get(0));
    // the same transaction, decoded again: not the one the block carries
    execution.submit(1, decodedAgain(run.transactions().get(1)));

    run.process(Optional.of(expected.blockAccessList()));

    assertThat(run.processor.hasStartedEarly()).isTrue();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("otherExecutionContexts")
  void blockWithAnotherExecutionContextDoesNotTakeThem(
      final String differentField, final Function<BlockHeader, BlockHeaderBuilder> otherContext) {
    final Run run = new Run(createFreshContext(), expected, tx1, tx2);
    final EarlyBlockExecution execution =
        run.startAndSubmitAll(
            otherContext.apply(run.block.getHeader()).buildProcessableBlockHeader(),
            Optional.of(expected.blockAccessList()));

    run.process(Optional.of(expected.blockAccessList()));

    assertThat(run.processor.hasStartedEarly()).isTrue();
    execution.cancel();
    assertThat(run.processor.hasStartedEarly()).isFalse();
  }

  static Stream<Arguments> otherExecutionContexts() {
    return Stream.of(
        otherContext("parent hash", header -> fromHeader(header).parentHash(Hash.ZERO)),
        otherContext("coinbase", header -> fromHeader(header).coinbase(Address.ZERO)),
        otherContext("number", header -> fromHeader(header).number(header.getNumber() + 1)),
        otherContext("gas limit", header -> fromHeader(header).gasLimit(header.getGasLimit() + 1)),
        otherContext(
            "timestamp", header -> fromHeader(header).timestamp(header.getTimestamp() + 1)),
        otherContext("base fee", header -> fromHeader(header).baseFee(BASE_FEE.add(Wei.ONE))),
        otherContext(
            "prevRandao", header -> fromHeader(header).prevRandao(Bytes32.repeat((byte) 1))),
        otherContext(
            "parent beacon block root",
            header -> fromHeader(header).parentBeaconBlockRoot(Bytes32.repeat((byte) 1))));
  }

  private static Arguments otherContext(
      final String differentField, final Function<BlockHeader, BlockHeaderBuilder> otherContext) {
    return Arguments.of(differentField, otherContext);
  }

  private static BlockHeaderBuilder fromHeader(final BlockHeader header) {
    return BlockHeaderBuilder.fromHeader(header);
  }

  @Test
  void blockWithAnotherAccessListInstanceDoesNotTakeThem() {
    final Run run = new Run(createFreshContext(), expected, tx1, tx2);
    run.startAndSubmitAll(run.block.getHeader(), Optional.of(expected.blockAccessList()));

    run.process(Optional.of(new BlockAccessList(expected.blockAccessList().accountChanges())));

    assertThat(run.processor.hasStartedEarly()).isTrue();
  }

  @Test
  void cancelledExecutionIsNotTakenAndRunsNothingMore() {
    final Run run = new Run(createFreshContext(), expected, tx1, tx2);
    final EarlyBlockExecution execution =
        run.start(run.block.getHeader(), Optional.of(expected.blockAccessList()));
    execution.cancel();
    // e.g. the payload turned out to be invalid while its transactions were being decoded
    execution.submit(0, run.transactions().get(0));

    run.process(Optional.of(expected.blockAccessList()));

    assertThat(run.processor.hasStartedEarly()).isFalse();
    assertThat(execution.isCancelled()).isTrue();
  }

  @Test
  void ancestorsTheTransactionsStartedEarlyReadAreAccessedByTheBlock() {
    // block 2 reads the hash of block 0: an ancestor deeper than the parent, which every block
    // hash lookup knows from the start
    final Transaction readsHashOfBlockZero = readingTheHashOfBlockZero();
    final Expected expectedAtBlockTwo =
        processSequentially(this::freshChainAtBlockOne, readsHashOfBlockZero);
    assertThat(expectedAtBlockTwo.accessedAncestors()).containsKey(0L);
    final Run run = new Run(freshChainAtBlockOne(), expectedAtBlockTwo, readsHashOfBlockZero);
    run.startAndSubmitAll(run.block.getHeader(), Optional.of(expectedAtBlockTwo.blockAccessList()));

    final BlockProcessingResult result =
        run.process(Optional.of(expectedAtBlockTwo.blockAccessList()));

    assertThat(run.processor.hasStartedEarly()).isFalse();
    assertThat(result.getYield().orElseThrow().getAccessedAncestors())
        .isEqualTo(expectedAtBlockTwo.accessedAncestors());
  }

  /** What processing the block sequentially yields: the parallel processing must yield it too. */
  private record Expected(
      Hash stateRoot,
      List<TransactionReceipt> receipts,
      BlockAccessList blockAccessList,
      Map<Long, Hash> accessedAncestors) {}

  /** Processes the block of {@code transactions} on top of the head of a fresh {@code chain}. */
  private Expected processSequentially(
      final Supplier<ExecutionContextTestFixture> chain, final Transaction... transactions) {
    final ExecutionContextTestFixture discovery = chain.get();
    final Hash stateRoot =
        discoverStateRootAtParent(
            discovery,
            discovery.getBlockchain().getChainHeadHeader(),
            BASE_FEE,
            MINING_BENEFICIARY,
            transactions);
    final ExecutionContextTestFixture ctx = chain.get();
    final MutableWorldState worldState = ctx.getStateArchive().getWorldState();
    final BlockProcessingResult result =
        createSequentialProcessor(ctx)
            .processBlock(
                ctx.getProtocolContext(),
                ctx.getBlockchain(),
                worldState,
                createBlock(ctx, stateRoot, BASE_FEE, transactions));
    assertThat(result.isSuccessful()).as(result.errorMessage.orElse("(no message)")).isTrue();
    assertThat(worldState.rootHash()).isEqualTo(stateRoot);
    final BlockProcessingOutputs outputs = result.getYield().orElseThrow();
    return new Expected(
        stateRoot,
        outputs.getReceipts(),
        outputs.getBlockAccessList().orElseThrow(),
        Map.copyOf(outputs.getAccessedAncestors()));
  }

  /**
   * {@inheritDoc} The requests hash it is given is the one of the blocks on top of genesis: there
   * are no requests on top of block 1.
   */
  @Override
  protected Block createBlock(
      final ExecutionContextTestFixture ctx,
      final BlockHeader parentHeader,
      final Hash stateRoot,
      final Wei baseFee,
      final Address coinbase,
      final Transaction... txs) {
    final Block block = super.createBlock(ctx, parentHeader, stateRoot, baseFee, coinbase, txs);
    if (parentHeader.getNumber() == 0) {
      return block;
    }
    return new Block(
        fromHeader(block.getHeader())
            .requestsHash(BodyValidation.requestsHash(List.of()))
            .blockHeaderFunctions(new MainnetBlockHeaderFunctions())
            .buildBlockHeader(),
        block.getBody());
  }

  /** A fresh chain whose head, world state included, is an empty block 1. */
  private ExecutionContextTestFixture freshChainAtBlockOne() {
    final ExecutionContextTestFixture ctx = createFreshContext();
    final Block emptyBlock = createBlock(ctx, discoverStateRoot(BASE_FEE), BASE_FEE);
    final MutableWorldState worldState = ctx.getStateArchive().getWorldState();
    final BlockProcessingResult result =
        createSequentialProcessor(ctx)
            .processBlock(ctx.getProtocolContext(), ctx.getBlockchain(), worldState, emptyBlock);
    assertThat(result.isSuccessful()).as(result.errorMessage.orElse("(no message)")).isTrue();
    worldState.persist(emptyBlock.getHeader());
    ctx.getBlockchain()
        .appendBlock(
            emptyBlock, result.getYield().orElseThrow().getReceipts(), getBlockAccessList(result));
    return ctx;
  }

  /** Creates a contract whose init code reads the hash of block 0: PUSH1 0, BLOCKHASH, POP. */
  private static Transaction readingTheHashOfBlockZero() {
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .nonce(0)
        .maxPriorityFeePerGas(Wei.of(2))
        .maxFeePerGas(Wei.of(10))
        .gasLimit(300_000L)
        .value(Wei.ZERO)
        .payload(Bytes.fromHexString("0x6000405000"))
        .chainId(BigInteger.valueOf(42))
        .signAndBuild(ACCOUNT_GENESIS_1_KEYPAIR);
  }

  private static Transaction decodedAgain(final Transaction transaction) {
    return TransactionDecoder.decodeOpaqueBytes(
        TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY),
        EncodingContext.BLOCK_BODY);
  }

  /** A block processed by a parallel block processor, on top of the head of its chain. */
  private final class Run {
    final ExecutionContextTestFixture ctx;
    final Expected expected;
    final Block block;
    final MainnetParallelBlockProcessor processor;

    Run(
        final ExecutionContextTestFixture ctx,
        final Expected expected,
        final Transaction... transactions) {
      this.ctx = ctx;
      this.expected = expected;
      this.block = createBlock(ctx, expected.stateRoot(), BASE_FEE, transactions);
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
      final EarlyBlockExecution execution =
          processor
              .startBlockExecution(
                  ctx.getProtocolContext(),
                  ctx.getBlockchain().getChainHeadHeader(),
                  executionContext,
                  maybeBlockAccessList,
                  transactions().size())
              .orElseThrow();
      started.add(execution);
      return execution;
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

    /** Processes the block, which must yield what processing it sequentially yields. */
    BlockProcessingResult process(final Optional<BlockAccessList> maybeBlockAccessList) {
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
      assertThat(worldState.rootHash()).isEqualTo(expected.stateRoot());
      assertThat(result.getYield().orElseThrow().getReceipts()).isEqualTo(expected.receipts());
      return result;
    }
  }
}
