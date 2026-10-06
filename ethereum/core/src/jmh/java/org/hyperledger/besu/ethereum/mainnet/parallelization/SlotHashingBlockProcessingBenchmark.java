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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Util;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Processes a block on Bonsai where every transaction reads the same hot slots and writes slots of
 * its own, before and after Amsterdam, sequentially and in parallel. Besides the time per block, it
 * prints the keccak256 computed per block, counted on every thread by {@link KeccakCounter}.
 *
 * <p>Run with {@code ./gradlew :ethereum:core:jmh -Pincludes=SlotHashingBlockProcessingBenchmark}.
 */
@State(Scope.Benchmark)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Djdk.attach.allowAttachSelf=true", "-XX:+EnableDynamicAgentLoading"})
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SlotHashingBlockProcessingBenchmark {

  private static final String GENESIS =
      "/org/hyperledger/besu/ethereum/mainnet/parallelization/slot-hashing-genesis.json";
  private static final Address CONTRACT =
      Address.fromHexString("0x00000000000000000000000000000000005107");
  private static final Address COINBASE =
      Address.fromHexString("0x000000000000000000000000000000000000c0b5");

  /** Reads slots [0, word 2) then sets slots [word 0, word 0 + word 1) to 1. */
  private static final String CONTRACT_CODE =
      "0x60005b8060403511156015578054506001016002565b5060005b80602035111560315760018160003501556001016019565b00";

  private static final long WRITE_SLOTS_BASE = 1_000_000L;
  private static final Wei BASE_FEE = Wei.of(5);

  /** Fork and execution path. */
  public enum Scenario {
    OSAKA_SEQUENTIAL(false, false, false),
    OSAKA_PARALLEL(false, true, false),
    AMSTERDAM_SEQUENTIAL(true, false, false),
    AMSTERDAM_PARALLEL(true, true, false),
    AMSTERDAM_BAL_PARALLEL(true, true, true);

    final boolean amsterdam;
    final boolean parallel;
    final boolean withBal;

    Scenario(final boolean amsterdam, final boolean parallel, final boolean withBal) {
      this.amsterdam = amsterdam;
      this.parallel = parallel;
      this.withBal = withBal;
    }
  }

  @Param({
    "OSAKA_SEQUENTIAL",
    "OSAKA_PARALLEL",
    "AMSTERDAM_SEQUENTIAL",
    "AMSTERDAM_PARALLEL",
    "AMSTERDAM_BAL_PARALLEL"
  })
  public Scenario scenario;

  /** Transactions per block, each from its own sender. */
  @Param({"64"})
  public int transactions;

  /** Hot slots read by every transaction. */
  @Param({"64"})
  public int sharedReadSlots;

  /** Slots written by each transaction, different from one transaction to another. */
  @Param({"16"})
  public int ownWriteSlots;

  private ExecutionContextTestFixture context;
  private BlockProcessor processor;
  private Block block;
  private Optional<BlockAccessList> blockAccessList;
  private BlockHeader genesisHeader;
  private MutableWorldState worldState;

  private long blocks;
  private long keccak;
  private long wordKeccak;

  @Setup(Level.Trial)
  public void setUp() {
    KeccakCounter.install();
    final List<KeyPair> senders = new ArrayList<>(transactions);
    for (int i = 0; i < transactions; i++) {
      senders.add(
          SignatureAlgorithmFactory.getInstance()
              .createKeyPair(
                  SECPPrivateKey.create(
                      UInt256.valueOf(i + 1L).toBytes(), SignatureAlgorithm.ALGORITHM)));
    }
    context =
        ExecutionContextTestFixture.builder(GenesisConfig.fromConfig(genesisJson(senders)))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    genesisHeader = context.getBlockchain().getChainHeadHeader();
    final ProtocolSpec spec =
        context
            .getProtocolSchedule()
            .getByBlockHeader(new BlockHeaderTestFixture().number(1L).buildHeader());

    final List<Transaction> txs = new ArrayList<>(transactions);
    for (int i = 0; i < transactions; i++) {
      final Bytes callData =
          Bytes.concatenate(
              UInt256.valueOf(WRITE_SLOTS_BASE + (long) i * ownWriteSlots),
              UInt256.valueOf(ownWriteSlots),
              UInt256.valueOf(sharedReadSlots));
      txs.add(
          Transaction.builder()
              .type(TransactionType.EIP1559)
              .nonce(0)
              .maxPriorityFeePerGas(Wei.ZERO)
              .maxFeePerGas(Wei.of(10))
              .gasLimit(5_000_000L)
              .to(CONTRACT)
              .value(Wei.ZERO)
              .payload(callData)
              .chainId(BigInteger.valueOf(42))
              .signAndBuild(senders.get(i)));
    }

    final BlockProcessor sequential =
        new MainnetBlockProcessor(
            spec.getTransactionProcessor(),
            spec.getTransactionReceiptFactory(),
            Wei.ZERO,
            BlockHeader::getCoinbase,
            true,
            context.getProtocolSchedule(),
            BalConfiguration.DEFAULT);
    block = discoverBlock(sequential, txs);
    final BlockProcessingResult reference = process(sequential, block, Optional.empty());
    if (!reference.isSuccessful()) {
      throw new IllegalStateException("block does not process: " + reference.errorMessage);
    }
    blockAccessList =
        scenario.withBal
            ? reference.getYield().flatMap(BlockProcessingOutputs::getBlockAccessList)
            : Optional.empty();
    if (scenario.withBal && blockAccessList.isEmpty()) {
      throw new IllegalStateException("no block access list for " + scenario);
    }
    processor =
        scenario.parallel
            ? new MainnetParallelBlockProcessor(
                spec.getTransactionProcessor(),
                spec.getTransactionReceiptFactory(),
                Wei.ZERO,
                BlockHeader::getCoinbase,
                true,
                context.getProtocolSchedule(),
                BalConfiguration.DEFAULT,
                new NoOpMetricsSystem())
            : sequential;
    final BlockProcessingResult check = process(processor, block, blockAccessList);
    if (!check.isSuccessful()) {
      throw new IllegalStateException(scenario + " fails: " + check.errorMessage);
    }
  }

  @Setup(Level.Invocation)
  public void freshWorldState() {
    worldState = parentWorldState();
  }

  @TearDown(Level.Invocation)
  public void closeWorldState() {
    ((BonsaiWorldState) worldState).close();
  }

  @Benchmark
  public BlockProcessingResult processBlock() {
    final long allBefore = KeccakCounter.ALL.sum();
    final long wordBefore = KeccakCounter.WORD_INPUTS.sum();
    final BlockProcessingResult result =
        processor.processBlock(
            context.getProtocolContext(),
            context.getBlockchain(),
            worldState,
            block,
            blockAccessList);
    keccak += KeccakCounter.ALL.sum() - allBefore;
    wordKeccak += KeccakCounter.WORD_INPUTS.sum() - wordBefore;
    blocks++;
    return result;
  }

  @TearDown(Level.Trial)
  public void printKeccakPerBlock() {
    System.out.printf(
        "%n%s: %,d keccak256 per block, %,d of them on 32-byte inputs (mostly slot keys)%n",
        scenario, keccak / blocks, wordKeccak / blocks);
  }

  private BlockProcessingResult process(
      final BlockProcessor blockProcessor,
      final Block toProcess,
      final Optional<BlockAccessList> bal) {
    final MutableWorldState ws = parentWorldState();
    try {
      return blockProcessor.processBlock(
          context.getProtocolContext(), context.getBlockchain(), ws, toProcess, bal);
    } finally {
      ((BonsaiWorldState) ws).close();
    }
  }

  private MutableWorldState parentWorldState() {
    return context
        .getStateArchive()
        .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(genesisHeader))
        .orElseThrow();
  }

  /** Finds the state root and requests hash of the block from the mismatch errors. */
  private Block discoverBlock(final BlockProcessor sequential, final List<Transaction> txs) {
    Hash stateRoot = Hash.ZERO;
    // requests hash of this genesis' predeploys, so that only the state root has to be found
    Hash requestsHash =
        Hash.fromHexString("0x5f7606bf4b9eb2a8414aaa53f4c84062ec8789d24c604453563dc26e4ae65837");
    for (int attempt = 0; attempt < 3; attempt++) {
      final Block candidate = createBlock(stateRoot, requestsHash, txs);
      final BlockProcessingResult result = process(sequential, candidate, Optional.empty());
      if (result.isSuccessful()) {
        return candidate;
      }
      final String error = result.errorMessage.orElse("");
      final String requestsMarker = "Requests hash mismatch, calculated: ";
      if (error.startsWith(requestsMarker)) {
        final String calculated = error.substring(requestsMarker.length());
        requestsHash = Hash.fromHexString(calculated.substring(0, calculated.indexOf(' ')));
      } else if (error.contains("calculated ")) {
        stateRoot = Hash.fromHexString(error.substring(error.lastIndexOf(' ') + 1));
      } else {
        throw new IllegalStateException("cannot build the block: " + error);
      }
    }
    throw new IllegalStateException("cannot build the block");
  }

  private Block createBlock(
      final Hash stateRoot, final Hash requestsHash, final List<Transaction> txs) {
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .number(1L)
            .parentHash(genesisHeader.getHash())
            .coinbase(COINBASE)
            .stateRoot(stateRoot)
            .gasLimit(1_000_000_000L)
            .baseFeePerGas(BASE_FEE)
            .requestsHash(requestsHash)
            .buildHeader();
    return new Block(header, new BlockBody(txs, Collections.emptyList(), Optional.empty()));
  }

  /** The base genesis plus the funded senders and the contract with its hot slots set. */
  private String genesisJson(final List<KeyPair> senders) {
    final StringBuilder alloc = new StringBuilder();
    for (final KeyPair sender : senders) {
      alloc
          .append('"')
          .append(Util.publicKeyToAddress(sender.getPublicKey()).toHexString())
          .append("\": {\"balance\": \"0xde0b6b3a7640000\"},");
    }
    final StringBuilder storage = new StringBuilder();
    for (int slot = 0; slot < sharedReadSlots; slot++) {
      storage
          .append(slot == 0 ? "" : ",")
          .append('"')
          .append(Bytes32.leftPad(Bytes.ofUnsignedLong(slot)).toHexString())
          .append("\": \"0x01\"");
    }
    alloc
        .append('"')
        .append(CONTRACT.toHexString())
        .append("\": {\"balance\": \"0x0\", \"code\": \"")
        .append(CONTRACT_CODE)
        .append("\", \"storage\": {")
        .append(storage)
        .append("}},");
    String json = readGenesis().replace("\"alloc\": {", "\"alloc\": {" + alloc);
    if (!scenario.amsterdam) {
      json = json.replace("\"amsterdamTime\": 0,", "");
    }
    return json;
  }

  private static String readGenesis() {
    try (InputStream in = SlotHashingBlockProcessingBenchmark.class.getResourceAsStream(GENESIS)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
