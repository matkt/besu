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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary.DefaultBinaryStateRootCommitter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.web3j.abi.FunctionEncoder;
import org.web3j.abi.datatypes.Function;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.generated.Uint256;

/**
 * Block import and processing integration tests for Amsterdam chains using the binary trie.
 *
 * <p>Genesis state is written via {@link org.hyperledger.besu.ethereum.chain.GenesisState} with
 * {@link DefaultBinaryStateRootCommitter} when Amsterdam is active at genesis.
 *
 * <p>Expected post-block state roots are computed by executing the block body (without persisting)
 * and running {@link DefaultBinaryStateRootCommitter}; the resulting root is set on the block
 * header before {@link BlockProcessor#processBlock} is called.
 */
@SuppressWarnings("rawtypes")
class BinaryBlockImportTest {

  private static final String GENESIS_RESOURCE =
      "/org/hyperledger/besu/ethereum/mainnet/genesis-bp-it.json";

  private static final String ACCOUNT_2 = "0x0000000000000000000000000000000000000002";
  private static final String ACCOUNT_3 = "0x0000000000000000000000000000000000000003";
  private static final String CONTRACT_ADDRESS = "0x00000000000000000000000000000000000fffff";

  private static final KeyPair ACCOUNT_GENESIS_1_KEYPAIR =
      generateKeyPair("c87509a1c067bbde78beb793e6fa76530b6382a4c0241e5e4a9ec0a0f44dc0d3");
  private static final KeyPair ACCOUNT_GENESIS_2_KEYPAIR =
      generateKeyPair("fc5141e75bf622179f8eedada7fab3e2e6b3e3da8eb9df4f46d84df22df7430e");

  private static final Wei COINBASE_REWARD = Wei.of(2_000_000_000_000_000L);
  private static final Wei BASE_FEE = Wei.of(7);
  private static final Hash BLOCK_REQUESTS_HASH =
      Hash.fromHexString("0x5f7606bf4b9eb2a8414aaa53f4c84062ec8789d24c604453563dc26e4ae65837");

  private ExecutionContextTestFixture fixture;
  private BonsaiWorldStateProvider stateArchive;
  private ProtocolContext protocolContext;
  private MutableBlockchain blockchain;
  private BlockHeader chainHeadHeader;
  private Hash initialStateRoot;

  @BeforeEach
  void setUp() {
    fixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_RESOURCE))
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    stateArchive = (BonsaiWorldStateProvider) fixture.getStateArchive();
    protocolContext = fixture.getProtocolContext();
    blockchain = fixture.getBlockchain();
    chainHeadHeader = blockchain.getChainHeadHeader();
    initialStateRoot = chainHeadHeader.getStateRoot();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (stateArchive != null) {
      stateArchive.close();
    }
  }

  @Test
  void processBlockWithSimpleTransfers_updatesStateRootAndBalances() {
    final Transaction transactionTransfer1 =
        createTransferTransaction(
            0, 1_000_000_000_000_000_000L, 300000L, 5L, 7L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);
    final Transaction transactionTransfer2 =
        createTransferTransaction(
            0, 2_000_000_000_000_000_000L, 300000L, 5L, 7L, ACCOUNT_3, ACCOUNT_GENESIS_2_KEYPAIR);

    final Hash expectedStateRoot =
        computeExpectedBinaryStateRoot(
            chainHeadHeader, BASE_FEE, transactionTransfer1, transactionTransfer2);
    assertThat(expectedStateRoot).isNotEqualTo(initialStateRoot);

    final MutableWorldState worldState = worldStateAtParent(chainHeadHeader);
    final Block block =
        createBlock(
            chainHeadHeader,
            expectedStateRoot,
            BASE_FEE,
            transactionTransfer1,
            transactionTransfer2);

    final BlockProcessingResult result =
        createBlockProcessor().processBlock(protocolContext, blockchain, worldState, block);

    assertTrue(result.isSuccessful(), () -> result.errorMessage.orElse("(no message)"));
    assertThat(worldState.rootHash()).isEqualTo(expectedStateRoot);
    assertThat(block.getHeader().getStateRoot()).isEqualTo(expectedStateRoot);

    final BonsaiAccount account2 =
        (BonsaiAccount) worldState.get(Address.fromHexStringStrict(ACCOUNT_2));
    final BonsaiAccount account3 =
        (BonsaiAccount) worldState.get(Address.fromHexStringStrict(ACCOUNT_3));
    assertThat(account2.getBalance()).isEqualTo(Wei.of(1_000_000_000_000_000_000L));
    assertThat(account3.getBalance()).isEqualTo(Wei.of(2_000_000_000_000_000_000L));
    assertThat(account2.getNonce()).isZero();
    assertThat(account3.getNonce()).isZero();
    assertThat(((BonsaiAccount) worldState.get(transactionTransfer1.getSender())).getNonce())
        .isEqualTo(1L);
    assertThat(((BonsaiAccount) worldState.get(transactionTransfer2.getSender())).getNonce())
        .isEqualTo(1L);
  }

  @Test
  void importBlockWithSimpleTransfers_advancesChainHeadAndStateRoot() {
    final Transaction transactionTransfer1 =
        createTransferTransaction(
            0, 1_000_000_000_000_000_000L, 300000L, 5L, 7L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR);
    final Transaction transactionTransfer2 =
        createTransferTransaction(
            0, 500_000_000_000_000_000L, 300000L, 5L, 7L, ACCOUNT_3, ACCOUNT_GENESIS_2_KEYPAIR);

    final Hash expectedStateRoot =
        computeExpectedBinaryStateRoot(
            chainHeadHeader, BASE_FEE, transactionTransfer1, transactionTransfer2);
    assertThat(expectedStateRoot).isNotEqualTo(initialStateRoot);
    final Block block =
        createBlock(
            chainHeadHeader,
            expectedStateRoot,
            BASE_FEE,
            transactionTransfer1,
            transactionTransfer2);

    final MutableWorldState worldState = worldStateAtParent(chainHeadHeader);
    final BlockProcessingResult processingResult =
        createBlockProcessor().processBlock(protocolContext, blockchain, worldState, block);
    assertTrue(
        processingResult.isSuccessful(),
        () -> processingResult.errorMessage.orElse("(no message)"));

    final BlockProcessingOutputs outputs = processingResult.getYield().orElseThrow();
    blockchain.appendBlock(block, outputs.getReceipts(), outputs.getBlockAccessList());

    try (BonsaiWorldState ignoredHeadLoad =
        (BonsaiWorldState)
            stateArchive
                .getWorldState(
                    WorldStateQueryParams.newBuilder()
                        .withParentBlockHeader(block.getHeader())
                        .withShouldWorldStateUpdateHead(true)
                        .build())
                .orElseThrow()) {
      // Mirror MainnetBlockImporter head advance after successful processing.
    }

    assertThat(blockchain.getChainHeadHash()).isEqualTo(block.getHash());
    assertThat(blockchain.getChainHeadHeader().getStateRoot()).isEqualTo(expectedStateRoot);
    assertThat(expectedStateRoot).isNotEqualTo(initialStateRoot);

    try (BonsaiWorldState headWorldState =
        (BonsaiWorldState)
            stateArchive
                .getWorldState(
                    WorldStateQueryParams.newBuilder()
                        .withParentBlockHeader(block.getHeader())
                        .withShouldWorldStateUpdateHead(false)
                        .build())
                .orElseThrow()) {
      assertThat(headWorldState.rootHash()).isEqualTo(expectedStateRoot);
      final BonsaiAccount account2 =
          (BonsaiAccount) headWorldState.get(Address.fromHexStringStrict(ACCOUNT_2));
      assertThat(account2.getBalance()).isEqualTo(Wei.of(1_000_000_000_000_000_000L));
    }

    final BlockImportResult importResult =
        fixture
            .getProtocolSchedule()
            .getByBlockHeader(block.getHeader())
            .getBlockImporter()
            .importBlock(
                protocolContext, block, HeaderValidationMode.FULL, HeaderValidationMode.FULL);
    assertThat(importResult.getStatus())
        .isEqualTo(BlockImportResult.BlockImportStatus.ALREADY_IMPORTED);
  }

  @Test
  void processBlockWithContractStorageUpdate_updatesStateRoot() {
    final Address contractAddress = Address.fromHexStringStrict(CONTRACT_ADDRESS);
    final Transaction setSlotTransaction =
        createContractUpdateSlotTransaction(
            0, contractAddress, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(42));
    final Transaction readSlotTransaction =
        createContractUpdateSlotTransaction(
            0, contractAddress, "getSlot1", ACCOUNT_GENESIS_2_KEYPAIR, Optional.empty());

    final Hash expectedStateRoot =
        computeExpectedBinaryStateRoot(
            chainHeadHeader, BASE_FEE, setSlotTransaction, readSlotTransaction);
    assertThat(expectedStateRoot).isNotEqualTo(initialStateRoot);

    final MutableWorldState worldState = worldStateAtParent(chainHeadHeader);
    final Block block =
        createBlock(
            chainHeadHeader, expectedStateRoot, BASE_FEE, setSlotTransaction, readSlotTransaction);

    final BlockProcessingResult result =
        createBlockProcessor().processBlock(protocolContext, blockchain, worldState, block);

    assertTrue(result.isSuccessful(), () -> result.errorMessage.orElse("(no message)"));
    assertThat(worldState.rootHash()).isEqualTo(expectedStateRoot);

    final BonsaiAccount contractAccount = (BonsaiAccount) worldState.get(contractAddress);
    assertThat(contractAccount.getStorageValue(UInt256.valueOf(0))).isEqualTo(UInt256.valueOf(42));
    assertThat(contractAccount.getCode().size()).isGreaterThan(0);
  }

  private AbstractBlockProcessor createBlockProcessor() {
    final ProtocolSpec protocolSpec =
        fixture
            .getProtocolSchedule()
            .getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());
    return new MainnetBlockProcessor(
        protocolSpec.getTransactionProcessor(),
        protocolSpec.getTransactionReceiptFactory(),
        COINBASE_REWARD,
        BlockHeader::getCoinbase,
        false,
        fixture.getProtocolSchedule(),
        BalConfiguration.DEFAULT);
  }

  private MutableWorldState worldStateAtParent(final BlockHeader parentHeader) {
    return stateArchive
        .getWorldState(WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(parentHeader))
        .orElseThrow();
  }

  private Hash computeExpectedBinaryStateRoot(
      final BlockHeader parentHeader, final Wei baseFee, final Transaction... transactions) {
    try (BonsaiWorldState worldState =
        (BonsaiWorldState)
            stateArchive
                .getWorldState(
                    WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(parentHeader))
                .orElseThrow()) {
      final Block draftBlock = createBlock(parentHeader, initialStateRoot, baseFee, transactions);
      final BlockProcessingResult bodyResult =
          createBlockProcessor()
              .processBlockBodyWithoutPersist(protocolContext, blockchain, worldState, draftBlock);
      assertTrue(
          bodyResult.isSuccessful(),
          () -> "Block body execution failed: " + bodyResult.errorMessage.orElse("(no message)"));

      final BonsaiWorldStateUpdateAccumulator accumulator = worldState.updater();
      return new DefaultBinaryStateRootCommitter().compute(worldState, null, accumulator).root();
    }
  }

  private Block createBlock(
      final BlockHeader parentHeader,
      final Hash stateRoot,
      final Wei baseFee,
      final Transaction... transactions) {
    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .number(parentHeader.getNumber() + 1L)
            .parentHash(parentHeader.getHash())
            .timestamp(parentHeader.getTimestamp() + 1L)
            .stateRoot(stateRoot)
            .gasLimit(30_000_000L)
            .baseFeePerGas(baseFee)
            .requestsHash(BLOCK_REQUESTS_HASH)
            .buildHeader();
    final BlockBody blockBody =
        new BlockBody(Arrays.asList(transactions), Collections.emptyList(), Optional.empty());
    return new Block(blockHeader, blockBody);
  }

  private static Transaction createTransferTransaction(
      final long nonce,
      final long value,
      final long gasLimit,
      final long maxPriorityFeePerGas,
      final long maxFeePerGas,
      final String hexAddress,
      final KeyPair keyPair) {
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .nonce(nonce)
        .maxPriorityFeePerGas(Wei.of(maxPriorityFeePerGas))
        .maxFeePerGas(Wei.of(maxFeePerGas))
        .gasLimit(gasLimit)
        .to(Address.fromHexStringStrict(hexAddress))
        .value(Wei.of(value))
        .payload(Bytes.EMPTY)
        .chainId(BigInteger.valueOf(42))
        .signAndBuild(keyPair);
  }

  private static Transaction createContractUpdateSlotTransaction(
      final int nonce,
      final Address contractAddress,
      final String methodSignature,
      final KeyPair keyPair,
      final Optional<Integer> value) {
    final Bytes payload = encodeFunctionCall(methodSignature, value);
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .nonce(nonce)
        .maxPriorityFeePerGas(Wei.of(5))
        .maxFeePerGas(Wei.of(7))
        .gasLimit(3_000_000L)
        .to(contractAddress)
        .value(Wei.ZERO)
        .payload(payload)
        .chainId(BigInteger.valueOf(42))
        .signAndBuild(keyPair);
  }

  private static Bytes encodeFunctionCall(
      final String methodSignature, final Optional<Integer> value) {
    final List<Type> inputParameters =
        value.<List<Type>>map(integer -> List.of(new Uint256(integer))).orElseGet(List::of);
    final Function function =
        new Function(methodSignature, inputParameters, Collections.emptyList());
    return Bytes.fromHexString(FunctionEncoder.encode(function));
  }

  private static KeyPair generateKeyPair(final String privateKeyHex) {
    return SignatureAlgorithmFactory.getInstance()
        .createKeyPair(
            SECPPrivateKey.create(
                Bytes32.fromHexString(privateKeyHex), SignatureAlgorithm.ALGORITHM));
  }
}
