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
package org.hyperledger.besu.evm.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.frame.MessageFrame.State.CODE_EXECUTING;
import static org.hyperledger.besu.evm.frame.MessageFrame.State.COMPLETED_SUCCESS;
import static org.hyperledger.besu.evm.frame.MessageFrame.State.EXCEPTIONAL_HALT;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.contractvalidation.MaxCodeSizeRule;
import org.hyperledger.besu.evm.contractvalidation.PrefixCodeRule;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.log.TransferLogEmitter;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.toy.ToyWorld;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Collections;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@Nested
@ExtendWith(MockitoExtension.class)
class ContractCreationProcessorTest
    extends AbstractMessageProcessorTest<ContractCreationProcessor> {

  EVM evm = MainnetEVMs.futureEips(EvmConfiguration.DEFAULT);

  private ContractCreationProcessor processor;

  @Test
  void shouldThrowAnExceptionWhenCodeContractFormatInvalidPreEOF() {
    processor =
        new ContractCreationProcessor(evm, true, Collections.singletonList(PrefixCodeRule.of()), 1);
    final Bytes contractCode = Bytes.fromHexString("EF01010101010101");
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(10600L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    // EIP-8037: validation failures use EXCEPTIONAL_HALT so handleStateGasHalt refunds
    // execution state gas to the reservoir; gasRemaining is cleared by exceptionalHalt() in
    // process(), not codeSuccess() in isolation.
    assertThat(messageFrame.getState()).isEqualTo(EXCEPTIONAL_HALT);
  }

  @Test
  void shouldNotThrowAnExceptionWhenCodeContractIsValid() {
    processor =
        new ContractCreationProcessor(evm, true, Collections.singletonList(PrefixCodeRule.of()), 1);
    final Bytes contractCode = Bytes.fromHexString("0101010101010101");
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(10600L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    assertThat(messageFrame.getState()).isEqualTo(COMPLETED_SUCCESS);
  }

  @Test
  void shouldNotThrowAnExceptionWhenPrefixCodeRuleNotAdded() {
    processor = new ContractCreationProcessor(evm, true, Collections.emptyList(), 1);
    final Bytes contractCode = Bytes.fromHexString("0F01010101010101");
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(10600L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    assertThat(messageFrame.getState()).isEqualTo(COMPLETED_SUCCESS);
  }

  @Test
  void shouldThrowAnExceptionWhenCodeContractTooLarge() {
    processor =
        new ContractCreationProcessor(
            evm,
            true,
            Collections.singletonList(
                MaxCodeSizeRule.from(EvmSpecVersion.SPURIOUS_DRAGON, EvmConfiguration.DEFAULT)),
            1);
    final Bytes contractCode =
        Bytes.fromHexString("00".repeat(EvmSpecVersion.SPURIOUS_DRAGON.getMaxCodeSize() + 1));
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(10_000_000L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    // EIP-8037: validation failures use EXCEPTIONAL_HALT so handleStateGasHalt refunds
    // execution state gas to the reservoir; gasRemaining is cleared by exceptionalHalt() in
    // process(), not codeSuccess() in isolation.
    assertThat(messageFrame.getState()).isEqualTo(EXCEPTIONAL_HALT);
  }

  @Test
  void shouldNotThrowAnExceptionWhenCodeContractTooLarge() {
    processor =
        new ContractCreationProcessor(
            evm,
            true,
            Collections.singletonList(
                MaxCodeSizeRule.from(EvmSpecVersion.SPURIOUS_DRAGON, EvmConfiguration.DEFAULT)),
            1);
    final Bytes contractCode =
        Bytes.fromHexString("00".repeat(EvmSpecVersion.SPURIOUS_DRAGON.getMaxCodeSize()));
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(5_000_000L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    assertThat(messageFrame.getState()).isEqualTo(COMPLETED_SUCCESS);
  }

  @Test
  void shouldRejectDeployedCodeAboveAmsterdamLimit() {
    processor =
        new ContractCreationProcessor(
            evm,
            true,
            Collections.singletonList(
                MaxCodeSizeRule.from(EvmSpecVersion.AMSTERDAM, EvmConfiguration.DEFAULT)),
            1);
    final Bytes contractCode =
        Bytes.fromHexString("00".repeat(EvmSpecVersion.AMSTERDAM.getMaxCodeSize() + 1));
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(10_000_000L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    // EIP-8037: validation failures use EXCEPTIONAL_HALT so handleStateGasHalt refunds
    // execution state gas to the reservoir; gasRemaining is cleared by exceptionalHalt() in
    // process(), not codeSuccess() in isolation.
    assertThat(messageFrame.getState()).isEqualTo(EXCEPTIONAL_HALT);
  }

  @Test
  void shouldAcceptDeployedCodeAtAmsterdamLimit() {
    processor =
        new ContractCreationProcessor(
            evm,
            true,
            Collections.singletonList(
                MaxCodeSizeRule.from(EvmSpecVersion.AMSTERDAM, EvmConfiguration.DEFAULT)),
            1);
    final Bytes contractCode =
        Bytes.fromHexString("00".repeat(EvmSpecVersion.AMSTERDAM.getMaxCodeSize()));
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    // EIP-7954: 64KiB code deposit costs 200 * 0x10000 = 13_107_200 regular gas.
    messageFrame.setGasRemaining(15_000_000L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    assertThat(messageFrame.getState()).isEqualTo(COMPLETED_SUCCESS);
  }

  @Test
  void shouldAcceptDeployedCodeBetweenOldAndNewAmsterdamLimit() {
    processor =
        new ContractCreationProcessor(
            evm,
            true,
            Collections.singletonList(
                MaxCodeSizeRule.from(EvmSpecVersion.AMSTERDAM, EvmConfiguration.DEFAULT)),
            1);
    final Bytes contractCode = Bytes.fromHexString("00".repeat(0x6001));
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(10_000_000L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    assertThat(messageFrame.getState()).isEqualTo(COMPLETED_SUCCESS);
  }

  @Test
  void shouldNotThrowAnExceptionWhenCodeSizeRuleNotAdded() {
    processor = new ContractCreationProcessor(evm, true, Collections.emptyList(), 1);
    final Bytes contractCode = Bytes.fromHexString("00".repeat(24 * 1024 + 1));
    final MessageFrame messageFrame = new TestMessageFrameBuilder().build();
    messageFrame.setOutputData(contractCode);
    messageFrame.setGasRemaining(5_000_000L);

    processor.codeSuccess(messageFrame, OperationTracer.NO_TRACING);
    assertThat(messageFrame.getState()).isEqualTo(COMPLETED_SUCCESS);
  }

  @Test
  void shouldHaltWhenTargetAccountHasNonEmptyStorageAndCheckEnabled() {
    // Default behavior (checkStorageEmptyOnCreate = true): an account with non-empty storage,
    // nonce 0, and empty code is considered to already exist, so CREATE halts with
    // ILLEGAL_STATE_CHANGE.
    processor = new ContractCreationProcessor(evm, true, Collections.emptyList(), 1);
    final Address contractAddress = Address.fromHexString("0xabc");
    final Address senderAddress = Address.fromHexString("0xdef");

    final ToyWorld world = new ToyWorld();
    world.createAccount(null, senderAddress, 0, Wei.of(100), Bytes.EMPTY);
    final MutableAccount contract =
        world.createAccount(null, contractAddress, 0, Wei.ZERO, Bytes.EMPTY);
    contract.setStorageValue(UInt256.ZERO, UInt256.ONE); // non-empty storage, nonce 0, empty code

    final MessageFrame messageFrame =
        new TestMessageFrameBuilder()
            .worldUpdater(world)
            .sender(senderAddress)
            .contract(contractAddress)
            .build();

    processor.start(messageFrame, OperationTracer.NO_TRACING);

    assertThat(messageFrame.getState()).isEqualTo(EXCEPTIONAL_HALT);
    assertThat(messageFrame.getExceptionalHaltReason())
        .contains(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
  }

  @Test
  void shouldProceedWhenTargetAccountHasNonEmptyStorageAndCheckDisabled() {
    // binaryTrie behavior (checkStorageEmptyOnCreate = false): an account with non-empty storage
    // but nonce 0 and empty code is NOT considered to already exist, so CREATE proceeds.
    processor =
        new ContractCreationProcessor(
            evm,
            true,
            Collections.emptyList(),
            1,
            Collections.emptySet(),
            TransferLogEmitter.NOOP,
            false);
    final Address contractAddress = Address.fromHexString("0xabc");
    final Address senderAddress = Address.fromHexString("0xdef");

    final ToyWorld world = new ToyWorld();
    world.createAccount(null, senderAddress, 0, Wei.of(100), Bytes.EMPTY);
    final MutableAccount contract =
        world.createAccount(null, contractAddress, 0, Wei.ZERO, Bytes.EMPTY);
    contract.setStorageValue(UInt256.ZERO, UInt256.ONE); // non-empty storage, nonce 0, empty code

    final MessageFrame messageFrame =
        new TestMessageFrameBuilder()
            .worldUpdater(world)
            .sender(senderAddress)
            .contract(contractAddress)
            .build();

    processor.start(messageFrame, OperationTracer.NO_TRACING);

    assertThat(messageFrame.getState()).isEqualTo(CODE_EXECUTING);
    assertThat(messageFrame.getExceptionalHaltReason()).isEmpty();
  }

  @Override
  protected ContractCreationProcessor getAbstractMessageProcessor() {
    return new ContractCreationProcessor(evm, true, Collections.emptyList(), 1);
  }
}
