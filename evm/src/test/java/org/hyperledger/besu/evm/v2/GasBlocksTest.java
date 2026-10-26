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
package org.hyperledger.besu.evm.v2;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.v2.operation.OperationsV2;
import org.hyperledger.besu.evm.v2.testutils.TestMessageFrameBuilderV2;

import java.math.BigInteger;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class GasBlocksTest {

  private static final EVM OSAKA = MainnetEVMs.osaka(BigInteger.ONE, ProgramRun.V2_BLOCK_GAS);
  private static final int[] COSTS = costs(OSAKA);

  @Test
  void blockEndsAtAnOperationThatIsNotInBlocks() {
    // PUSH1 5, PUSH1 3, ADD, PUSH1 0x20, JUMP, PUSH1 1
    final GasBlocks blocks = analyse("0x600560030160205660015b");

    assertThat(blocks.entry(0)).isEqualTo(entry(12, 7));
    assertThat(blocks.entry(7)).isZero();
    assertThat(blocks.entry(8)).isEqualTo(entry(3, 2));
  }

  @Test
  void jumpDestStartsABlock() {
    // PUSH1 1, JUMPDEST, PUSH1 2, ADD
    final GasBlocks blocks = analyse("0x60015b600201");

    assertThat(blocks.entry(0)).isEqualTo(entry(3, 2));
    assertThat(blocks.entry(2)).isEqualTo(entry(7, 4));
  }

  @Test
  void blocksAreFoundAcrossChunksOfCode() {
    // PUSH1 1, JUMP, 100 times: a block of PUSH1 every 3 bytes, over 300 bytes of code
    final GasBlocks blocks = analyse("0x" + "600156".repeat(100));

    for (int pc = 0; pc < 300; pc++) {
      assertThat(blocks.entry(pc)).as("pc %d", pc).isEqualTo(pc % 3 == 0 ? entry(3, 2) : 0);
    }
  }

  @Test
  void pushDataIsNotAnOperation() {
    // PUSH2 0x5b56, POP: neither the JUMPDEST nor the JUMP byte is an operation
    final GasBlocks blocks = analyse("0x615b5650");

    assertThat(blocks.entry(0)).isEqualTo(entry(5, 4));
    assertThat(blocks.entry(1)).isZero();
  }

  @Test
  void truncatedPushEndsTheCode() {
    // PUSH1 1, PUSH32 with a single byte of data
    final GasBlocks blocks = analyse("0x60017f00");

    assertThat(blocks.entry(0)).isEqualTo(entry(6, 35));
  }

  @Test
  void blockIsSplitAtTheMostGas() {
    final int pops = 40_000;
    final GasBlocks blocks = analyse("0x" + "50".repeat(pops));

    final int first = GasBlocks.MAX_GAS / 2;
    assertThat(blocks.entry(0)).isEqualTo(entry(first * 2, first));
    assertThat(blocks.entry(first)).isEqualTo(entry((pops - first) * 2, pops - first));
  }

  @Test
  void blockIsSplitAtTheMostBytes() {
    final int pushes = 2_100;
    final GasBlocks blocks = analyse("0x" + ("7f" + "00".repeat(32)).repeat(pushes));

    final int first = GasBlocks.MAX_LENGTH / 33;
    assertThat(blocks.entry(0)).isEqualTo(entry(first * 3, first * 33));
    assertThat(blocks.entry(first * 33))
        .isEqualTo(entry((pushes - first) * 3, (pushes - first) * 33));
  }

  @Test
  void opcodeOfALaterForkIsNotInBlocks() {
    final int[] byzantium = costs(MainnetEVMs.byzantium(ProgramRun.V2_BLOCK_GAS));
    // PUSH1 1, PUSH1 1, SHL: SHL comes with Constantinople
    final byte[] code = Bytes.fromHexString("0x600160011b").toArrayUnsafe();

    assertThat(GasBlocks.analyse(code, byzantium).entry(0)).isEqualTo(entry(6, 4));
    assertThat(GasBlocks.analyse(code, COSTS).entry(0)).isEqualTo(entry(9, 5));
  }

  @Test
  void blocksAreAnalysedOncePerCodeAndFork() {
    final Code code = new Code(Bytes.fromHexString("0x6001600201"));
    final int[] byzantium = costs(MainnetEVMs.byzantium(ProgramRun.V2_BLOCK_GAS));

    final GasBlocks onOsaka = GasBlocks.of(code, COSTS);

    assertThat(GasBlocks.of(code, COSTS)).isSameAs(onOsaka);
    final GasBlocks onByzantium = GasBlocks.of(code, byzantium);
    assertThat(onByzantium).isNotSameAs(onOsaka);
    assertThat(onByzantium.entry(0)).isEqualTo(onOsaka.entry(0)).isEqualTo(entry(9, 5));
  }

  @Test
  void evmRunsTheCodeInBlocks() {
    final Code code = new Code(Bytes.fromHexString("0x600160020100"));
    final MessageFrame frame = new TestMessageFrameBuilderV2().code(code).initialGas(100).build();
    frame.setState(MessageFrame.State.CODE_EXECUTING);

    OSAKA.runToHalt(frame, OperationTracer.NO_TRACING);

    assertThat(code.getGasBlocks()).isNotNull();
    assertThat(frame.getState()).isEqualTo(MessageFrame.State.CODE_SUCCESS);
    assertThat(frame.getRemainingGas()).isEqualTo(91);
    assertThat(TestMessageFrameBuilderV2.getV2StackItem(frame, 0).u0()).isEqualTo(3);
  }

  private static GasBlocks analyse(final String code) {
    return GasBlocks.analyse(Bytes.fromHexString(code).toArrayUnsafe(), COSTS);
  }

  private static int entry(final int gas, final int length) {
    return gas << 16 | length;
  }

  private static int[] costs(final EVM evm) {
    return GasBlocks.costs(
        evm.getOperationsUnsafe(),
        OperationsV2.of(evm.getOperationsUnsafe(), evm.getGasCalculator(), evm.getEvmVersion()));
  }
}
