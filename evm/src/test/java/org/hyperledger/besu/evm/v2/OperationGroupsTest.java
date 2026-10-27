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
import org.hyperledger.besu.evm.v2.OperationGroups.Link;
import org.hyperledger.besu.evm.v2.operation.OperationsV2;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class OperationGroupsTest {

  private static final EVM OSAKA = MainnetEVMs.osaka(BigInteger.ONE, ProgramRun.V1);

  private static OperationGroups groups(final int threshold) {
    return new OperationGroups(
        threshold,
        OSAKA.getOperationsUnsafe(),
        OperationsV2.of(
            OSAKA.getOperationsUnsafe(), OSAKA.getGasCalculator(), OSAKA.getEvmVersion()));
  }

  private static byte[] code(final String hex) {
    return Bytes.fromHexString(hex).toArrayUnsafe();
  }

  @Test
  void aRunOfOperationsIsOneGroupWithTheirGas() {
    // PUSH1 1 PUSH1 2 ADD SLOAD
    final Link link = groups(1).link(code("0x600160020154"), 0);

    assertThat(link.sequence.opcodes).containsExactly(0x60, 0x60, 0x01);
    assertThat(link.gas()).isEqualTo(9);
  }

  @Test
  void aJumpDestStartsANewRun() {
    // PUSH1 1 JUMPDEST PUSH1 2 ADD
    final byte[] code = code("0x60015b600201");
    final OperationGroups groups = groups(1);

    assertThat(groups.link(code, 0).sequence).isNull();
    final Link fromJumpDest = groups.link(code, 2);
    assertThat(fromJumpDest.sequence.opcodes).containsExactly(0x5b, 0x60, 0x01);
    assertThat(fromJumpDest.gas()).isEqualTo(7);
  }

  @Test
  void aPushWhoseDataRunsPastTheCodeEndsTheRun() {
    // PUSH1 1 PUSH2 with one byte of data
    assertThat(groups(1).link(code("0x6001610a"), 0).sequence).isNull();
  }

  @Test
  void aRunIsCutAtTheMostOperationsOfAGroup() {
    // PUSH0 ...
    final byte[] code = new byte[OperationGroups.MAX_OPERATIONS + 5];
    Arrays.fill(code, (byte) 0x5f);

    assertThat(groups(1).link(code, 0).sequence.opcodes).hasSize(OperationGroups.MAX_OPERATIONS);
  }

  @Test
  void aGroupIsGeneratedOnceItsSequenceRanThresholdTimesInAnyCode() {
    final OperationGroups groups = groups(3);
    // PUSH1 1 PUSH1 2 ADD and PUSH1 3 PUSH1 4 ADD share their sequence
    final Link first = groups.link(code("0x6001600201"), 0);
    final Link second = groups.link(code("0x6003600401"), 0);

    assertThat(groups.group(first)).isNull();
    assertThat(groups.group(second)).isNull();
    final OperationGroup group = groups.group(first);

    assertThat(group).isNotNull();
    assertThat(groups.group(second)).isSameAs(group);
  }

  @Test
  void aCodeKeepsTheLinkOfEachPcThatRan() {
    final OperationGroups groups = groups(1);
    // JUMPDEST PUSH0 ..., so that each JUMPDEST starts a run
    final byte[] code = new byte[200];
    for (int pc = 0; pc < code.length; pc += 2) {
      code[pc] = 0x5b;
      code[pc + 1] = 0x5f;
    }
    final OperationGroupLinks links = groups.linksOf(new Code(Bytes.wrap(code)));

    for (int pc = 0; pc < code.length; pc += 2) {
      assertThat(links.at(code, pc).pc).isEqualTo(pc);
    }
    for (int pc = 0; pc < code.length; pc += 2) {
      assertThat(links.at(code, pc)).isSameAs(links.at(code, pc));
    }
  }

  @Test
  void evmV2RunsTheGroupOfARunOfOperations() {
    final EVM evm = MainnetEVMs.osaka(BigInteger.ONE, ProgramRun.V2_GROUPS);
    // PUSH1 1 PUSH1 2 ADD STOP
    final Bytes program = Bytes.fromHexString("0x600160020100");

    final ProgramRun.Execution execution =
        ProgramRun.execute(evm, program, Bytes.EMPTY, Bytes.EMPTY, 100_000L, false, List.of());

    final OperationGroupLinks links = execution.frame().getCode().getOperationGroupLinks();
    assertThat(links.at(program.toArrayUnsafe(), 0).group).isNotNull();
    assertThat(execution.outcome().remainingGas()).isEqualTo(100_000L - 9);
    assertThat(execution.outcome().stack()).hasSize(1);
  }

  @Test
  void aCodeRunByAnotherForkGetsNewLinks() {
    final Code code = new Code(Bytes.fromHexString("0x6001600201"));
    final OperationGroups groups = groups(1);
    final OperationGroupLinks links = groups.linksOf(code);

    assertThat(groups.linksOf(code)).isSameAs(links);
    assertThat(groups(1).linksOf(code)).isNotSameAs(links);
  }
}
