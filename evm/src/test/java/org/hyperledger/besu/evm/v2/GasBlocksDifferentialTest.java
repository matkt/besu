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

import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.operation.InvalidOperation;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.v2.ProgramRun.Fork;
import org.hyperledger.besu.evm.v2.ProgramRun.Outcome;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.LongStream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

/**
 * Runs programs on EVM v2 with and without block gas and requires both to end in the same state,
 * with the same halt reason, whatever gas they start with.
 */
class GasBlocksDifferentialTest {

  private static final int PROGRAMS_PER_FORK = 300;
  private static final long GAS = 3_000_000L;

  static final List<Fork> FORKS = ProgramRun.FORKS;

  private static final List<Bytes> PROGRAMS =
      List.of(
          // counts down from 10 in a loop of fixed cost operations
          Bytes.fromHexString("0x600a5b600190038060025700"),
          // ADD without a second operand, after a first ADD
          Bytes.fromHexString("0x6001600201015b00"),
          // PUSH1 1025 times
          Bytes.fromHexString("0x" + "6001".repeat(1025) + "00"),
          // ends in a PUSH32 with a single byte of data
          Bytes.fromHexString("0x60016002017f00"),
          // jumps to a JUMPDEST in the middle of fixed cost operations
          Bytes.fromHexString("0x600756600160025b600360040100"),
          // a block that a SLOAD ends, then one that a JUMPDEST starts
          Bytes.fromHexString("0x600160025460035b60040100"));

  @ParameterizedTest(name = "{0}")
  @FieldSource("FORKS")
  void programsEndInTheSameStateWhateverGasTheyStartWith(final Fork fork) {
    final EVM v2 = fork.factory().apply(ProgramRun.V2);
    final EVM blockGas = fork.factory().apply(ProgramRun.V2_BLOCK_GAS);
    final List<String> differences = new ArrayList<>();
    for (final Bytes program : PROGRAMS) {
      final long used = GAS - run(v2, program, GAS).remainingGas();
      // every gas up to the end of the program, then around the gas it uses
      final LongStream gases =
          LongStream.concat(
              LongStream.rangeClosed(0, Math.min(used + 2, 400)),
              LongStream.of(used - 1, used, used + 1, GAS));
      gases.forEach(
          gas -> {
            final Outcome onV2 = run(v2, program, gas);
            final Outcome withBlockGas = run(blockGas, program, gas);
            if (!withBlockGas.equals(onV2)) {
              differences.add(
                  difference(program.toHexString() + " with gas " + gas, onV2, withBlockGas));
            }
          });
    }
    assertThat(differences).isEmpty();
  }

  @ParameterizedTest(name = "{0}")
  @FieldSource("FORKS")
  void randomProgramsEndInTheSameState(final Fork fork) {
    final EVM v2 = fork.factory().apply(ProgramRun.V2);
    final EVM blockGas = fork.factory().apply(ProgramRun.V2_BLOCK_GAS);
    final Operation[] operations = v2.getOperationsUnsafe();
    final List<Integer> opcodes = new ArrayList<>();
    for (int opcode = 0; opcode < 0x100; opcode++) {
      if (!(operations[opcode] instanceof InvalidOperation)) {
        opcodes.add(opcode);
      }
    }
    final List<Integer> statefulOpcodes =
        RandomProgramV1V2DifferentialTest.STATEFUL_OPCODES.stream()
            .filter(opcodes::contains)
            .toList();
    final List<String> differences = new ArrayList<>();
    for (int i = 0; i < PROGRAMS_PER_FORK; i++) {
      final Random random = new Random(fork.name().hashCode() * 37L + i);
      final Bytes contract =
          RandomProgramV1V2DifferentialTest.program(random, opcodes, statefulOpcodes, operations);
      final Bytes callee =
          RandomProgramV1V2DifferentialTest.program(random, opcodes, statefulOpcodes, operations);
      final Bytes secondCallee =
          RandomProgramV1V2DifferentialTest.program(random, opcodes, statefulOpcodes, operations);
      final boolean isStatic = random.nextInt(8) == 0;
      // little gas half of the time, so that blocks run out of it
      final long gas = random.nextBoolean() ? random.nextInt(2_000) : GAS;
      final Outcome onV2 =
          ProgramRun.run(
              v2,
              contract,
              callee,
              secondCallee,
              gas,
              isStatic,
              RandomProgramV1V2DifferentialTest.SLOTS);
      final Outcome withBlockGas =
          ProgramRun.run(
              blockGas,
              contract,
              callee,
              secondCallee,
              gas,
              isStatic,
              RandomProgramV1V2DifferentialTest.SLOTS);
      if (!withBlockGas.equals(onV2)) {
        differences.add(
            difference(
                String.format(
                    "program %d with gas %d%s%n  contract %s%n  callee %s%n  second callee %s",
                    i,
                    gas,
                    isStatic ? " in a static frame" : "",
                    contract.toHexString(),
                    callee.toHexString(),
                    secondCallee.toHexString()),
                onV2,
                withBlockGas));
      }
    }
    assertThat(differences).isEmpty();
  }

  private static Outcome run(final EVM evm, final Bytes program, final long gas) {
    return ProgramRun.run(
        evm,
        program,
        Bytes.EMPTY,
        Bytes.EMPTY,
        gas,
        false,
        RandomProgramV1V2DifferentialTest.SLOTS);
  }

  private static String difference(
      final String program, final Outcome onV2, final Outcome withBlockGas) {
    return String.format("%s%n  v2 %s%n  block gas %s", program, onV2, withBlockGas);
  }
}
