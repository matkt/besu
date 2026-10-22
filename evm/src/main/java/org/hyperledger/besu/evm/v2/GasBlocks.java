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

import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.operation.AbstractFixedCostOperation;
import org.hyperledger.besu.evm.operation.JumpDestOperation;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.PushOperation;
import org.hyperledger.besu.evm.v2.operation.EqOperationV2;

/**
 * The blocks of a code that EVM v2 runs with the gas of the whole block charged at once: runs of
 * operations of fixed cost that can only fail on the stack, starting at a JUMPDEST or after any
 * other operation.
 *
 * <p>A block fails exactly where its operations would fail one at a time: the frame halts with all
 * its gas consumed either way, and a frame without the gas for the whole block runs it one
 * operation at a time.
 */
public final class GasBlocks {

  /** The most bytes of code in a block, so that its length fits in the low half of an entry. */
  public static final int MAX_LENGTH = 0xFFFF;

  /** The most gas of a block, so that it fits in the high half of an entry. */
  static final int MAX_GAS = 0xFFFF;

  private static final int EQ = 0x14;

  private final int[] costs;
  private final int[] entries;

  private GasBlocks(final int[] costs, final int[] entries) {
    this.costs = costs;
    this.entries = entries;
  }

  /**
   * The fixed cost of each opcode that EVM v2 runs inside a block in a fork.
   *
   * @param operations the operations the fork registers, indexed by opcode
   * @param operationsV2 their v2 versions, null where the interpreter loop runs the opcode itself
   * @return the cost of each opcode, -1 for the opcodes that are not in blocks
   */
  public static int[] costs(final Operation[] operations, final Operation[] operationsV2) {
    final int[] costs = new int[256];
    for (int opcode = 0; opcode < costs.length; opcode++) {
      final boolean runByLoop =
          operationsV2[opcode] == null
              || (opcode == EQ && operationsV2[opcode] instanceof EqOperationV2);
      costs[opcode] =
          runByLoop
                  && isBlockOpcode(opcode)
                  && operations[opcode] instanceof AbstractFixedCostOperation operation
                  && operation.getOpcode() == opcode
              ? (int) operation.getGasCost()
              : -1;
    }
    return costs;
  }

  /**
   * The blocks of the code, analysed once per code and fork.
   *
   * @param code the code
   * @param costs the costs of the fork, from {@link #costs}
   * @return for each pc, the gas of the block starting there in the high 16 bits and its length in
   *     the low 16 bits, 0 where no block starts
   */
  public static int[] of(final Code code, final int[] costs) {
    final GasBlocks blocks = code.getGasBlocks();
    if (blocks != null && blocks.costs == costs) {
      return blocks.entries;
    }
    final int[] entries = analyse(code.getBytes().toArrayUnsafe(), costs);
    code.setGasBlocks(new GasBlocks(costs, entries));
    return entries;
  }

  /**
   * The size of an operation, with its immediate data.
   *
   * @param opcode the opcode
   * @return the number of bytes of code it takes
   */
  public static int size(final int opcode) {
    if (opcode > PushOperation.PUSH_BASE && opcode <= PushOperation.PUSH_MAX) {
      return opcode - PushOperation.PUSH_BASE + 1;
    }
    // DUPN, SWAPN and EXCHANGE (EIP-8024)
    return opcode >= 0xe6 && opcode <= 0xe8 ? 2 : 1;
  }

  static int[] analyse(final byte[] code, final int[] costs) {
    final int[] entries = new int[code.length];
    int pc = 0;
    while (pc < code.length) {
      final int entry = block(code, pc, costs);
      entries[pc] = entry;
      pc += entry == 0 ? size(code[pc] & 0xff) : entry & MAX_LENGTH;
    }
    return entries;
  }

  /** The entry of the block starting at {@code start}, 0 if its operation is not in blocks. */
  private static int block(final byte[] code, final int start, final int[] costs) {
    int pc = start;
    int gas = 0;
    while (pc < code.length && continuesBlock(code, start, pc, gas, costs)) {
      final int opcode = code[pc] & 0xff;
      gas += costs[opcode];
      pc += size(opcode);
    }
    return pc == start ? 0 : gas << 16 | (pc - start);
  }

  /** Whether the operation at {@code pc} belongs to the block starting at {@code start}. */
  private static boolean continuesBlock(
      final byte[] code, final int start, final int pc, final int gas, final int[] costs) {
    final int opcode = code[pc] & 0xff;
    final int cost = costs[opcode];
    return cost >= 0
        // a JUMPDEST starts a new block, as a jump can enter there
        && (pc == start || opcode != JumpDestOperation.OPCODE)
        && pc + size(opcode) - start <= MAX_LENGTH
        && gas + cost <= MAX_GAS;
  }

  /** Whether the opcode is run inside blocks, in the forks that have it. */
  private static boolean isBlockOpcode(final int opcode) {
    return (opcode >= 0x01 && opcode <= 0x09) // ADD to MULMOD
        || opcode == 0x0b // SIGNEXTEND
        || (opcode >= 0x10 && opcode <= 0x1e) // LT to CLZ
        || opcode == 0x50 // POP
        || opcode == JumpDestOperation.OPCODE
        || (opcode >= PushOperation.PUSH_BASE && opcode <= 0x9f); // PUSH0 to SWAP16
  }
}
