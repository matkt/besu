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

import java.util.Arrays;

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

  /** The costs the blocks were computed with, which tell forks apart. */
  private final int[] costs;

  /**
   * One long per 32 bytes of code: the number of blocks starting before them in the high 32 bits,
   * and one bit per byte, set where a block starts, in the low 32 bits.
   */
  private final long[] chunks;

  /** The entry of each block, in code order. */
  private final int[] entries;

  private GasBlocks(final int[] costs, final long[] chunks, final int[] entries) {
    this.costs = costs;
    this.chunks = chunks;
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
   * @return the blocks
   */
  public static GasBlocks of(final Code code, final int[] costs) {
    GasBlocks blocks = code.getGasBlocks();
    if (blocks == null || blocks.costs != costs) {
      blocks = analyse(code.getBytes().toArrayUnsafe(), costs);
      code.setGasBlocks(blocks);
    }
    return blocks;
  }

  /**
   * The block starting at a pc.
   *
   * @param pc the pc, inside the code
   * @return the gas of the block in the high 16 bits and its length in the low 16 bits, 0 where no
   *     block starts
   */
  public int entry(final int pc) {
    final long chunk = chunks[pc >>> 5];
    // an int shift uses only the low five bits of pc, its offset in the chunk
    final int bit = 1 << pc;
    if (((int) chunk & bit) == 0) {
      return 0;
    }
    return entries[(int) (chunk >>> 32) + Integer.bitCount((int) chunk & (bit - 1))];
  }

  /** The size of an operation, with its immediate data. */
  private static int size(final int opcode) {
    if (opcode > PushOperation.PUSH_BASE && opcode <= PushOperation.PUSH_MAX) {
      return opcode - PushOperation.PUSH_BASE + 1;
    }
    // DUPN, SWAPN and EXCHANGE (EIP-8024)
    return opcode >= 0xe6 && opcode <= 0xe8 ? 2 : 1;
  }

  static GasBlocks analyse(final byte[] code, final int[] costs) {
    final long[] chunks = new long[(code.length >>> 5) + 1];
    // every block takes at least one byte of code
    final int[] entries = new int[code.length];
    int count = 0;
    int pc = 0;
    while (pc < code.length) {
      final int entry = block(code, pc, costs);
      if (entry == 0) {
        pc += size(code[pc] & 0xff);
      } else {
        chunks[pc >>> 5] |= 1L << (pc & 31);
        entries[count++] = entry;
        pc += entry & MAX_LENGTH;
      }
    }
    long blocksBefore = 0;
    for (int chunk = 0; chunk < chunks.length; chunk++) {
      final long starts = chunks[chunk];
      chunks[chunk] = blocksBefore << 32 | starts;
      blocksBefore += Long.bitCount(starts);
    }
    return new GasBlocks(costs, chunks, Arrays.copyOf(entries, count));
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
