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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;

/**
 * The operation groups of an EVM. A group is a run of operations of fixed cost that can only fail
 * on the stack, so that its gas can be charged at once. Groups are found as the code runs, from the
 * pc where a run starts, and are shared by every code with the same sequence of opcodes: once a
 * sequence has run {@code threshold} times, a class is generated for it.
 *
 * <p>A group fails exactly where its operations would fail one at a time: the frame halts with all
 * its gas consumed either way, and a frame without the gas of the whole group runs it one operation
 * at a time.
 */
public final class OperationGroups {

  /** The most operations in a group, so that their code fits in C2's inlining budget. */
  static final int MAX_OPERATIONS = 24;

  /** The most groups generated, which bounds the classes they take. */
  static final int MAX_GROUPS = 4096;

  /** The most sequences counted, which bounds the memory the counts take. */
  static final int MAX_SEQUENCES = 1 << 16;

  private static final int EQ = 0x14;

  private final int threshold;

  /** The fixed cost of each opcode in groups, -1 for the other opcodes. */
  private final int[] costs;

  private final Map<Bytes, Sequence> sequences = new ConcurrentHashMap<>();
  private final AtomicInteger generated = new AtomicInteger();
  private volatile boolean full;

  /**
   * Creates the operation groups of an EVM.
   *
   * @param threshold the runs of a sequence after which its group is generated, at least 1
   * @param operations the operations of the fork, indexed by opcode
   * @param operationsV2 their v2 versions, null where the interpreter loop runs the opcode itself
   */
  public OperationGroups(
      final int threshold, final Operation[] operations, final Operation[] operationsV2) {
    this.threshold = threshold;
    this.costs = costs(operations, operationsV2);
  }

  /** A sequence of opcodes, counted until its group is generated. */
  static final class Sequence {
    final byte[] opcodes;
    int runs;
    volatile OperationGroup group;

    Sequence(final byte[] opcodes) {
      this.opcodes = opcodes;
    }
  }

  /** The run that starts at a pc of a code, with its sequence, null if it is not a group. */
  public static final class Link {
    final int pc;
    final int gas;
    final Sequence sequence;
    OperationGroup group;

    Link(final int pc, final int gas, final Sequence sequence) {
      this.pc = pc;
      this.gas = gas;
      this.sequence = sequence;
    }

    /**
     * The gas of the whole group.
     *
     * @return the gas
     */
    public int gas() {
      return gas;
    }
  }

  /**
   * The links of a code, kept on the code.
   *
   * @param code the code
   * @return the links
   */
  public OperationGroupLinks linksOf(final Code code) {
    OperationGroupLinks links = code.getOperationGroupLinks();
    // a code run by another fork has links to that fork's groups
    if (links == null || links.groups() != this) {
      links = new OperationGroupLinks(this);
      code.setOperationGroupLinks(links);
    }
    return links;
  }

  /**
   * Whether an opcode can be in a group in this fork.
   *
   * @param opcode the opcode
   * @return whether it can be in a group
   */
  public boolean inGroups(final int opcode) {
    return costs[opcode] >= 0;
  }

  /**
   * The group of a link, counting a run of its sequence until the group is generated.
   *
   * @param link the link
   * @return the group, or null if the run is not a group or not generated yet
   */
  public OperationGroup group(final Link link) {
    OperationGroup group = link.group;
    if (group == null && link.sequence != null) {
      group = link.sequence.group;
      // the count is a heuristic, so lost updates between threads don't matter
      if (group == null && !full && ++link.sequence.runs >= threshold) {
        group = generate(link.sequence);
      }
      link.group = group;
    }
    return group;
  }

  private OperationGroup generate(final Sequence sequence) {
    synchronized (sequence) {
      if (sequence.group == null) {
        if (generated.incrementAndGet() > MAX_GROUPS) {
          full = true;
          return null;
        }
        sequence.group = OperationGroupCompiler.compile(sequence.opcodes);
      }
      return sequence.group;
    }
  }

  /** The link of the run starting at a pc, found by reading the code from there. */
  Link link(final byte[] code, final int pc) {
    final byte[] opcodes = new byte[MAX_OPERATIONS];
    int count = 0;
    int gas = 0;
    int next = pc;
    while (count < MAX_OPERATIONS && next < code.length && continuesRun(code, pc, next)) {
      final int opcode = code[next] & 0xff;
      opcodes[count++] = (byte) opcode;
      gas += costs[opcode];
      next += size(opcode);
    }
    // a group of one operation saves nothing
    if (count < 2) {
      return new Link(pc, 0, null);
    }
    final Bytes key = Bytes.wrap(Arrays.copyOf(opcodes, count));
    final Sequence sequence =
        sequences.size() < MAX_SEQUENCES
            ? sequences.computeIfAbsent(key, k -> new Sequence(k.toArrayUnsafe()))
            : sequences.get(key);
    return new Link(pc, gas, sequence);
  }

  private boolean continuesRun(final byte[] code, final int start, final int pc) {
    final int opcode = code[pc] & 0xff;
    return costs[opcode] >= 0
        // a JUMPDEST starts a new run, as a jump can enter there
        && (pc == start || opcode != JumpDestOperation.OPCODE)
        // a push whose data runs past the code stays out, so a group ends inside the code
        && pc + size(opcode) <= code.length;
  }

  /**
   * The size of an operation in groups, with its immediate data.
   *
   * @param opcode the opcode
   * @return the size
   */
  static int size(final int opcode) {
    return opcode > PushOperation.PUSH_BASE && opcode <= PushOperation.PUSH_MAX
        ? opcode - PushOperation.PUSH_BASE + 1
        : 1;
  }

  private static int[] costs(final Operation[] operations, final Operation[] operationsV2) {
    final int[] costs = new int[256];
    for (int opcode = 0; opcode < costs.length; opcode++) {
      // groups call the static versions the loop runs, and EQ's, whose instance the loop runs
      final boolean runByLoop =
          operationsV2[opcode] == null
              || (opcode == EQ && operationsV2[opcode] instanceof EqOperationV2);
      costs[opcode] =
          runByLoop
                  && isGroupOpcode(opcode)
                  && operations[opcode] instanceof AbstractFixedCostOperation operation
                  && operation.getOpcode() == opcode
              ? (int) operation.getGasCost()
              : -1;
    }
    return costs;
  }

  /**
   * Whether the opcode can be in groups, in the forks that have it.
   *
   * @param opcode the opcode
   * @return whether it can be in groups
   */
  static boolean isGroupOpcode(final int opcode) {
    return (opcode >= 0x01 && opcode <= 0x09) // ADD to MULMOD
        || opcode == 0x0b // SIGNEXTEND
        || (opcode >= 0x10 && opcode <= 0x1e) // LT to CLZ
        || opcode == 0x50 // POP
        || opcode == JumpDestOperation.OPCODE
        || (opcode >= PushOperation.PUSH_BASE && opcode <= 0x9f); // PUSH0 to SWAP16
  }
}
