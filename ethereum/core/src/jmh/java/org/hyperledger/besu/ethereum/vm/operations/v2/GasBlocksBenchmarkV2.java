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
package org.hyperledger.besu.ethereum.vm.operations.v2;

import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/** Runs loops of fixed cost operations on EVM v2, with and without block gas. */
@State(Scope.Thread)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class GasBlocksBenchmarkV2 {

  /** 10,000 iterations of (i * 7 + 3) & 0xff, then i - 1. */
  private static final String ARITHMETIC = "0x6127105b6007810260030160ff1650600190038060035700";

  /** 10,000 iterations of a PUSH32 compared to i, then (i >> 2) < 5, then i - 1. */
  private static final String MIXED =
      "0x6127105b7f"
          + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
          + "811415508060021c60051050600190038060035700";

  @Param({"ARITHMETIC", "MIXED"})
  private String program;

  @Param({"false", "true"})
  private boolean blockGas;

  private EVM evm;
  private Code code;
  private MessageFrame.Builder frames;

  @Setup
  public void setUp() {
    evm =
        MainnetEVMs.osaka(
            BigInteger.ONE,
            new EvmConfiguration(
                EvmConfiguration.DEFAULT.jumpDestCacheWeightKB(),
                EvmConfiguration.DEFAULT.worldUpdaterMode(),
                EvmConfiguration.DEFAULT.enableOptimizedOpcodes(),
                true,
                blockGas));
    code = new Code(Bytes.fromHexString(program.equals("ARITHMETIC") ? ARITHMETIC : MIXED));
    frames = BenchmarkHelperV2.messageCallFrameBuilder().code(code);
    final MessageFrame.State state = execute().getState();
    if (state != MessageFrame.State.CODE_SUCCESS) {
      throw new IllegalStateException(program + " ends in " + state);
    }
  }

  @Benchmark
  public long run() {
    return execute().getRemainingGas();
  }

  private MessageFrame execute() {
    final MessageFrame frame = frames.build();
    frame.setState(MessageFrame.State.CODE_EXECUTING);
    evm.runToHalt(frame, OperationTracer.NO_TRACING);
    frame.releaseStackV2();
    return frame;
  }
}
