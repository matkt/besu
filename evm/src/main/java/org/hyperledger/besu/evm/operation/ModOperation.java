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
package org.hyperledger.besu.evm.operation;

import org.hyperledger.besu.datatypes.UInt256;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** The Mod operation. */
@SuppressWarnings("unused")
public class ModOperation extends AbstractFixedCostOperation {

  private static final OperationResult modSuccess = new OperationResult(5, null);

  /**
   * Instantiates a new Mod operation.
   *
   * @param gasCalculator the gas calculator
   */
  public ModOperation(final GasCalculator gasCalculator) {
    super(0x06, "MOD", 2, 1, gasCalculator, gasCalculator.getLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(
      final MessageFrame frame, final EVM evm) {
    return staticOperation(frame);
  }

  /**
   * Performs Mod operation.
   *
   * @param frame the frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    final Bytes value0 = frame.popStackItem();
    final Bytes value1 = frame.popStackItem();
    if (value1.isZero()) {
      frame.pushStackItem(Bytes32.ZERO);
    } else {

      int[] limbs0 = new int[8];
      int[] limbs1 = new int[8];

      final byte[] bytes0 = value0.toArrayUnsafe();
      final byte[] bytes1 = value1.toArrayUnsafe();

      byte[] padded0 = bytes0.length == Bytes32.SIZE ? bytes0 : padTo32Bytes(bytes0);
      byte[] padded1 = bytes1.length == Bytes32.SIZE ? bytes1 : padTo32Bytes(bytes1);

      ByteBuffer buffer0 = ByteBuffer.wrap(padded0).order(ByteOrder.BIG_ENDIAN);
      ByteBuffer buffer1 = ByteBuffer.wrap(padded1).order(ByteOrder.BIG_ENDIAN);

      limbs0[0] = buffer0.getInt(0);
      limbs0[1] = buffer0.getInt(4);
      limbs0[2] = buffer0.getInt(8);
      limbs0[3] = buffer0.getInt(12);
      limbs0[4] = buffer0.getInt(16);
      limbs0[5] = buffer0.getInt(20);
      limbs0[6] = buffer0.getInt(24);
      limbs0[7] = buffer0.getInt(28);

      limbs1[0] = buffer0.getInt(0);
      limbs1[1] = buffer0.getInt(4);
      limbs1[2] = buffer0.getInt(8);
      limbs1[3] = buffer0.getInt(12);
      limbs1[4] = buffer0.getInt(16);
      limbs1[5] = buffer0.getInt(20);
      limbs1[6] = buffer0.getInt(24);
      limbs1[7] = buffer0.getInt(28);

      final UInt256 b1 = new UInt256(limbs0);
      final UInt256 b2 = new UInt256(limbs1);

      final UInt256 result = b1.mod(b2);

      Bytes resultBytes = Bytes.wrap(result.toBytesBE());
      frame.pushStackItem(resultBytes);
    }

    return modSuccess;
  }

  private static byte[] padTo32Bytes(final byte[] input) {
    byte[] padded = new byte[Bytes32.SIZE];
    System.arraycopy(input, 0, padded, Bytes32.SIZE - input.length, input.length);
    return padded;
  }
}
