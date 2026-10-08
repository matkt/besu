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
package org.hyperledger.besu.evm.operation;

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

import org.apache.tuweni.units.bigints.UInt256;

/** The Add operation. */
public class AddOperation extends AbstractFixedCostOperation {

  /** The Add operation success result. */
  static final OperationResult addSuccess = new OperationResult(3, null);

  /**
   * Instantiates a new Add operation.
   *
   * @param gasCalculator the gas calculator
   */
  public AddOperation(final GasCalculator gasCalculator) {
    super(0x01, "ADD", 2, 1, gasCalculator, gasCalculator.getVeryLowTierGasCost());
  }

  @Override
  public Operation.OperationResult executeFixedCostOperation(final MessageFrame frame) {
    return staticOperation(frame);
  }

  /**
   * Static operation.
   *
   * @param frame the frame
   * @return the operation result
   */
  public static OperationResult staticOperation(final MessageFrame frame) {
    final UInt256 value0 = UInt256.fromBytes(frame.popStackItem());
    final UInt256 value1 = UInt256.fromBytes(frame.popStackItem());
    frame.pushStackItem(value0.add(value1).toBytes());
    return addSuccess;
  }
}
