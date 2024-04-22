/*
 * Copyright contributors to Hyperledger Besu
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

import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.Words;

/** The Code size operation. */
public class CodeSizeOperation extends AbstractOperation {

  /**
   * Instantiates a new Code size operation.
   *
   * @param gasCalculator the gas calculator
   */
  public CodeSizeOperation(final GasCalculator gasCalculator) {
    super(0x38, "CODESIZE", 0, 1, gasCalculator);
  }

  protected long cost(final MessageFrame frame) {
    return gasCalculator().extCodeSizeOperationGasCost(frame);
  }


  @Override
  public Operation.OperationResult execute(
      final MessageFrame frame, final EVM evm) {
    final Code code = frame.getCode();
    frame.pushStackItem(Words.intBytes(code.getSize()));
    return new OperationResult(cost(frame),null);
  }
}
