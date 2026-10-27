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

import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;

/**
 * A sequence of operations run as one, by a class generated for that sequence by {@link
 * OperationGroupCompiler}. The caller charges the gas of the whole sequence first.
 */
public abstract class OperationGroup {

  /** Default constructor, for the generated classes. */
  protected OperationGroup() {}

  /**
   * Runs the operations of the group.
   *
   * @param frame the frame, already charged with the gas of the group
   * @param code the code
   * @param pc the pc of the first operation
   * @return null with the frame's pc after the group, or the result of the operation that halted
   *     with the frame's pc at it
   */
  public abstract OperationResult run(MessageFrame frame, byte[] code, int pc);
}
