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

import static org.hyperledger.besu.evm.internal.Words.clampedToLong;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.Code;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.code.CodeFactory;
import org.hyperledger.besu.evm.code.CodeInvalid;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.Words;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;

/** The Abstract create operation. */
public abstract class AbstractCreateOperation extends AbstractOperation {

  /** The constant UNDERFLOW_RESPONSE. */
  protected static final OperationResult UNDERFLOW_RESPONSE =
      new OperationResult(0L, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);

  /** The maximum init code size */
  protected int maxInitcodeSize;

  /**
   * Instantiates a new Abstract create operation.
   *
   * @param opcode the opcode
   * @param name the name
   * @param stackItemsConsumed the stack items consumed
   * @param stackItemsProduced the stack items produced
   * @param gasCalculator the gas calculator
   * @param maxInitcodeSize Maximum init code size
   */
  protected AbstractCreateOperation(
      final int opcode,
      final String name,
      final int stackItemsConsumed,
      final int stackItemsProduced,
      final GasCalculator gasCalculator,
      final int maxInitcodeSize) {
    super(opcode, name, stackItemsConsumed, stackItemsProduced, gasCalculator);
    this.maxInitcodeSize = maxInitcodeSize;
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    // manual check because some reads won't come until the "complete" step.
    if (frame.stackSize() < getStackItemsConsumed()) {
      return UNDERFLOW_RESPONSE;
    }

    Supplier<Code> codeSupplier = () -> getInitCode(frame, evm);
    System.out.println("before create cos " + frame.getRemainingGas());

    final long cost = cost(frame, codeSupplier);

    System.out.println("after create cos " + frame.getRemainingGas());
    if (frame.isStatic()) {
      return new OperationResult(cost, ExceptionalHaltReason.ILLEGAL_STATE_CHANGE);
    } else if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    final Wei value = Wei.wrap(frame.getStackItem(0));

    final Address address = frame.getRecipientAddress();
    final MutableAccount account = frame.getWorldUpdater().getAccount(address);

    frame.clearReturnData();
    final long inputOffset = clampedToLong(frame.getStackItem(1));
    final long inputSize = clampedToLong(frame.getStackItem(2));
    if (inputSize > maxInitcodeSize) {
      frame.popStackItems(getStackItemsConsumed());
      return new OperationResult(cost, ExceptionalHaltReason.CODE_TOO_LARGE);
    }

    if (value.compareTo(account.getBalance()) > 0
        || frame.getDepth() >= 1024
        || account.getNonce() == -1
        || codeSupplier.get() == null) {
      fail(frame);
    } else {
      account.incrementNonce();

      final Bytes inputData = frame.readMemory(inputOffset, inputSize);
      // Never cache CREATEx initcode. The amount of reuse is very low, and caching mostly
      // addresses disk loading delay, and we already have the code.
      Code code = evm.getCodeUncached(inputData);

      final Address contractAddress = targetContractAddress(frame, code);
      final Wei contractValue = Wei.wrap(frame.getStackItem(0));

      if (code.isValid() && frame.getCode().getEofVersion() <= code.getEofVersion()) {

        long statlessGasCost = statelessCost(frame, contractAddress, contractValue);
        System.out.println("touching " + statlessGasCost);
        frame.decrementRemainingGas(statlessGasCost);
        frame.decrementRemainingGas(cost);
        spawnChildMessage(frame, contractAddress, contractValue, code, evm);
        frame.incrementRemainingGas(cost);
      } else {
        fail(frame);
      }
    }

    return new OperationResult(cost, null);
  }

  /**
   * Cost operation.
   *
   * @param frame the frame
   * @param codeSupplier a supplier for the initcode, if needed for costing
   * @return the long
   */
  protected abstract long cost(final MessageFrame frame, Supplier<Code> codeSupplier);

  protected abstract long statelessCost(
      final MessageFrame frame, final Address contractAddress, final Wei value);

  /**
   * Target contract address.
   *
   * @param frame the frame
   * @param initcode the initcode for the new contract.
   * @return the address
   */
  protected abstract Address targetContractAddress(MessageFrame frame, Code initcode);

  /**
   * Gets the initcode that will be run.
   *
   * @param frame The message frame the operation executed in
   * @param evm the EVM executing the message frame
   * @return the initcode, raw bytes, unparsed and unvalidated
   */
  protected abstract Code getInitCode(MessageFrame frame, EVM evm);

  private void fail(final MessageFrame frame) {
    final long inputOffset = clampedToLong(frame.getStackItem(1));
    final long inputSize = clampedToLong(frame.getStackItem(2));
    frame.readMutableMemory(inputOffset, inputSize);
    frame.popStackItems(getStackItemsConsumed());
    frame.pushStackItem(FAILURE_STACK_ITEM);
  }

  private void spawnChildMessage(
      final MessageFrame parent,
      final Address contractAddress,
      final Wei contractValue,
      final Code code,
      final EVM evm) {

    final long childGasStipend =
        gasCalculator().gasAvailableForChildCreate(parent.getRemainingGas());
    parent.decrementRemainingGas(childGasStipend);

    // frame addition is automatically handled by parent messageFrameStack
    MessageFrame.builder()
        .parentMessageFrame(parent)
        .type(MessageFrame.Type.CONTRACT_CREATION)
        .initialGas(childGasStipend)
        .address(contractAddress)
        .contract(contractAddress)
        .inputData(Bytes.EMPTY)
        .sender(parent.getRecipientAddress())
        .value(contractValue)
        .apparentValue(contractValue)
        .code(code)
        .accessWitness(parent.getAccessWitness())
        .completer(child -> complete(parent, child, evm))
        .build();

    parent.setState(MessageFrame.State.CODE_SUSPENDED);
  }

  private void complete(final MessageFrame frame, final MessageFrame childFrame, final EVM evm) {
    frame.setState(MessageFrame.State.CODE_EXECUTING);

    Code outputCode =
        CodeFactory.createCode(childFrame.getOutputData(), evm.getMaxEOFVersion(), true);
    frame.popStackItems(getStackItemsConsumed());

    if (outputCode.isValid()) {
      frame.incrementRemainingGas(childFrame.getRemainingGas());
      frame.addLogs(childFrame.getLogs());
      frame.addSelfDestructs(childFrame.getSelfDestructs());
      frame.addCreates(childFrame.getCreates());

      if (childFrame.getState() == MessageFrame.State.COMPLETED_SUCCESS) {
        Address createdAddress = childFrame.getContractAddress();
        frame.pushStackItem(Words.fromAddress(createdAddress));
        onSuccess(frame, createdAddress);
      } else {
        frame.setReturnData(childFrame.getOutputData());
        frame.pushStackItem(FAILURE_STACK_ITEM);
        onFailure(frame, childFrame.getExceptionalHaltReason());
      }
    } else {
      frame.getWorldUpdater().deleteAccount(childFrame.getRecipientAddress());
      frame.setReturnData(childFrame.getOutputData());
      frame.pushStackItem(FAILURE_STACK_ITEM);
      onInvalid(frame, (CodeInvalid) outputCode);
    }
    final int currentPC = frame.getPC();
    frame.setPC(currentPC + 1);
  }

  /**
   * Called when the child {@code CONTRACT_CREATION} message has completed successfully, used to
   * give library users a chance to do implementation specific logic.
   *
   * @param frame the frame running the successful operation
   * @param createdAddress the address of the newly created contract
   */
  protected void onSuccess(final MessageFrame frame, final Address createdAddress) {
    // no-op by default
  }

  /**
   * Called when the child {@code CONTRACT_CREATION} message has failed to execute, used to give
   * library users a chance to do implementation specific logic.
   *
   * @param frame the frame running the successful operation
   * @param haltReason the exceptional halt reason of the child frame
   */
  protected void onFailure(
      final MessageFrame frame, final Optional<ExceptionalHaltReason> haltReason) {
    // no-op by default
  }

  /**
   * Called when the child {@code CONTRACT_CREATION} message has completed successfully but the
   * returned contract is invalid per chain rules, used to give library users a chance to do
   * implementation specific logic.
   *
   * @param frame the frame running the successful operation
   * @param invalidCode the code object containing the invalid code
   */
  protected void onInvalid(final MessageFrame frame, final CodeInvalid invalidCode) {
    // no-op by default
  }
}
