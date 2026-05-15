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
package org.hyperledger.besu.evm.gascalculator;

import static org.hyperledger.besu.evm.gascalculator.stateless.Eip4762AccessWitness.BASIC_DATA_LEAF_KEY;
import static org.hyperledger.besu.evm.gascalculator.stateless.Eip4762AccessWitness.CODE_HASH_LEAF_KEY;
import static org.hyperledger.besu.evm.internal.Words.clampedAdd;

import org.hyperledger.besu.datatypes.AccessEvent;
import org.hyperledger.besu.datatypes.AccessWitness;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.stateless.Eip4762AccessWitness;

import java.util.function.Supplier;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * Gas Calculator for BinTrie (EIP-4762 Stateless Gas Costs).
 *
 * <p>Implements the new gas schedule for Binary Trie based state access:
 *
 * <ul>
 *   <li>WITNESS_BRANCH_COST = 1900 - Cost to access a new branch (stem)
 *   <li>WITNESS_CHUNK_COST = 200 - Cost to access a new leaf (chunk)
 *   <li>SUBTREE_EDIT_COST = 3000 - Cost to write to a new branch
 *   <li>CHUNK_EDIT_COST = 500 - Cost to reset/modify an existing leaf
 *   <li>CHUNK_FILL_COST = 6200 - Cost to fill a previously empty leaf
 * </ul>
 */
public class BinTrieGasCalculator extends OsakaGasCalculator {

  private static final long CREATE_OPERATION_GAS_COST = 1_000L;

  /** Instantiates a new BinTrie Gas Calculator. */
  public BinTrieGasCalculator() {
    super(Address.BLS12_MAP_FP2_TO_G2.getBytes().toArrayUnsafe()[19]);
  }

  /**
   * Instantiates a new BinTrie Gas Calculator.
   *
   * @param maxPrecompile the max precompile
   */
  protected BinTrieGasCalculator(final int maxPrecompile) {
    super(maxPrecompile);
  }

  @Override
  public long getColdSloadCost() {
    return 0L;
  }

  @Override
  public long getColdAccountAccessCost() {
    return 0L;
  }

  @Override
  public long callValueTransferGasCost() {
    return 0L;
  }

  @Override
  public long txCreateCost() {
    return CREATE_OPERATION_GAS_COST;
  }

  @Override
  public long calculateStorageRefundAmount(
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {
    return 0L;
  }

  @Override
  public long getSelfDestructRefundAmount() {
    return 0L;
  }

  @Override
  public long proofOfAbsenceCost(final MessageFrame frame, final Address address) {
    final Eip4762AccessWitness witness = frame.getAccessWitness();
    if (witness == null) return 0L;
    return witness.touchAndChargeProofOfAbsence(address, frame.getRemainingGas());
  }

  @Override
  public long callOperationGasCost(
      final MessageFrame frame,
      final long staticCallCost,
      final long stipend,
      final long inputDataOffset,
      final long inputDataLength,
      final long outputDataOffset,
      final long outputDataLength,
      final Wei transferValue,
      final Address recipientAddress,
      final boolean accountIsWarm) {

    long gas =
        super.callOperationGasCost(
            frame,
            staticCallCost,
            stipend,
            inputDataOffset,
            inputDataLength,
            outputDataOffset,
            outputDataLength,
            transferValue,
            recipientAddress,
            false);

    final Eip4762AccessWitness witness = frame.getAccessWitness();
    if (witness == null) return gas;

    if (!transferValue.isZero()) {
      final Account recipient = frame.getWorldUpdater().get(recipientAddress);
      gas =
          clampedAdd(
              gas,
              witness.touchAndChargeValueTransfer(
                  frame.getContractAddress(),
                  recipientAddress,
                  recipient == null,
                  getWarmStorageReadCost(),
                  frame.getRemainingGas()));
      return gas;
    }

    if (isPrecompile(recipientAddress)) {
      return clampedAdd(gas, getWarmStorageReadCost());
    }

    long messageCallGas =
        witness.touchAddressAndChargeRead(
            recipientAddress, BASIC_DATA_LEAF_KEY, frame.getRemainingGas());
    if (messageCallGas == 0) {
      messageCallGas = getWarmStorageReadCost();
    }

    return clampedAdd(gas, messageCallGas);
  }

  @Override
  public long codeDepositGasCost(final MessageFrame frame, final int codeSize) {
    final Eip4762AccessWitness witness = frame.getAccessWitness();
    if (witness == null) return codeDepositGasCost(codeSize);
    return witness.touchCodeChunksUponContractCreation(
        frame.getContractAddress(), codeSize, frame.getRemainingGas());
  }

  @Override
  public long completedCreateContractGasCost(final MessageFrame frame) {
    final Eip4762AccessWitness witness = frame.getAccessWitness();
    if (witness == null) return 0L;
    return witness.touchAndChargeContractCreateCompleted(
        frame.getContractAddress(), frame.getRemainingGas());
  }

  @Override
  public long extCodeCopyOperationGasCost(
      final MessageFrame frame,
      final Address address,
      final boolean accountIsWarm,
      final long memOffset,
      final long codeOffset,
      final long readSize,
      final long codeSize) {
    long gas = extCodeCopyOperationGasCost(frame, memOffset, readSize);

    final Eip4762AccessWitness witness = frame.getAccessWitness();
    if (witness == null) return gas;

    gas =
        clampedAdd(
            gas,
            witness.touchCodeChunks(
                address, false, codeOffset, readSize, codeSize, frame.getRemainingGas()));

    if (isPrecompile(address)) {
      return clampedAdd(gas, getWarmStorageReadCost());
    }

    long readTargetGas =
        witness.touchAddressAndChargeRead(address, BASIC_DATA_LEAF_KEY, frame.getRemainingGas());
    if (readTargetGas == 0) {
      readTargetGas = getWarmStorageReadCost();
    }

    return clampedAdd(gas, readTargetGas);
  }

  @Override
  public long codeCopyOperationGasCost(
      final MessageFrame frame,
      final long memOffset,
      final long codeOffset,
      final long readSize,
      final long codeSize) {
    long gas = super.dataCopyOperationGasCost(frame, memOffset, readSize);

    final Eip4762AccessWitness witness = frame.getAccessWitness();
    if (witness == null) return gas;

    final Address contractAddress = frame.getContractAddress();
    gas =
        clampedAdd(
            gas,
            witness.touchCodeChunks(
                contractAddress,
                frame.wasCreatedInTransaction(contractAddress),
                codeOffset,
                readSize,
                codeSize,
                frame.getRemainingGas()));

    return gas;
  }

  @Override
  public long pushOperationGasCost(
      final MessageFrame frame, final long codeOffset, final long readSize, final long codeSize) {
    long gas = super.pushOperationGasCost(frame, codeOffset, readSize, codeSize);

    final Eip4762AccessWitness witness = frame.getAccessWitness();
    if (witness == null) return gas;

    if (frame.wasCreatedInTransaction(frame.getContractAddress())
        || (readSize == 1 && codeOffset % 31 != 0)) {
      return gas;
    }

    final Address contractAddress = frame.getContractAddress();
    gas =
        clampedAdd(
            gas,
            witness.touchCodeChunks(
                contractAddress,
                frame.wasCreatedInTransaction(contractAddress),
                codeOffset,
                readSize,
                codeSize,
                frame.getRemainingGas()));

    return gas;
  }

  @Override
  public long balanceOperationGasCost(
      final MessageFrame frame, final boolean accountIsWarm, final Address address) {
    final Eip4762AccessWitness witness = frame.getAccessWitness();
    if (witness == null) return getBalanceOperationGasCost();
    final long gas =
        witness.touchAddressAndChargeRead(address, BASIC_DATA_LEAF_KEY, frame.getRemainingGas());
    if (gas == 0) {
      return getWarmStorageReadCost();
    }
    return gas;
  }

  @Override
  public long extCodeHashOperationGasCost(
      final MessageFrame frame, final boolean accountIsWarm, final Address address) {
    if (isPrecompile(address)) {
      return getWarmStorageReadCost();
    }

    final Eip4762AccessWitness witness = frame.getAccessWitness();
    if (witness == null) return extCodeHashOperationGasCost();

    long gas =
        witness.touchAddressAndChargeRead(address, CODE_HASH_LEAF_KEY, frame.getRemainingGas());
    if (gas == 0) {
      return getWarmStorageReadCost();
    }
    return gas;
  }

  @Override
  public long extCodeSizeOperationGasCost(
      final MessageFrame frame, final boolean accountIsWarm, final Address address) {
    if (isPrecompile(address)) {
      return getWarmStorageReadCost();
    }

    final Eip4762AccessWitness witness = frame.getAccessWitness();
    if (witness == null) return getExtCodeSizeOperationGasCost();

    long gas =
        witness.touchAddressAndChargeRead(address, BASIC_DATA_LEAF_KEY, frame.getRemainingGas());
    if (gas == 0) {
      return getWarmStorageReadCost();
    }
    return gas;
  }

  @Override
  public long selfDestructOperationGasCost(
      final MessageFrame frame,
      final Account recipient,
      final Address recipientAddress,
      final Wei value,
      final Address originatorAddress) {
    long gas = 5000L;

    final Eip4762AccessWitness witness = frame.getAccessWitness();
    if (witness == null) return gas;

    if (!value.isZero()) {
      gas =
          clampedAdd(
              gas,
              witness.touchAndChargeValueTransferSelfDestruct(
                  originatorAddress,
                  recipientAddress,
                  recipient == null,
                  getWarmStorageReadCost(),
                  frame.getRemainingGas()));
    }

    gas =
        clampedAdd(
            gas,
            witness.touchAddressAndChargeRead(
                originatorAddress, BASIC_DATA_LEAF_KEY, frame.getRemainingGas()));

    if (isPrecompile(recipientAddress)) {
      return gas;
    }

    if (!recipientAddress.equals(originatorAddress)) {
      gas =
          clampedAdd(
              gas,
              witness.touchAddressAndChargeRead(
                  recipientAddress, BASIC_DATA_LEAF_KEY, frame.getRemainingGas()));
    }

    return gas;
  }

  @Override
  public long sloadOperationGasCost(
      final MessageFrame frame, final UInt256 key, final boolean slotIsWarm) {
    final Eip4762AccessWitness witness = frame.getAccessWitness();
    if (witness == null) return slotIsWarm ? getWarmStorageReadCost() : getColdSloadCost();

    long gas =
        witness.touchAndChargeStorageLoad(frame.getContractAddress(), key, frame.getRemainingGas());

    if (gas == 0) {
      return getWarmStorageReadCost();
    }

    return gas;
  }

  @Override
  public long calculateStorageCost(
      final MessageFrame frame,
      final UInt256 key,
      final UInt256 newValue,
      final Supplier<UInt256> currentValue,
      final Supplier<UInt256> originalValue) {
    final Eip4762AccessWitness witness = frame.getAccessWitness();
    if (witness == null) return calculateStorageCost(newValue, currentValue, originalValue);

    long gas =
        witness.touchAndChargeStorageStore(
            frame.getRecipientAddress(), key, originalValue != null, frame.getRemainingGas());

    if (gas == 0) {
      return getWarmStorageReadCost();
    }

    return gas;
  }

  @Override
  public AccessWitness newAccessWitness() {
    return new Eip4762AccessWitness();
  }

  /**
   * Returns the cost to read a new branch (stem) in the Binary Trie.
   *
   * @return the witness branch cost (1900)
   */
  public long getWitnessBranchCost() {
    return AccessEvent.getBranchReadCost();
  }

  /**
   * Returns the cost to read a new leaf (chunk) in the Binary Trie.
   *
   * @return the witness chunk cost (200)
   */
  public long getWitnessChunkCost() {
    return AccessEvent.getLeafReadCost();
  }

  /**
   * Returns the cost to write to a new branch (subtree edit).
   *
   * @return the subtree edit cost (3000)
   */
  public long getSubtreeEditCost() {
    return AccessEvent.getBranchWriteCost();
  }

  /**
   * Returns the cost to reset/modify an existing leaf.
   *
   * @return the chunk edit cost (500)
   */
  public long getChunkEditCost() {
    return AccessEvent.getLeafResetCost();
  }

  /**
   * Returns the cost to fill a previously empty leaf.
   *
   * @return the chunk fill cost (6200)
   */
  public long getChunkFillCost() {
    return AccessEvent.getLeafSetCost();
  }
}
