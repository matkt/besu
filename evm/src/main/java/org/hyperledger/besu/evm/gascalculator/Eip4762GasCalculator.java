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

import static org.hyperledger.besu.datatypes.Address.KZG_POINT_EVAL;
import static org.hyperledger.besu.evm.internal.Words.clampedAdd;

import org.hyperledger.besu.datatypes.AccessWitness;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.gascalculator.stateless.Eip4762AccessWitness;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.units.bigints.UInt256;

public class Eip4762GasCalculator extends PragueGasCalculator {
  private static final long CREATE_OPERATION_GAS_COST = 1_000L;

  /** Instantiates a new Prague Gas Calculator. */
  public Eip4762GasCalculator() {
    super(KZG_POINT_EVAL.toArrayUnsafe()[19]);
  }

  @Override
  public long getColdSloadCost() {
    return 0; // no cold gas cost after verkle
  }

  @Override
  public long getColdAccountAccessCost() {
    return 0; // no cold gas cost after verkle
  }

  @Override
  public long computeBaseAccessEventsCost(
          final AccessWitness accessWitness,
          final Transaction transaction,
          final MutableAccount sender) {
    //For a transaction, make these access events:
    //(tx.origin, 0, BASIC_DATA_LEAF_KEY)
    //(tx.origin, 0, CODEHASH_LEAF_KEY)
    //(tx.target, 0, BASIC_DATA_LEAF_KEY)
    long statelessCost = accessWitness.accessAccountBasicData(transaction.getSender());
    statelessCost = clampedAdd(statelessCost, accessWitness.accessAccountCodeHash(transaction.getSender()));
    if (transaction.getTo().isPresent()) {
      final Address to = transaction.getTo().get();
      statelessCost = clampedAdd(statelessCost, accessWitness.accessAccountBasicData(to));
      final boolean sendsValue = !transaction.getValue().equals(Wei.ZERO);
      if(sendsValue){
        //(tx.target, 0, BASIC_DATA_LEAF_KEY)
        statelessCost = clampedAdd(statelessCost, accessWitness.writeAccountBasicData(to));
      }
    }
    //(tx.origin, 0, BASIC_DATA_LEAF_KEY)
    statelessCost = clampedAdd(statelessCost, accessWitness.writeAccountBasicData(transaction.getSender()));

    return statelessCost;
  }

  @Override
  public long callOperationGasCost(
          final MessageFrame frame,
          final long stipend,
          final long inputDataOffset,
          final long inputDataLength,
          final long outputDataOffset,
          final long outputDataLength,
          final Wei transferValue,
          final Address recipient,
          final Address to,
          final boolean accountIsWarm) {
    final long inputDataMemoryExpansionCost =
            memoryExpansionGasCost(frame, inputDataOffset, inputDataLength);
    final long outputDataMemoryExpansionCost =
            memoryExpansionGasCost(frame, outputDataOffset, outputDataLength);
    final long memoryExpansionCost =
            Math.max(inputDataMemoryExpansionCost, outputDataMemoryExpansionCost);

    long cost = clampedAdd(callOperationBaseGasCost(), memoryExpansionCost);

    final MutableAccount recipientAccount = frame.getWorldUpdater().getAccount(recipient);
    if ((recipientAccount == null || recipientAccount.isEmpty()) && !transferValue.isZero()) {
      cost = clampedAdd(cost, newAccountGasCost());
    }
    if(accountIsWarm){
      cost = clampedAdd(cost, getWarmStorageReadCost());
    }

    long statelessGasCost = 0;

    final AccessWitness accessWitness = frame.getAccessWitness();

    if (!transferValue.isZero()) {
      //(caller_address, 0, BASIC_DATA_LEAF_KEY)
      //(callee_address, 0, BASIC_DATA_LEAF_KEY)
      statelessGasCost = clampedAdd(statelessGasCost, accessWitness.accessAccountBasicData(recipient));
      if(!recipient.equals(to)) {
        statelessGasCost = clampedAdd(statelessGasCost, accessWitness.accessAccountBasicData(to));
      }
      //(sender, 0, BASIC_DATA_LEAF_KEY)
      //(recipient, 0, BASIC_DATA_LEAF_KEY)
      statelessGasCost = clampedAdd(statelessGasCost, accessWitness.writeAccountBasicData(recipient));
      if(!recipient.equals(to)) {
        statelessGasCost = clampedAdd(statelessGasCost, accessWitness.writeAccountBasicData(to));
      }
    } else  if(!isPrecompile(to)){
      //(address, 0, BASIC_DATA_LEAF_KEY)
      statelessGasCost = clampedAdd(statelessGasCost, accessWitness.accessAccountBasicData(to));
    }



    return clampedAdd(cost, statelessGasCost);
  }

  @Override
  public long selfDestructOperationGasCost(
          final MessageFrame frame,
          final Account recipient,
          final Address recipientAddress,
          final Wei inheritance,
          final Address originatorAddress) {
    final long gasCost =
            super.selfDestructOperationGasCost(
                    frame, recipient, recipientAddress, inheritance, originatorAddress);

    long statelessGasCost = 0;

    final AccessWitness accessWitness = frame.getAccessWitness();

    statelessGasCost = clampedAdd(statelessGasCost, accessWitness.accessAccountBasicData(recipientAddress));
    if(!recipientAddress.equals(originatorAddress)) {
      statelessGasCost = clampedAdd(statelessGasCost, accessWitness.accessAccountBasicData(originatorAddress));
    }
    //(sender, 0, BASIC_DATA_LEAF_KEY)
    //(recipient, 0, BASIC_DATA_LEAF_KEY)
    statelessGasCost = clampedAdd(statelessGasCost, accessWitness.writeAccountBasicData(recipientAddress));
    if(!recipientAddress.equals(originatorAddress)) {
      statelessGasCost = clampedAdd(statelessGasCost, accessWitness.writeAccountBasicData(originatorAddress));
    }

    return clampedAdd(gasCost, statelessGasCost);
  }

  @Override
  public long getExtCodeSizeOperationGasCost(
          final MessageFrame frame, final boolean accountIsWarm, final Optional<Address> maybeAddress) {
    if (maybeAddress.isPresent()) {
      final Address address = maybeAddress.get();
      if(!isPrecompile(address)){
        //a non-precompile address is the target
        return frame.getAccessWitness().accessAccountBasicData(address);
      }
    }
    return 0L;
  }

  @Override
  public long getBalanceOperationGasCost(
          final MessageFrame frame, final boolean accountIsWarm, final Optional<Address> maybeAddress) {
    if (maybeAddress.isPresent()) {
      final Address address = maybeAddress.get();
      return frame.getAccessWitness().accessAccountBasicData(address);
    }
    return 0L;
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
    long gasCost = copyWordsToMemoryGasCost(frame, 0L, COPY_WORD_GAS_COST, memOffset, readSize);


    long statelessGasCost = 0;
    final AccessWitness accessWitness = frame.getAccessWitness();

    if(!isPrecompile(address)){
      //a non-precompile address is the target
      statelessGasCost = accessWitness.accessAccountBasicData(address);
    }

    if (!frame.wasCreatedInTransaction(frame.getContractAddress())) {
      statelessGasCost =
              clampedAdd(
                      statelessGasCost,
                      accessWitness.touchCodeChunks(address, codeOffset, readSize, codeSize));
    }

    return clampedAdd(gasCost, statelessGasCost);
  }

  @Override
  public long codeCopyOperationGasCost(
          final MessageFrame frame,
          final long memOffset,
          final long codeOffset,
          final long readSize,
          final long codeSize) {
    long gasCost = super.dataCopyOperationGasCost(frame, memOffset, readSize);

    long statelessGasCost = 0;
    final AccessWitness accessWitness = frame.getAccessWitness();
    if (!frame.wasCreatedInTransaction(frame.getContractAddress())) {
      statelessGasCost = accessWitness.accessAccountBasicData(frame.getContractAddress());
      statelessGasCost =
              clampedAdd(
                      statelessGasCost,
                      accessWitness.touchCodeChunks(frame.getContractAddress(), codeOffset, readSize, codeSize));
    }
    return clampedAdd(gasCost, statelessGasCost);
  }

  @Override
  public long extCodeHashOperationGasCost(
          final MessageFrame frame, final boolean accountIsWarm, final Optional<Address> maybeAddress) {
    if (maybeAddress.isPresent()) {
      final Address address = maybeAddress.get();
      return frame
              .getAccessWitness()
              .accessAccountCodeHash(address);
    }
    return 0L;
  }

  @Override
  public long getSloadOperationGasCost(
          final MessageFrame frame, final UInt256 key, final boolean slotIsWarm) {
    return frame
            .getAccessWitness()
            .accessAccountStorage(frame.getContractAddress(), key);
  }

  @Override
  public long calculateStorageCost(
          final MessageFrame frame,
          final UInt256 key,
          final UInt256 newValue,
          final Supplier<UInt256> currentValue,
          final Supplier<UInt256> originalValue) {

    long gasCost = 0;

    final UInt256 localCurrentValue = currentValue.get();
    if (localCurrentValue.equals(newValue)) {
      gasCost=SLOAD_GAS;
    } else {
      final UInt256 localOriginalValue = originalValue.get();
      if (!localOriginalValue.equals(localCurrentValue)) {
        gasCost=SLOAD_GAS;
      }
    }

    long statelessGasCost = 0;
    final AccessWitness accessWitness = frame.getAccessWitness();

    statelessGasCost= clampedAdd(statelessGasCost,
            accessWitness.accessAccountStorage(frame.getContractAddress(), key));
    statelessGasCost= clampedAdd(statelessGasCost,
            accessWitness.writeAccountStorage(frame.getContractAddress(), key));

    return clampedAdd(gasCost,statelessGasCost);
  }


  @Override
  public long pushOperationGasCost(
          final MessageFrame frame, final long codeOffset, final long readSize, final long codeSize) {
    long gasCost = super.pushOperationGasCost(frame, codeOffset, readSize, codeSize);

    long statelessGasCost = 0;
    if (!frame.wasCreatedInTransaction(frame.getContractAddress())) {
      statelessGasCost = frame
                              .getAccessWitness()
                              .touchCodeChunks(frame.getContractAddress(), codeOffset, readSize, codeSize);
    }
    return clampedAdd(gasCost,statelessGasCost);
  }


  @Override
  public long txCreateCost(final MessageFrame frame) {
    final long statelessGasCost = frame.getAccessWitness().writeAccountBasicData(frame.getContractAddress());
    return clampedAdd(CREATE_OPERATION_GAS_COST, statelessGasCost);
  }

  @Override
  public long initcodeCost(final int initCodeLength) {
    return super.initcodeCost(initCodeLength);
  }


  @Override
  public long codeDepositGasCost(final MessageFrame frame, final int codeSize) {
    return frame
        .getAccessWitness()
        .touchCodeChunksUponContractCreation(frame.getContractAddress(), codeSize);
  }

  @Override
  public long completedCreateContractGasCost(final MessageFrame frame) {
    final AccessWitness accessWitness = frame.getAccessWitness();
    long statelessCost = accessWitness.writeAccountBasicData(frame.getContractAddress());
    return clampedAdd(statelessCost, accessWitness.writeAccountBasicData(frame.getContractAddress()));
  }

  @Override
  public AccessWitness newAccessWitness() {
    return new Eip4762AccessWitness();
  }
}
