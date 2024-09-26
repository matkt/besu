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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Transaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.log.Log;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldView;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;

public abstract class ParallelizedOperationTracer implements OperationTracer {
  private final OperationTracer delegate;

  public ParallelizedOperationTracer(final OperationTracer delegate) {
    this.delegate = delegate;
  }

  @Override
  public void tracePreExecution(final MessageFrame frame) {
    delegate.tracePreExecution(frame);
  }

  @Override
  public void tracePostExecution(
      final MessageFrame frame, final Operation.OperationResult operationResult) {
    delegate.tracePostExecution(frame, operationResult);
  }

  @Override
  public void tracePrecompileCall(
      final MessageFrame frame, final long gasRequirement, final Bytes output) {
    delegate.tracePrecompileCall(frame, gasRequirement, output);
  }

  @Override
  public void traceAccountCreationResult(
      final MessageFrame frame, final Optional<ExceptionalHaltReason> haltReason) {
    delegate.traceAccountCreationResult(frame, haltReason);
  }

  @Override
  public void tracePrepareTransaction(final WorldView worldView, final Transaction transaction) {
    delegate.tracePrepareTransaction(worldView, transaction);
  }

  @Override
  public void traceStartTransaction(final WorldView worldView, final Transaction transaction) {
    delegate.traceStartTransaction(worldView, transaction);
  }

  @Override
  public void traceBeforeRewardTransaction(
      final WorldView worldView, final Transaction tx, final Wei miningReward) {
    catchBeforeRewardTransaction(worldView, tx, miningReward);
    delegate.traceBeforeRewardTransaction(worldView, tx, miningReward);
  }

  @Override
  public void traceEndTransaction(
      final WorldView worldView,
      final Transaction tx,
      final boolean status,
      final Bytes output,
      final List<Log> logs,
      final long gasUsed,
      final Set<Address> selfDestructs,
      final long timeNs) {
    delegate.traceEndTransaction(
        worldView, tx, status, output, logs, gasUsed, selfDestructs, timeNs);
  }

  @Override
  public void traceContextEnter(final MessageFrame frame) {
    delegate.traceContextEnter(frame);
  }

  @Override
  public void traceContextReEnter(final MessageFrame frame) {
    delegate.traceContextReEnter(frame);
  }

  @Override
  public void traceContextExit(final MessageFrame frame) {
    delegate.traceContextExit(frame);
  }

  @Override
  public boolean isExtendedTracing() {
    return delegate.isExtendedTracing();
  }

  abstract void catchBeforeRewardTransaction(
      final WorldView worldView, final Transaction tx, final Wei miningReward);
}
