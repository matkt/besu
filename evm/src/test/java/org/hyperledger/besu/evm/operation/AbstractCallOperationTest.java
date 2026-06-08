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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.frame.SoftFailureReason;
import org.hyperledger.besu.evm.gascalculator.AmsterdamGasCalculator;
import org.hyperledger.besu.evm.internal.Words;
import org.hyperledger.besu.evm.testutils.TestMessageFrameBuilder;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbstractCallOperationTest {

  private static final Address CALLER =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address TARGET =
      Address.fromHexString("0x2222222222222222222222222222222222222222");

  private final WorldUpdater worldUpdater = mock(WorldUpdater.class);
  private final Account callerAccount = mock(Account.class);
  private final CallOperation operation = new CallOperation(new AmsterdamGasCalculator());

  @BeforeEach
  void setUp() {
    when(worldUpdater.updater()).thenReturn(worldUpdater);
  }

  @Test
  void amsterdamDoesNotReadTargetAccountWhenDepthCheckFails() {
    final MessageFrame frame = callFrame(Wei.ZERO);
    while (frame.getDepth() < 1024) {
      frame.getMessageFrameStack().addFirst(frame);
    }

    final Operation.OperationResult result = operation.execute(frame, null);

    assertThat(result.getSoftFailureReason()).contains(SoftFailureReason.LEGACY_MAX_CALL_DEPTH);
    verify(worldUpdater, never()).get(TARGET);
  }

  @Test
  void amsterdamDoesNotReadTargetAccountWhenBalanceCheckFails() {
    when(worldUpdater.get(CALLER)).thenReturn(callerAccount);
    when(callerAccount.getBalance()).thenReturn(Wei.ZERO);

    final Operation.OperationResult result = operation.execute(callFrame(Wei.ONE), null);

    assertThat(result.getSoftFailureReason())
        .contains(SoftFailureReason.LEGACY_INSUFFICIENT_BALANCE);
    verify(worldUpdater).get(CALLER);
    verify(worldUpdater, never()).get(TARGET);
  }

  @Test
  void amsterdamReadsTargetAccountAfterEntryChecksPass() {
    final Operation.OperationResult result = operation.execute(callFrame(Wei.ZERO), null);

    assertThat(result.getHaltReason()).isNull();
    verify(worldUpdater).get(TARGET);
  }

  private MessageFrame callFrame(final Wei transferValue) {
    final MessageFrame frame =
        new TestMessageFrameBuilder()
            .address(CALLER)
            .contract(CALLER)
            .sender(CALLER)
            .worldUpdater(worldUpdater)
            .initialGas(1_000_000L)
            .build();

    frame.pushStackItem(UInt256.ZERO); // output length
    frame.pushStackItem(UInt256.ZERO); // output offset
    frame.pushStackItem(UInt256.ZERO); // input length
    frame.pushStackItem(UInt256.ZERO); // input offset
    frame.pushStackItem(transferValue);
    frame.pushStackItem(Words.fromAddress(TARGET));
    frame.pushStackItem(UInt256.valueOf(10_000L)); // child gas
    return frame;
  }
}
