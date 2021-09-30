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
package org.hyperledger.besu.ethereum.eth.messages;

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.immutables.value.Value;

public final class ByteCodesMessage extends AbstractSnapMessageData {

  public static ByteCodesMessage readFrom(final MessageData message) {
    if (message instanceof ByteCodesMessage) {
      return (ByteCodesMessage) message;
    }
    final int code = message.getCode();
    if (code != SnapV1.BYTECODES) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a ByteCodesMessage.", code));
    }
    return new ByteCodesMessage(message.getData());
  }

  public static ByteCodesMessage create(final List<Bytes> codes) {
    return create(Optional.empty(), codes);
  }

  public static ByteCodesMessage create(
      final Optional<BigInteger> requestId, final List<Bytes> codes) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    requestId.ifPresent(tmp::writeBigIntegerScalar);
    tmp.writeList(codes, (code, rlpOutput) -> rlpOutput.writeBytes(code));
    tmp.endList();
    return new ByteCodesMessage(tmp.encoded());
  }

  private ByteCodesMessage(final Bytes data) {
    super(data);
  }

  @Override
  protected Bytes wrap(final BigInteger requestId) {
    final ByteCodesMessage.ByteCodes bytecodes = bytecodes(false);
    return create(Optional.of(requestId), bytecodes.codes()).getData();
  }

  @Override
  public int getCode() {
    return SnapV1.BYTECODES;
  }

  public ByteCodes bytecodes(final boolean withRequestId) {
    final RLPInput input = new BytesValueRLPInput(data, false);
    input.enterList();
    if (withRequestId) input.skipNext();
    final ImmutableByteCodes byteCodes =
        ImmutableByteCodes.builder().codes(input.readList(RLPInput::readBytes)).build();
    input.leaveList();
    return byteCodes;
  }

  @Value.Immutable
  public interface ByteCodes {

    List<Bytes> codes();
  }
}
