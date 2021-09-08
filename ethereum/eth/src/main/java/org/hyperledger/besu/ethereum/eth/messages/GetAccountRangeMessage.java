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

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.ArrayList;
import java.util.Collection;

public final class GetAccountRangeMessage extends AbstractMessageData {

  public static GetAccountRangeMessage readFrom(final MessageData message) {
    if (message instanceof GetAccountRangeMessage) {
      return (GetAccountRangeMessage) message;
    }
    final int code = message.getCode();
    if (code != SnapV1.GET_ACCOUNT_RANGE) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a GetAccountRangeMessage.", code));
    }
    return new GetAccountRangeMessage(message.getData());
  }

  public static GetAccountRangeMessage create(
                                              final long requestId,
                                              final Hash rootHash,
                                              final Hash startingHash,
                                              final Hash endingHash) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeLongScalar(requestId);
    tmp.writeBytes(rootHash);
    tmp.writeBytes(startingHash);
    tmp.writeBytes(endingHash);
    tmp.writeIntScalar(512 * 1024);
    tmp.endList();
    return new GetAccountRangeMessage(tmp.encoded());
  }

  private GetAccountRangeMessage(final Bytes data) {
    super(data);
  }

  @Override
  public int getCode() {
    return SnapV1.GET_ACCOUNT_RANGE;
  }

}
