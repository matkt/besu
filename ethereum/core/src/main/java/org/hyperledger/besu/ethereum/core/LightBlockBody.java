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
package org.hyperledger.besu.ethereum.core;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.List;
import java.util.Optional;

public class LightBlockBody  extends BlockBody {

  final Bytes rlp;
  final Bytes32 transactionRoot;
  final Bytes32 ommersHash;

  public LightBlockBody(final List<Transaction> transactions, final List<BlockHeader> ommers, final Bytes rlp, final Bytes32 transactionRoot, final Bytes32 ommersHash) {
    super(transactions, ommers);
    this.rlp = rlp;
    this.transactionRoot = transactionRoot;
    this.ommersHash = ommersHash;
  }

  public static LightBlockBody readFrom(
      final RLPInput input, final BlockHeaderFunctions blockHeaderFunctions) {
    Bytes raw = input.raw();
    input.reset();
    input.enterList();

    List<Transaction> transactions = input.readList(Transaction::readFrom);
    Bytes32 transactionRoot= BodyValidation.transactionsRoot(transactions);
    List<BlockHeader> ommers = input.readList(rlp -> LightBlockHeader.readFrom(rlp, blockHeaderFunctions));
    Bytes32 ommersHash= BodyValidation.ommersHash(ommers);

    final LightBlockBody body =
        new LightBlockBody(
                transactions, ommers,raw, transactionRoot, ommersHash);
    input.leaveList();
    return body;
  }

  @Override
  public Optional<Bytes> getRlp() {
    return Optional.of(rlp);
  }

  @Override
  public Optional<Bytes32> getTransactionRoot() {
    return Optional.of(transactionRoot);
  }

  @Override
  public Optional<Bytes32> getOmmersHash() {
    return Optional.of(ommersHash);
  }
}
