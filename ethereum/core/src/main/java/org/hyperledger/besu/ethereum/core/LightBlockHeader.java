/*
 * Copyright Hyperledger Besu Contributors.
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

import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.evm.log.LogsBloomFilter;

import java.util.Optional;

/** A mined Ethereum block header. */
public class LightBlockHeader extends BlockHeader {

  protected final Hash hash;

  protected Bytes rlp;

  public LightBlockHeader(final Hash parentHash,
                          final Hash ommersHash,
                          final Hash hash,
                          final Address coinbase,
                          final Hash stateRoot,
                          final Hash transactionsRoot,
                          final Hash receiptsRoot,
                          final LogsBloomFilter logsBloom,
                          final Difficulty difficulty,
                          final long number,
                          final long gasLimit,
                          final long gasUsed,
                          final long timestamp,
                          final Bytes32 mixHashOrPrevRandao,
                          final long nonce,
                          final Wei baseFee,
                          final Bytes rlp) {
    super(parentHash,
            ommersHash,
            coinbase,
            stateRoot,transactionsRoot,receiptsRoot,logsBloom,difficulty,number,gasLimit,gasUsed,timestamp,null, baseFee, mixHashOrPrevRandao, nonce, null);
    this.rlp = rlp;
    this.hash = hash;
  }

  public static LightBlockHeader readFrom(final RLPInput input) {
    try {
      Bytes raw = input.raw();
      input.reset();
      input.enterList();
      final Hash parentHash = Hash.wrap(input.readBytes32());
      final Hash ommersHash = Hash.wrap(input.readBytes32());
      final Address coinbase = Address.readFrom(input);
      final Hash stateRoot = Hash.wrap(input.readBytes32());
      final Hash transactionsRoot = Hash.wrap(input.readBytes32());
      final Hash receiptsRoot = Hash.wrap(input.readBytes32());
      final LogsBloomFilter logsBloom = LogsBloomFilter.readFrom(input);
      final Difficulty difficulty = Difficulty.of(input.readUInt256Scalar());
      final long number = input.readLongScalar();
      final long gasLimit = input.readLongScalar();
      final long gasUsed = input.readLongScalar();
      final long timestamp = input.readLongScalar();
      input.skipNext();
      final Bytes32 mixHashOrPrevRandao = input.readBytes32();
      final long nonce = input.readLong();
      final Wei baseFee = !input.isEndOfCurrentList() ? Wei.of(input.readUInt256Scalar()) : null;
      input.leaveListLenient();
      return new LightBlockHeader(parentHash, ommersHash, Hash.hash(raw), coinbase, stateRoot, transactionsRoot, receiptsRoot, logsBloom, difficulty, number,
              gasLimit, gasUsed, timestamp, mixHashOrPrevRandao, nonce, baseFee, raw);
    }catch (Exception e){
      e.printStackTrace();
      throw e;
    }
  }

  @Override
  public Hash getHash() {
    return hash;
  }

  @Override
  public Optional<Bytes> getRlp() {
    return Optional.of(rlp);
  }
}
