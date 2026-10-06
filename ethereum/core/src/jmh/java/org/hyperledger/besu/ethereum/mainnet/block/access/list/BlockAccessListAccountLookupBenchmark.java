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
package org.hyperledger.besu.ethereum.mainnet.block.access.list;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Indexing the block access list of a block: the state root committer and the parallel execution
 * each built their own index, now they share one. Every invocation is a new block, whose block
 * access list has new addresses, as when it is decoded from a payload.
 */
@State(Scope.Thread)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BlockAccessListAccountLookupBenchmark {

  /** Accounts in the block access list, e.g. a block of 10k ether transfers to new receivers. */
  @Param({"20000"})
  public int accounts;

  /** Storage slots changed per account. */
  @Param({"0", "4"})
  public int storageSlotsPerAccount;

  private BlockAccessList blockAccessList;
  private Address firstAddress;

  @Setup(Level.Invocation)
  public void newBlock() {
    final List<BlockAccessList.AccountChanges> accountChanges = new ArrayList<>(accounts);
    for (int i = 0; i < accounts; i++) {
      final List<BlockAccessList.SlotChanges> slotChanges = new ArrayList<>(storageSlotsPerAccount);
      for (int s = 0; s < storageSlotsPerAccount; s++) {
        slotChanges.add(
            new BlockAccessList.SlotChanges(
                new StorageSlotKey(UInt256.fromBytes(Bytes.random(32))),
                List.of(new BlockAccessList.StorageChange(i, UInt256.valueOf(s + 1)))));
      }
      accountChanges.add(
          new BlockAccessList.AccountChanges(
              Address.wrap(Bytes.random(Address.SIZE)),
              slotChanges,
              List.of(),
              List.of(new BlockAccessList.BalanceChange(i, Wei.of(i + 1L))),
              List.of(),
              List.of()));
    }
    blockAccessList = new BlockAccessList(accountChanges);
    firstAddress = accountChanges.getFirst().address();
  }

  /** Before: the state root committer and the parallel execution each index it. */
  @Benchmark
  public void indexedByEachUser(final Blackhole blackhole) {
    final BlockAccessListAccountLookup forStateRoot =
        BlockAccessListAccountLookup.of(blockAccessList);
    blackhole.consume(forStateRoot.getAccountChanges(firstAddress));
    final BlockAccessListAccountLookup forExecution =
        BlockAccessListAccountLookup.of(blockAccessList);
    blackhole.consume(forExecution.getAccountChanges(firstAddress));
  }

  /** After: both use one lookup, indexed once. */
  @Benchmark
  public void indexedOnceAndShared(final Blackhole blackhole) {
    final BlockAccessListAccountLookup shared = BlockAccessListAccountLookup.of(blockAccessList);
    blackhole.consume(shared.getAccountChanges(firstAddress));
    blackhole.consume(shared.getAccountChanges(firstAddress));
  }
}
