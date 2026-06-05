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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.FrozenSnapTrieNodeStorage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Disk-to-disk comparison: frozen PlainTable RocksDB read by hash (snap-sync snapshot, keyed by
 * node hash) vs. main Bonsai RocksDB trie-branch read by location (post-sync). Both datasets are
 * persisted on disk; the PlainTable store is frozen and reopened read-only before measurement.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FrozenTrieNodeReadBenchmark {

  @Param({"1000", "10000"})
  int nodeCount;

  private List<Bytes32> hashes;
  private List<Bytes> locations;
  private List<byte[]> nodePayloads;
  private SegmentedKeyValueStorage rocksDbStorage;
  private FrozenSnapTrieNodeStorage plainTableStorage;
  private Path benchRootDirectory;
  private int cursor;

  @Setup(Level.Trial)
  public void setUpTrial() throws Exception {
    hashes = new ArrayList<>(nodeCount);
    locations = new ArrayList<>(nodeCount);
    nodePayloads = new ArrayList<>(nodeCount);
    for (int i = 0; i < nodeCount; i++) {
      final Bytes payload = Bytes.repeat((byte) (i & 0xff), 80 + (i % 48));
      nodePayloads.add(payload.toArrayUnsafe());
      hashes.add(Bytes32.wrap(Hash.hash(payload).getBytes()));
      locations.add(Bytes.fromHexString(String.format("0x%040x", i)));
    }

    benchRootDirectory = Files.createTempDirectory("besu-trie-node-disk-bench");
    final Path rocksDbDirectory = benchRootDirectory.resolve("rocksdb-trie-branch");
    final Path plainTableDirectory = benchRootDirectory.resolve("rocksdb-plain-table");

    final FrozenTrieNodeDiskBenchmarkStorage diskRocksDb =
        FrozenTrieNodeDiskBenchmarkStorage.create(rocksDbDirectory);
    rocksDbStorage = diskRocksDb.openTrieBranchStorage();
    final SegmentedKeyValueStorageTransaction rocksTx = rocksDbStorage.startTransaction();
    for (int i = 0; i < nodeCount; i++) {
      rocksTx.put(TRIE_BRANCH_STORAGE, locations.get(i).toArrayUnsafe(), nodePayloads.get(i));
    }
    rocksTx.commit();
    // Keep RocksDB open: the factory shares one DB instance; close() would break reads.

    plainTableStorage = FrozenSnapTrieNodeStorage.open(plainTableDirectory);
    for (int i = 0; i < nodeCount; i++) {
      plainTableStorage.put(hashes.get(i), Bytes.wrap(nodePayloads.get(i)));
    }
    plainTableStorage.freeze();
    plainTableStorage.close();
    plainTableStorage = FrozenSnapTrieNodeStorage.open(plainTableDirectory);

    cursor = 0;
  }

  @TearDown(Level.Trial)
  public void tearDownTrial() throws Exception {
    if (rocksDbStorage != null) {
      rocksDbStorage.close();
      rocksDbStorage = null;
    }
    if (plainTableStorage != null) {
      plainTableStorage.close();
      plainTableStorage = null;
    }
    FrozenTrieNodeDiskBenchmarkStorage.deleteRecursively(benchRootDirectory);
  }

  @Setup(Level.Invocation)
  public void rotateCursor() {
    cursor = (cursor + 1) % nodeCount;
  }

  @Benchmark
  public void readPlainTableByHash(final Blackhole blackhole) {
    final Bytes value = plainTableStorage.get(hashes.get(cursor)).orElseThrow();
    blackhole.consume(value);
  }

  @Benchmark
  public void readRocksDbByLocation(final Blackhole blackhole) {
    final byte[] value =
        rocksDbStorage
            .get(TRIE_BRANCH_STORAGE, locations.get(cursor).toArrayUnsafe())
            .orElseThrow();
    blackhole.consume(value);
  }
}
