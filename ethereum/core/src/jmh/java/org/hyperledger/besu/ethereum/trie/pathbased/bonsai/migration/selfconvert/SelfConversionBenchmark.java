/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.selfconvert;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile.AccountPreimages;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter.Entry;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappableKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfigurationBuilder;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.OptimisticRocksDBColumnarKeyValueStorage;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * The self-conversion's choices, measured on a synthetic RocksDB flat database: 1M accounts, 100k
 * contracts of 30 slots sharing 5k codes of 1 KB. Built once under {@code build/jmh-selfconvert}.
 */
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx8g"})
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SelfConversionBenchmark {

  private static final int ACCOUNTS = 1_000_000;
  private static final int CONTRACT_EVERY = 10;
  private static final int SLOTS_PER_CONTRACT = 30;
  private static final int CODES = 50_000;
  private static final int CODE_BYTES = 1024;

  /** The dataset, opened by each fork; built by the first one. */
  @State(Scope.Benchmark)
  public static class Dataset {
    SegmentedKeyValueStorage database;
    BonsaiWorldStateKeyValueStorage storage;
    SnappableKeyValueStorage flat;
    Path preimages;
    BlockHeader target;
    List<Bytes32> codeHashes;
    byte[][] hotAccounts;

    @Setup(Level.Trial)
    public void open() throws IOException {
      final Path dir = Path.of(System.getProperty("user.dir"), "build", "jmh-selfconvert");
      final boolean built = Files.exists(dir.resolve("ready"));
      if (!built && Files.exists(dir)) {
        MoreFiles.deleteRecursively(dir, RecursiveDeleteOption.ALLOW_INSECURE);
      }
      // RocksDB creates the database directory, not its parents
      Files.createDirectories(dir);
      database =
          new OptimisticRocksDBColumnarKeyValueStorage(
              new RocksDBConfigurationBuilder().databaseDir(dir.resolve("db")).build(),
              Arrays.stream(KeyValueSegmentIdentifier.values())
                  .filter(segment -> segment.includeInDatabaseFormat(DataStorageFormat.BONSAI))
                  .<SegmentIdentifier>map(segment -> segment)
                  .toList(),
              List.of(),
              new NoOpMetricsSystem(),
              RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS);
      storage =
          (BonsaiWorldStateKeyValueStorage)
              new KeyValueStorageProvider(
                      segments -> database, new InMemoryKeyValueStorage(), new NoOpMetricsSystem())
                  .createWorldStateStorage(DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
      flat = (SnappableKeyValueStorage) storage.getComposedWorldStateStorage();
      preimages = dir.resolve("preimages");
      target = new BlockHeaderTestFixture().number(1).buildHeader();
      // the codes the contracts hold: contract i holds code i % CODES
      codeHashes =
          IntStream.iterate(0, i -> i < CODES, i -> i + CONTRACT_EVERY)
              .mapToObj(code -> Bytes32.wrap(Hash.hash(code(code)).getBytes()))
              .sorted(Bytes::compareTo)
              .toList();
      final Random random = new Random(42);
      hotAccounts = new byte[50_000][];
      for (int i = 0; i < hotAccounts.length; i++) {
        hotAccounts[i] = FlatState.account(address(random.nextInt(ACCOUNTS)));
      }
      if (!built) {
        build();
        Files.writeString(dir.resolve("ready"), "");
      }
    }

    private void build() throws IOException {
      final List<AccountPreimages> records = new ArrayList<>(ACCOUNTS);
      BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
      for (int i = 0; i < ACCOUNTS; i++) {
        final Address address = address(i);
        final boolean contract = i % CONTRACT_EVERY == 0;
        final Bytes code = contract ? code(i % CODES) : Bytes.EMPTY;
        final Hash codeHash = Hash.hash(code);
        updater.putAccountInfoState(
            address.addressHash(),
            RLP.encode(
                new PatriciaTrieAccountValue(i, Wei.of(i + 1L), Hash.EMPTY_TRIE_HASH, codeHash)
                    ::writeTo));
        final List<Bytes32> slots = new ArrayList<>();
        if (contract) {
          updater.putCode(address.addressHash(), codeHash, code);
          for (int slot = 0; slot < SLOTS_PER_CONTRACT; slot++) {
            final Bytes32 key = Bytes32.leftPad(Bytes.ofUnsignedInt(slot));
            updater.putStorageValueBySlotHash(
                address.addressHash(), Hash.hash(key), UInt256.valueOf(slot + 1L));
            slots.add(key);
          }
        }
        records.add(new AccountPreimages(address, slots));
        if (i % 20_000 == 19_999) {
          updater.commit();
          updater = storage.updater();
        }
      }
      updater.putFlatDbBlockHash(target.getBlockHash());
      updater.commit();
      storage.upgradeToFullFlatDbMode();
      Eip8347PreimageFile.write(preimages, records);
    }

    @TearDown(Level.Trial)
    public void close() throws IOException {
      database.close();
    }
  }

  // ---- How many entries per short-lived snapshot ----

  @State(Scope.Benchmark)
  public static class Batches {
    @Param({"1000", "10000", "100000"})
    public int entries;
  }

  /** Reads the whole flat database, range by range, a snapshot per batch. */
  @Benchmark
  public long scan(final Dataset data, final Batches batches) throws IOException {
    long entries = 0;
    for (int range = 0; range < FlatState.RANGES; range++) {
      final FlatState.Batches reader = new FlatState.Batches(data.flat, range, batches.entries);
      FlatState.Batch batch;
      do {
        batch = reader.next();
        entries += batch.entries().size();
      } while (!batch.last());
    }
    return entries;
  }

  // ---- How to read a code by hash ----

  @State(Scope.Benchmark)
  public static class CodeReads {
    @Param({"get", "seek"})
    public String mode;
  }

  /** Reads every code by hash, uncached, 32 codes per snapshot as the code phase does. */
  @Benchmark
  public long codes(final Dataset data, final CodeReads reads) throws IOException {
    long bytes = 0;
    for (int first = 0; first < data.codeHashes.size(); first += 32) {
      try (final SnappedKeyValueStorage snapshot = data.flat.takeSnapshot()) {
        for (final Bytes32 hash :
            data.codeHashes.subList(first, Math.min(first + 32, data.codeHashes.size()))) {
          bytes += read(snapshot, hash.toArrayUnsafe(), reads.mode).length;
        }
      }
    }
    return bytes;
  }

  private static byte[] read(
      final SnappedKeyValueStorage snapshot, final byte[] key, final String mode) {
    if (mode.equals("get")) {
      return snapshot.getUncached(CODE_STORAGE, key).orElseThrow();
    }
    try (final Stream<Pair<byte[], byte[]>> entries =
        snapshot.streamFromKeyUncached(CODE_STORAGE, key)) {
      return entries
          .findFirst()
          .filter(e -> Arrays.equals(e.getKey(), key))
          .orElseThrow()
          .getValue();
    }
  }

  // ---- Where the ranges start in the preimage file ----

  @State(Scope.Benchmark)
  public static class RangeIndex {
    @Param({"sampled", "everyAccountHashed"})
    public String mode;
  }

  @Benchmark
  public long[] preimageRanges(final Dataset data, final RangeIndex index) throws IOException {
    return index.mode.equals("sampled")
        ? RangeScan.starts(data.preimages)
        : startsHashingEveryAccount(data.preimages);
  }

  private static long[] startsHashingEveryAccount(final Path path) throws IOException {
    final long[] starts = new long[FlatState.RANGES + 1];
    Arrays.fill(starts, Files.size(path));
    try (final Eip8347PreimageFile file = Eip8347PreimageFile.withoutOrderCheck(path)) {
      int nextRange = 0;
      long offset = file.position();
      for (Eip8347PreimageFile.Account account = file.nextAccount();
          account != null;
          account = file.nextAccount()) {
        final int range = FlatState.rangeOf(FlatState.account(account.address()));
        while (nextRange <= range) {
          starts[nextRange++] = offset;
        }
        file.skipSlots();
        offset = file.position();
      }
    }
    return starts;
  }

  // ---- Can one thread sort the leaves behind the sink ----

  @State(Scope.Benchmark)
  public static class Sorting {
    @Param({"1", "4", "12"})
    public int sortThreads;

    ForkJoinPool pool;
    Path dir;
    byte[][] keys;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
      pool = new ForkJoinPool(sortThreads);
      dir = Files.createTempDirectory("jmh-sort");
      final Random random = new Random(7);
      keys = new byte[5_000_000][];
      for (int i = 0; i < keys.length; i++) {
        keys[i] = new byte[32];
        random.nextBytes(keys[i]);
      }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      pool.shutdownNow();
      MoreFiles.deleteRecursively(dir, RecursiveDeleteOption.ALLOW_INSECURE);
    }
  }

  /** 5M leaves added from one thread, as the sink does, with 64 MB buffers; then drained. */
  @Benchmark
  public long sortLeaves(final Sorting sorting) throws IOException {
    final byte[] value = new byte[] {1, 2, 3, 4};
    long count = 0;
    try (final Eip8347ExternalSorter sorter =
        Eip8347ExternalSorter.partitioned(sorting.dir, "leaves", 64L << 20, sorting.pool, 16)) {
      for (final byte[] key : sorting.keys) {
        sorter.add(key, value);
      }
      final Iterator<Entry> sorted = sorter.sorted();
      while (sorted.hasNext()) {
        sorted.next();
        count++;
      }
    }
    return count;
  }

  // ---- The whole conversion, by threads ----

  @State(Scope.Benchmark)
  public static class Conversion {
    @Param({"1", "2", "4", "8"})
    public int threads;

    ForkJoinPool pool;
    Path workDir;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
      pool = new ForkJoinPool(threads);
      workDir = Files.createTempDirectory("jmh-selfconvert-work");
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      pool.shutdownNow();
      MoreFiles.deleteRecursively(workDir, RecursiveDeleteOption.ALLOW_INSECURE);
    }
  }

  @Benchmark
  public long convert(final Dataset data, final Conversion conversion) throws IOException {
    return new SelfConversion(
            data.storage, chainOf(data.target), conversion.workDir, 64L << 20, conversion.pool)
        .convert(data.preimages, data.target, data.target, (location, hash, value) -> {})
        .leafCount();
  }

  private static SelfConversion.Chain chainOf(final BlockHeader target) {
    return new SelfConversion.Chain() {
      @Override
      public Optional<BlockHeader> header(final Hash blockHash) {
        return Optional.of(target).filter(header -> header.getBlockHash().equals(blockHash));
      }

      @Override
      public BlockHeader canonicalHeader(final long number) {
        return target;
      }

      @Override
      public Optional<TrieLog> trieLog(final Hash blockHash) {
        return Optional.empty();
      }

      @Override
      public List<BlockAccessList> bals(final List<BlockHeader> blocks) {
        return List.of();
      }
    };
  }

  // ---- What a scan costs the reads of block import ----

  @State(Scope.Benchmark)
  public static class ScanMode {
    @Param({"none", "uncached", "cached"})
    public String mode;
  }

  /** A scanning thread's position: its range and the key it goes on from. */
  @State(Scope.Thread)
  public static class ScanPosition {
    int range;
    byte[] from = new byte[] {0};

    @Setup(Level.Trial)
    public void setUp() {
      range = (int) (Thread.currentThread().threadId() * 37 % FlatState.RANGES);
      from = new byte[] {(byte) range};
    }
  }

  /** A read of a hot account, as block import reads the state, while the scanners run. */
  @Benchmark
  @Group("importDuringScan")
  @GroupThreads(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Warmup(iterations = 2, time = 5)
  @Measurement(iterations = 3, time = 10)
  public Optional<byte[]> importRead(final Dataset data) {
    final byte[] key =
        data.hotAccounts[ThreadLocalRandom.current().nextInt(data.hotAccounts.length)];
    return data.database.get(ACCOUNT_INFO_STATE, key);
  }

  /** One batch of a scan of the accounts the reads hit, through the block cache or not. */
  @Benchmark
  @Group("importDuringScan")
  @GroupThreads(4)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Warmup(iterations = 2, time = 5)
  @Measurement(iterations = 3, time = 10)
  public int scanBatch(final Dataset data, final ScanMode scan, final ScanPosition position)
      throws IOException {
    if (scan.mode.equals("none")) {
      return 0;
    }
    final boolean uncached = scan.mode.equals("uncached");
    int read = 0;
    try (final SnappedKeyValueStorage snapshot = data.flat.takeSnapshot();
        final Stream<Pair<byte[], byte[]>> entries =
            uncached
                ? snapshot.streamFromKeyUncached(ACCOUNT_INFO_STATE, position.from)
                : snapshot.streamFromKey(ACCOUNT_INFO_STATE, position.from)) {
      final Iterator<Pair<byte[], byte[]>> it = entries.iterator();
      byte[] last = null;
      while (read < SelfConversion.DEFAULT_BATCH_ENTRIES && it.hasNext()) {
        last = it.next().getKey();
        read++;
      }
      position.from = last == null ? new byte[] {0} : FlatState.after(last);
    }
    return read;
  }

  private static Address address(final int index) {
    return Address.fromHexString(String.format("0x%040x", index + 1));
  }

  /** A deterministic 1 KB code, not a delegation indicator. */
  private static Bytes code(final int index) {
    final byte[] code = new byte[CODE_BYTES];
    for (int i = 0; i < code.length; i++) {
      code[i] = (byte) (0x60 + (index + i) % 32);
    }
    code[code.length - 4] = (byte) (index >>> 24);
    code[code.length - 3] = (byte) (index >>> 16);
    code[code.length - 2] = (byte) (index >>> 8);
    code[code.length - 1] = (byte) index;
    return Bytes.wrap(code);
  }
}
