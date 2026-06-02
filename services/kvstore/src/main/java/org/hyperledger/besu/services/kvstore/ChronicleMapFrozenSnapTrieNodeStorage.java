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
package org.hyperledger.besu.services.kvstore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import net.openhft.chronicle.map.ChronicleMap;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@value FrozenSnapTrieNodeChronicleConfig#NUM_SHARDS} Chronicle Map files; each shard has a
 * dedicated writer thread so parallel snap-sync {@code commit()} flushes do not queue on one mmap.
 */
public class ChronicleMapFrozenSnapTrieNodeStorage implements FrozenSnapTrieNodeStorage {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChronicleMapFrozenSnapTrieNodeStorage.class);

  private static final String FROZEN_MARKER_FILE = ".frozen";

  private final Path directory;
  private final Shard[] shards;
  private final AtomicBoolean frozen = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private ChronicleMapFrozenSnapTrieNodeStorage(final Path directory, final boolean readOnly) {
    this.directory = directory;
    if (readOnly) {
      frozen.set(true);
    }
    this.shards = new Shard[FrozenSnapTrieNodeChronicleConfig.NUM_SHARDS];
    for (int i = 0; i < shards.length; i++) {
      shards[i] = new Shard(i, directory.resolve(FrozenSnapTrieNodeChronicleConfig.shardFileName(i)));
      if (readOnly && Files.exists(shards[i].mapFile)) {
        shards[i].map = shards[i].openMap(true);
      }
    }
  }

  public static ChronicleMapFrozenSnapTrieNodeStorage open(final Path directory) {
    try {
      Files.createDirectories(directory);
    } catch (final IOException e) {
      throw new IllegalStateException(
          "Failed to create Chronicle Map directory " + directory, e);
    }
    warnIfLegacySingleFilePresent(directory);
    final boolean alreadyFrozen = Files.exists(directory.resolve(FROZEN_MARKER_FILE));
    return new ChronicleMapFrozenSnapTrieNodeStorage(directory, alreadyFrozen);
  }

  private static void warnIfLegacySingleFilePresent(final Path directory) {
    final Path legacy = directory.resolve(FrozenSnapTrieNodeChronicleConfig.LEGACY_MAP_FILE_NAME);
    if (Files.exists(legacy)) {
      LOG.warn(
          "Legacy single-file map {} found; delete {} and re-run snap sync for sharded storage",
          legacy,
          directory);
    }
  }

  @Override
  public void put(final Bytes32 hash, final Bytes value) {
    ensureWritable();
    final int shard = FrozenSnapTrieNodeChronicleConfig.shardIndex(hash);
    join(
        shards[shard].run(
            () -> {
              shards[shard].ensureMapOpen(false).put(keyBytes(hash), valueBytes(value));
              return null;
            }));
  }

  @Override
  public void putAll(final Map<Bytes32, Bytes> entries) {
    if (entries.isEmpty()) {
      return;
    }
    ensureWritable();

    final List<Map<Bytes32, Bytes>> byShard = new ArrayList<>();
    for (int i = 0; i < FrozenSnapTrieNodeChronicleConfig.NUM_SHARDS; i++) {
      byShard.add(new HashMap<>());
    }
    for (final Map.Entry<Bytes32, Bytes> entry : entries.entrySet()) {
      byShard.get(FrozenSnapTrieNodeChronicleConfig.shardIndex(entry.getKey()))
          .put(entry.getKey(), entry.getValue());
    }

    final List<CompletableFuture<Void>> writes = new ArrayList<>();
    for (int i = 0; i < byShard.size(); i++) {
      final Map<Bytes32, Bytes> shardBatch = byShard.get(i);
      if (shardBatch.isEmpty()) {
        continue;
      }
      final Shard shard = shards[i];
      writes.add(
          CompletableFuture.runAsync(
              () -> putAllOnShard(shard, shardBatch), shard.executor));
    }
    awaitAll(writes);
  }

  private void putAllOnShard(final Shard shard, final Map<Bytes32, Bytes> shardBatch) {
    final ChronicleMap<byte[], byte[]> openMap = shard.ensureMapOpen(false);
    try {
      for (final Map.Entry<Bytes32, Bytes> entry : shardBatch.entrySet()) {
        openMap.put(keyBytes(entry.getKey()), valueBytes(entry.getValue()));
      }
    } catch (final IllegalStateException e) {
      if (e.getMessage() != null && e.getMessage().contains("extra segment tier")) {
        throw new IllegalStateException(
            e.getMessage()
                + "\nDelete "
                + directory
                + " and re-run snap sync, or raise"
                + " FrozenSnapTrieNodeChronicleConfig.TOTAL_ENTRIES / MAX_BLOAT_FACTOR.",
            e);
      }
      throw e;
    }
  }

  @Override
  public Optional<Bytes> get(final Bytes32 hash) {
    if (closed.get()) {
      throw new IllegalStateException("Frozen snap-sync trie node storage is closed");
    }
    final Shard shard = shards[FrozenSnapTrieNodeChronicleConfig.shardIndex(hash)];
    if (!Files.exists(shard.mapFile)) {
      return Optional.empty();
    }
    return shard
        .run(
            () -> {
              final byte[] value = shard.ensureMapOpen(frozen.get()).get(keyBytes(hash));
              if (value == null) {
                return Optional.<Bytes>empty();
              }
              return Optional.of(Bytes.wrap(Arrays.copyOf(value, value.length)));
            })
        .join();
  }

  @Override
  public void freeze() {
    if (!frozen.compareAndSet(false, true)) {
      return;
    }
    final List<CompletableFuture<Void>> closeTasks = new ArrayList<>();
    for (final Shard shard : shards) {
      closeTasks.add(
          CompletableFuture.runAsync(
              () -> {
                if (shard.map != null) {
                  shard.map.close();
                  shard.map = null;
                }
              },
              shard.executor));
    }
    awaitAll(closeTasks);

    try {
      Files.createFile(directory.resolve(FROZEN_MARKER_FILE));
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to write frozen marker at " + directory, e);
    }

    final List<CompletableFuture<Void>> reopen = new ArrayList<>();
    for (final Shard shard : shards) {
      if (!Files.exists(shard.mapFile)) {
        continue;
      }
      reopen.add(
          CompletableFuture.runAsync(() -> shard.map = shard.openMap(true), shard.executor));
    }
    awaitAll(reopen);

    LOG.info(
        "Frozen snap-sync trie node Chronicle Map at {} (shards={}, longSize={})",
        directory,
        FrozenSnapTrieNodeChronicleConfig.NUM_SHARDS,
        entryCount());
  }

  @Override
  public boolean isFrozen() {
    return frozen.get();
  }

  @Override
  public long entryCount() {
    long total = 0;
    final List<CompletableFuture<Long>> counts = new ArrayList<>();
    for (final Shard shard : shards) {
      if (!Files.exists(shard.mapFile)) {
        continue;
      }
      counts.add(shard.run(() -> shard.ensureMapOpen(frozen.get()).longSize()));
    }
    for (final CompletableFuture<Long> count : counts) {
      total += count.join();
    }
    return total;
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    final List<CompletableFuture<Void>> tasks = new ArrayList<>();
    for (final Shard shard : shards) {
      tasks.add(
          CompletableFuture.runAsync(
              () -> {
                if (shard.map != null) {
                  shard.map.close();
                  shard.map = null;
                }
                shard.executor.shutdown();
              },
              shard.executor));
    }
    awaitAll(tasks);
    for (final Shard shard : shards) {
      try {
        if (!shard.executor.awaitTermination(30, TimeUnit.SECONDS)) {
          shard.executor.shutdownNow();
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        shard.executor.shutdownNow();
      }
    }
  }

  private void ensureWritable() {
    if (frozen.get()) {
      throw new IllegalStateException("Frozen snap-sync trie node storage is read-only");
    }
    if (closed.get()) {
      throw new IllegalStateException("Frozen snap-sync trie node storage is closed");
    }
  }

  private static void awaitAll(final List<CompletableFuture<Void>> futures) {
    join(CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)));
  }

  private static void join(final CompletableFuture<?> future) {
    try {
      future.join();
    } catch (final java.util.concurrent.CompletionException e) {
      if (e.getCause() instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw e;
    }
  }

  private static byte[] keyBytes(final Bytes32 hash) {
    return Arrays.copyOf(hash.toArrayUnsafe(), FrozenSnapTrieNodeChronicleConfig.KEY_SIZE_BYTES);
  }

  private static byte[] valueBytes(final Bytes value) {
    final byte[] raw = value.toArrayUnsafe();
    return Arrays.copyOf(raw, raw.length);
  }

  private static final class Shard {
    private final int index;
    private final Path mapFile;
    private final ExecutorService executor;
    private ChronicleMap<byte[], byte[]> map;

    private Shard(final int index, final Path mapFile) {
      this.index = index;
      this.mapFile = mapFile;
      this.executor =
          Executors.newSingleThreadExecutor(
              r -> {
                final Thread t = new Thread(r, "frozen-snap-trie-shard-" + index);
                t.setDaemon(true);
                return t;
              });
    }

    private ChronicleMap<byte[], byte[]> ensureMapOpen(final boolean readOnly) {
      if (map == null) {
        map = openMap(readOnly);
      }
      return map;
    }

    private ChronicleMap<byte[], byte[]> openMap(final boolean readOnly) {
      final File file = mapFile.toFile();
      try {
        if (readOnly || file.exists()) {
          return FrozenSnapTrieNodeChronicleConfig.newBuilderForShard(index)
              .recoverPersistedTo(file, readOnly);
        }
        final ChronicleMap<byte[], byte[]> created =
            FrozenSnapTrieNodeChronicleConfig.newBuilderForShard(index).createPersistedTo(file);
        LOG.info(
            "Created snap-sync trie shard {} at {} (entriesPerShard={}, sparse={})",
            index,
            file,
            FrozenSnapTrieNodeChronicleConfig.ENTRIES_PER_SHARD,
            FrozenSnapTrieNodeChronicleConfig.USE_SPARSE_FILE);
        return created;
      } catch (final IOException e) {
        throw new IllegalStateException("Failed to open Chronicle Map at " + file, e);
      }
    }

    private <T> CompletableFuture<T> run(final Supplier<T> action) {
      return CompletableFuture.supplyAsync(action, executor);
    }
  }
}
