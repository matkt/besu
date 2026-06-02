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
 * Sharded Chronicle Map storage: {@value FrozenSnapTrieNodeChronicleConfig#NUM_SHARDS} files, each
 * written by its own thread so snap-sync can flush trie nodes in parallel without file-lock races.
 */
public class ChronicleMapFrozenSnapTrieNodeStorage implements FrozenSnapTrieNodeStorage {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChronicleMapFrozenSnapTrieNodeStorage.class);

  private static final String FROZEN_MARKER_FILE = ".frozen";
  private static final String LEGACY_MAP_FILE = "snap_trie_nodes.dat";

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
    if (Files.exists(directory.resolve(LEGACY_MAP_FILE))) {
      LOG.warn(
          "Legacy single-file frozen trie map {} detected; delete {} and re-run snap sync to use"
              + " sharded storage",
          directory.resolve(LEGACY_MAP_FILE),
          directory);
    }
    final boolean alreadyFrozen = Files.exists(directory.resolve(FROZEN_MARKER_FILE));
    return new ChronicleMapFrozenSnapTrieNodeStorage(directory, alreadyFrozen);
  }

  @Override
  public void put(final Bytes32 hash, final Bytes value) {
    ensureWritable();
    final int shard = FrozenSnapTrieNodeChronicleConfig.shardIndex(hash);
    shards[shard]
        .run(() -> shards[shard].ensureMapOpen(false).put(keyBytes(hash), valueBytes(value)))
        .join();
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
              () -> {
                final ChronicleMap<byte[], byte[]> map = shard.ensureMapOpen(false);
                for (final Map.Entry<Bytes32, Bytes> entry : shardBatch.entrySet()) {
                  map.put(keyBytes(entry.getKey()), valueBytes(entry.getValue()));
                }
              },
              shard.executor));
    }
    awaitAll(writes);
  }

  @Override
  public Optional<Bytes> get(final Bytes32 hash) {
    if (closed.get()) {
      throw new IllegalStateException("Frozen snap-sync trie node storage is closed");
    }
    final int shardIndex = FrozenSnapTrieNodeChronicleConfig.shardIndex(hash);
    final Shard shard = shards[shardIndex];
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
    final List<CompletableFuture<Void>> tasks = new ArrayList<>();
    for (final Shard shard : shards) {
      tasks.add(
          CompletableFuture.runAsync(
              () -> {
                if (shard.map != null) {
                  shard.map.close();
                  shard.map = null;
                }
              },
              shard.executor));
    }
    awaitAll(tasks);

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
          CompletableFuture.runAsync(
              () -> shard.map = shard.openMap(true), shard.executor));
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
      counts.add(
          shard.run(() -> shard.ensureMapOpen(frozen.get()).longSize()));
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
    CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
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
        if (!readOnly && map.longSize() == 0) {
          LOG.debug("Opened writable Chronicle shard {} at {}", index, mapFile);
        }
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
            "Created snap-sync trie shard {} at {} (sparse={}, entriesPerShard={})",
            index,
            file,
            FrozenSnapTrieNodeChronicleConfig.USE_SPARSE_FILE,
            FrozenSnapTrieNodeChronicleConfig.ENTRIES_PER_SHARD);
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
