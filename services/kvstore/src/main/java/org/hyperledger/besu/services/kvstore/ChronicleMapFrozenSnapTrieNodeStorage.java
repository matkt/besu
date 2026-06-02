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
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import net.openhft.chronicle.map.ChronicleMap;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-file Chronicle Map for snap-sync trie nodes. All map access runs on one writer thread so
 * parallel snap-sync pipeline threads can call {@link #putAll} without corrupting the map.
 */
public class ChronicleMapFrozenSnapTrieNodeStorage implements FrozenSnapTrieNodeStorage {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChronicleMapFrozenSnapTrieNodeStorage.class);

  private static final String FROZEN_MARKER_FILE = ".frozen";
  private static final String SHARD_FILE_GLOB_PREFIX = "snap_trie_nodes_";

  private final Path directory;
  private final Path mapFile;
  private final ExecutorService mapExecutor;
  private final AtomicBoolean frozen = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicLong flushPutCount = new AtomicLong();

  private volatile ChronicleMap<byte[], byte[]> map;

  private ChronicleMapFrozenSnapTrieNodeStorage(final Path directory, final boolean readOnly) {
    this.directory = directory;
    this.mapFile = directory.resolve(FrozenSnapTrieNodeChronicleConfig.MAP_FILE_NAME);
    this.mapExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              final Thread t = new Thread(r, "frozen-snap-trie-map");
              t.setDaemon(true);
              return t;
            });
    if (readOnly) {
      frozen.set(true);
      if (Files.exists(mapFile)) {
        map = runOnMapThread(() -> openMap(true));
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
    warnIfShardedMapsPresent(directory);
    final boolean alreadyFrozen = Files.exists(directory.resolve(FROZEN_MARKER_FILE));
    return new ChronicleMapFrozenSnapTrieNodeStorage(directory, alreadyFrozen);
  }

  private static void warnIfShardedMapsPresent(final Path directory) {
    try (var stream = Files.list(directory)) {
      final boolean hasShards =
          stream
              .map(path -> path.getFileName().toString())
              .anyMatch(
                  name ->
                      name.startsWith(SHARD_FILE_GLOB_PREFIX)
                          && name.endsWith(".dat")
                          && !name.equals(FrozenSnapTrieNodeChronicleConfig.MAP_FILE_NAME));
      if (hasShards) {
        LOG.warn(
            "Sharded frozen trie map files detected under {}; delete the directory and re-run snap"
                + " sync for single-file mode",
            directory);
      }
    } catch (final IOException e) {
      LOG.debug("Could not list {}", directory, e);
    }
  }

  @Override
  public void put(final Bytes32 hash, final Bytes value) {
    runOnMapThread(
        () -> {
          ensureWritable();
          ensureMapOpen(false).put(keyBytes(hash), valueBytes(value));
          flushPutCount.incrementAndGet();
          return null;
        });
  }

  @Override
  public void putAll(final Map<Bytes32, Bytes> entries) {
    if (entries.isEmpty()) {
      return;
    }
    runOnMapThread(
        () -> {
          ensureWritable();
          final ChronicleMap<byte[], byte[]> openMap = ensureMapOpen(false);
          try {
            for (final Map.Entry<Bytes32, Bytes> entry : entries.entrySet()) {
              openMap.put(keyBytes(entry.getKey()), valueBytes(entry.getValue()));
            }
          } catch (final IllegalStateException e) {
            if (e.getMessage() != null && e.getMessage().contains("extra segment tier")) {
              throw new IllegalStateException(
                  e.getMessage()
                      + "\nDelete "
                      + directory
                      + " and re-run snap sync after upgrading, or raise"
                      + " FrozenSnapTrieNodeChronicleConfig.INITIAL_ENTRIES / MAX_BLOAT_FACTOR.",
                  e);
            }
            throw e;
          }
          flushPutCount.addAndGet(entries.size());
          return null;
        });
  }

  @Override
  public Optional<Bytes> get(final Bytes32 hash) {
    if (closed.get()) {
      throw new IllegalStateException("Frozen snap-sync trie node storage is closed");
    }
    return runOnMapThread(
        () -> {
          if (!Files.exists(mapFile)) {
            return Optional.empty();
          }
          final byte[] value = ensureMapOpen(frozen.get()).get(keyBytes(hash));
          if (value == null) {
            return Optional.empty();
          }
          return Optional.of(Bytes.wrap(Arrays.copyOf(value, value.length)));
        });
  }

  @Override
  public void freeze() {
    runOnMapThread(
        () -> {
          if (!frozen.compareAndSet(false, true)) {
            return null;
          }
          if (map != null) {
            map.close();
            map = null;
          }
          try {
            Files.createFile(directory.resolve(FROZEN_MARKER_FILE));
          } catch (final IOException e) {
            throw new IllegalStateException("Failed to write frozen marker at " + directory, e);
          }
          if (Files.exists(mapFile)) {
            map = openMap(true);
          }
          LOG.info(
              "Frozen snap-sync trie node Chronicle Map at {} (longSize={})",
              directory,
              entryCountOnMapThread());
          return null;
        });
  }

  @Override
  public boolean isFrozen() {
    return frozen.get();
  }

  @Override
  public long entryCount() {
    if (!Files.exists(mapFile)) {
      return 0;
    }
    return runOnMapThread(this::entryCountOnMapThread);
  }

  /** Cumulative puts from flush batches (includes overwrites); cheap for logging. */
  public long flushPutCount() {
    return flushPutCount.get();
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    runOnMapThread(
        () -> {
          if (map != null) {
            map.close();
            map = null;
          }
          return null;
        });
    mapExecutor.shutdown();
    try {
      if (!mapExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        mapExecutor.shutdownNow();
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      mapExecutor.shutdownNow();
    }
  }

  private long entryCountOnMapThread() {
    return map == null ? 0 : map.longSize();
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
        return FrozenSnapTrieNodeChronicleConfig.newBuilder().recoverPersistedTo(file, readOnly);
      }
      final ChronicleMap<byte[], byte[]> created =
          FrozenSnapTrieNodeChronicleConfig.newBuilder().createPersistedTo(file);
      LOG.info(
          "Created snap-sync trie Chronicle Map at {} (sparse={}, initialEntries={},"
              + " maxBloatFactor={}, approxMaxEntries={})",
          file,
          FrozenSnapTrieNodeChronicleConfig.USE_SPARSE_FILE,
          FrozenSnapTrieNodeChronicleConfig.INITIAL_ENTRIES,
          FrozenSnapTrieNodeChronicleConfig.MAX_BLOAT_FACTOR,
          (long)
              (FrozenSnapTrieNodeChronicleConfig.INITIAL_ENTRIES
                  * FrozenSnapTrieNodeChronicleConfig.MAX_BLOAT_FACTOR));
      return created;
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to open Chronicle Map at " + file, e);
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

  private <T> T runOnMapThread(final Supplier<T> action) {
    try {
      return mapExecutor.submit(action::get).get();
    } catch (final Exception e) {
      if (e.getCause() instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw new IllegalStateException("Chronicle Map operation failed", e);
    }
  }

  private static byte[] keyBytes(final Bytes32 hash) {
    return Arrays.copyOf(hash.toArrayUnsafe(), FrozenSnapTrieNodeChronicleConfig.KEY_SIZE_BYTES);
  }

  private static byte[] valueBytes(final Bytes value) {
    final byte[] raw = value.toArrayUnsafe();
    return Arrays.copyOf(raw, raw.length);
  }
}
