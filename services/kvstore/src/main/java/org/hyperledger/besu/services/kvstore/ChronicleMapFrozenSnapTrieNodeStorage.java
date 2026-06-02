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
import java.util.concurrent.atomic.AtomicBoolean;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Chronicle Map-backed {@link FrozenSnapTrieNodeStorage} keyed by trie node hash (32 bytes). */
public class ChronicleMapFrozenSnapTrieNodeStorage implements FrozenSnapTrieNodeStorage {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChronicleMapFrozenSnapTrieNodeStorage.class);

  private static final String FROZEN_MARKER_FILE = ".frozen";
  private static final String MAP_FILE_NAME = "snap_trie_nodes.dat";

  private final Path directory;
  private ChronicleMap<byte[], byte[]> map;
  private final AtomicBoolean frozen = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean(false);
  private static final java.util.Set<ChronicleMapFrozenSnapTrieNodeStorage> OPEN_WRITABLE =
      java.util.Collections.synchronizedSet(new java.util.HashSet<>());

  private ChronicleMapFrozenSnapTrieNodeStorage(final Path directory) {
    this.directory = directory;
  }

  public static ChronicleMapFrozenSnapTrieNodeStorage open(final Path directory) {
    try {
      Files.createDirectories(directory);
    } catch (final IOException e) {
      throw new IllegalStateException(
          "Failed to create Chronicle Map directory " + directory, e);
    }
    final boolean alreadyFrozen = Files.exists(directory.resolve(FROZEN_MARKER_FILE));
    final ChronicleMapFrozenSnapTrieNodeStorage storage =
        new ChronicleMapFrozenSnapTrieNodeStorage(directory);
    storage.frozen.set(alreadyFrozen);
    if (alreadyFrozen) {
      storage.map = storage.openMap(true);
    }
    // Writable snap capture: lazy open until first put/flush to avoid mapping an empty store.
    if (!alreadyFrozen) {
      OPEN_WRITABLE.add(storage);
      registerShutdownHookIfNeeded();
    }
    return storage;
  }

  private static void registerShutdownHookIfNeeded() {
    if (SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    for (final ChronicleMapFrozenSnapTrieNodeStorage storage :
                        java.util.List.copyOf(OPEN_WRITABLE)) {
                      if (!storage.closed.get()) {
                        if (storage.map != null) {
                          LOG.info(
                              "Shutdown hook closing frozen Chronicle Map at {} (longSize={})",
                              storage.directory,
                              storage.map.longSize());
                        }
                        storage.close();
                      }
                    }
                  },
                  "frozen-snap-trie-chronicle-close"));
    }
  }

  @Override
  public void put(final Bytes32 hash, final Bytes value) {
    ensureWritable();
    map.put(keyBytes(hash), valueBytes(value));
  }

  @Override
  public void putAll(final Map<Bytes32, Bytes> entries) {
    if (entries.isEmpty()) {
      return;
    }
    ensureWritable();
    for (final Map.Entry<Bytes32, Bytes> entry : entries.entrySet()) {
      map.put(keyBytes(entry.getKey()), valueBytes(entry.getValue()));
    }
  }

  @Override
  public void releaseMappedMemoryAfterFlush() {
    if (frozen.get() || closed.get() || map == null) {
      return;
    }
    map.close();
    map = null;
  }

  @Override
  public Optional<Bytes> get(final Bytes32 hash) {
    ensureMapOpen();
    final byte[] value = map.get(keyBytes(hash));
    if (value == null) {
      return Optional.empty();
    }
    return Optional.of(Bytes.wrap(Arrays.copyOf(value, value.length)));
  }

  @Override
  public void freeze() {
    if (!frozen.compareAndSet(false, true)) {
      return;
    }
    OPEN_WRITABLE.remove(this);
    ensureMapOpen();
    map.close();
    map = null;
    try {
      Files.createFile(directory.resolve(FROZEN_MARKER_FILE));
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to write frozen marker at " + directory, e);
    }
    map = openMap(true);
    LOG.info("Frozen snap-sync trie node Chronicle Map at {}", directory);
  }

  @Override
  public boolean isFrozen() {
    return frozen.get();
  }

  @Override
  public long entryCount() {
    ensureMapOpen();
    return map.longSize();
  }

  @Override
  public void close() {
    OPEN_WRITABLE.remove(this);
    if (closed.compareAndSet(false, true) && map != null) {
      map.close();
      map = null;
    }
  }

  private ChronicleMap<byte[], byte[]> openMap(final boolean readOnly) {
    final File mapFile = directory.resolve(MAP_FILE_NAME).toFile();
    final ChronicleMapBuilder<byte[], byte[]> builder = FrozenSnapTrieNodeChronicleConfig.newBuilder();
    try {
      if (readOnly || mapFile.exists()) {
        return builder.recoverPersistedTo(mapFile, readOnly);
      }
      return builder.createPersistedTo(mapFile);
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to open Chronicle Map at " + mapFile, e);
    }
  }

  private void ensureWritable() {
    if (frozen.get()) {
      throw new IllegalStateException("Frozen snap-sync trie node storage is read-only");
    }
    ensureMapOpen();
  }

  private void ensureMapOpen() {
    if (closed.get()) {
      throw new IllegalStateException("Frozen snap-sync trie node storage is closed");
    }
    if (map == null) {
      map = openMap(frozen.get());
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
