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
 *
 */
package org.hyperledger.besu.ethereum.bonsai.storage.flat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.rlp.RLP;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageAdapter;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;

/**
 * This class represents a FlatDbReaderStrategy, which is responsible for reading data from flat
 * databases. It implements various methods for retrieving account data, code data, and storage data
 * from the corresponding KeyValueStorage.
 */
public abstract class FlatDbReaderStrategy {

  protected final MetricsSystem metricsSystem;
  protected final Counter getAccountCounter;
  protected final Counter getAccountFoundInFlatDatabaseCounter;

  protected final Counter getStorageValueCounter;
  protected final Counter getStorageValueFlatDatabaseCounter;

  public FlatDbReaderStrategy(final MetricsSystem metricsSystem) {
    this.metricsSystem = metricsSystem;

    getAccountCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_account_total",
            "Total number of calls to getAccount");

    getAccountFoundInFlatDatabaseCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_account_flat_database",
            "Number of accounts found in the flat database");

    getStorageValueCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_storagevalue_total",
            "Total number of calls to getStorageValueBySlotHash");

    getStorageValueFlatDatabaseCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_storagevalue_flat_database",
            "Number of storage slots found in the flat database");
  }

  /*
   * Retrieves the account data for the given account hash, using the world state root hash supplier and node loader.
   */
  public abstract Optional<Bytes> getAccount(
      Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      NodeLoader nodeLoader,
      Hash accountHash,
      KeyValueStorage accountStorage);

  /*
   * Retrieves the storage value for the given account hash and storage slot key, using the world state root hash supplier, storage root supplier, and node loader.
   */

  public abstract Optional<Bytes> getStorageValueByStorageSlotKey(
      Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      Supplier<Optional<Hash>> storageRootSupplier,
      NodeLoader nodeLoader,
      Hash accountHash,
      StorageSlotKey storageSlotKey,
      KeyValueStorage storageStorage);

  /*
   * Retrieves the code data for the given code hash and account hash.
   */
  public Optional<Bytes> getCode(
      final Bytes32 codeHash, final Hash accountHash, final KeyValueStorageAdapter worldStateStorage) {
    if (codeHash.equals(Hash.EMPTY)) {
      return Optional.of(Bytes.EMPTY);
    } else {
      return worldStateStorage
          .get(CODE_STORAGE, accountHash.toArrayUnsafe())
          .map(Bytes::wrap)
          .filter(b -> Hash.hash(b).equals(codeHash));
    }
  }

  public void clearAll(
      final KeyValueStorageAdapter worldStateStorage) {
    worldStateStorage.clear(List.of(ACCOUNT_INFO_STATE, ACCOUNT_STORAGE_STORAGE, CODE_STORAGE));
  }

  public void resetOnResync(final KeyValueStorageAdapter worldStateStorage) {
    worldStateStorage.clear(List.of(ACCOUNT_INFO_STATE, ACCOUNT_STORAGE_STORAGE));
  }

  public Map<Bytes32, Bytes> streamAccountFlatDatabase(
      final KeyValueStorage accountStorage,
      final Bytes startKeyHash,
      final Bytes32 endKeyHash,
      final long max) {
    final Stream<Pair<Bytes32, Bytes>> pairStream =
        accountStorage
            .streamFromKey(startKeyHash.toArrayUnsafe())
            .limit(max)
            .map(pair -> new Pair<>(Bytes32.wrap(pair.getKey()), Bytes.wrap(pair.getValue())))
            .takeWhile(pair -> pair.getFirst().compareTo(endKeyHash) <= 0);

    final TreeMap<Bytes32, Bytes> collected =
        pairStream.collect(
            Collectors.toMap(Pair::getFirst, Pair::getSecond, (v1, v2) -> v1, TreeMap::new));
    pairStream.close();
    return collected;
  }

  public Map<Bytes32, Bytes> streamStorageFlatDatabase(
      final KeyValueStorage storageStorage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Bytes32 endKeyHash,
      final long max) {
    final Stream<Pair<Bytes32, Bytes>> pairStream =
        storageStorage
            .streamFromKey(Bytes.concatenate(accountHash, startKeyHash).toArrayUnsafe())
            .takeWhile(pair -> Bytes.wrap(pair.getKey()).slice(0, Hash.SIZE).equals(accountHash))
            .limit(max)
            .map(
                pair ->
                    new Pair<>(
                        Bytes32.wrap(Bytes.wrap(pair.getKey()).slice(Hash.SIZE)),
                        RLP.encodeValue(Bytes.wrap(pair.getValue()).trimLeadingZeros())))
            .takeWhile(pair -> pair.getFirst().compareTo(endKeyHash) <= 0);

    final TreeMap<Bytes32, Bytes> collected =
        pairStream.collect(
            Collectors.toMap(Pair::getFirst, Pair::getSecond, (v1, v2) -> v1, TreeMap::new));
    pairStream.close();
    return collected;
  }
}
