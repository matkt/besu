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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredNodeFactory;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration.PathBasedUnstable;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.rlp.RLP;

/**
 * Partial flat DB with account rows (32-byte keys) and flat storage rows (64-byte keys) in {@code
 * ACCOUNT_INFO_STATE} only. Requires a new datadir with the merged column-family layout (see
 * {@link PathBasedUnstable#getMergedFlatWorldStateColumnFamilyEnabled()}).
 */
public class BonsaiPartialMergedFlatDbStrategy extends BonsaiPartialFlatDbStrategy {

  public BonsaiPartialMergedFlatDbStrategy(
      final MetricsSystem metricsSystem, final CodeStorageStrategy codeStorageStrategy) {
    super(metricsSystem, codeStorageStrategy);
  }

  @Override
  public Optional<Bytes> getFlatStorageValueByStorageSlotKey(
      final Supplier<Optional<Bytes>> worldStateRootHashSupplier,
      final Supplier<Optional<Hash>> storageRootSupplier,
      final NodeLoader nodeLoader,
      final Hash accountHash,
      final StorageSlotKey storageSlotKey,
      final SegmentedKeyValueStorage storage) {
    getStorageValueCounter.inc();
    Optional<Bytes> response =
        storage
            .get(
                ACCOUNT_INFO_STATE,
                Bytes.concatenate(accountHash.getBytes(), storageSlotKey.getSlotHash().getBytes())
                    .toArrayUnsafe())
            .map(Bytes::wrap);
    if (response.isEmpty()) {
      final Optional<Hash> storageRoot = storageRootSupplier.get();
      final Optional<Bytes> worldStateRootHash = worldStateRootHashSupplier.get();
      if (storageRoot.isPresent() && worldStateRootHash.isPresent()) {
        response =
            new StoredMerklePatriciaTrie<>(
                    new StoredNodeFactory<>(nodeLoader, Function.identity(), Function.identity()),
                    Bytes32.wrap(storageRoot.get().getBytes()))
                .get(storageSlotKey.getSlotHash().getBytes())
                .map(bytes -> Bytes32.leftPad(RLP.decodeValue(bytes)));
        if (response.isEmpty()) getStorageValueMissingMerkleTrieCounter.inc();
        else getStorageValueMerkleTrieCounter.inc();
      }
    } else {
      getStorageValueFlatDatabaseCounter.inc();
    }
    return response;
  }

  @Override
  public void putFlatAccountStorageValueByStorageSlotHash(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash,
      final Bytes storageValue) {
    final byte[] key =
        Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()).toArrayUnsafe();
    transaction.put(ACCOUNT_INFO_STATE, key, storageValue.toArrayUnsafe());
  }

  @Override
  public void removeFlatAccountStorageValueByStorageSlotHash(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash) {
    transaction.remove(
        ACCOUNT_INFO_STATE,
        Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()).toArrayUnsafe());
  }

  @Override
  public void clearAll(final SegmentedKeyValueStorage storage) {
    storage.clear(ACCOUNT_INFO_STATE);
    storage.clear(CODE_STORAGE);
  }

  @Override
  public void resetOnResync(final SegmentedKeyValueStorage storage) {
    storage.clear(ACCOUNT_INFO_STATE);
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> storageToPairStream(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Function<Bytes, Bytes> valueMapper) {
    return storage
        .streamFromKey(
            ACCOUNT_INFO_STATE,
            Bytes.concatenate(accountHash.getBytes(), startKeyHash).toArrayUnsafe())
        .takeWhile(
            pair -> Bytes.wrap(pair.getKey()).slice(0, Bytes32.SIZE).equals(accountHash.getBytes()))
        .filter(pair -> pair.getKey().length > Bytes32.SIZE)
        .map(
            pair ->
                new Pair<>(
                    Bytes32.wrap(Bytes.wrap(pair.getKey()).slice(Bytes32.SIZE)),
                    valueMapper.apply(Bytes.wrap(pair.getValue()).trimLeadingZeros())));
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> storageToPairStream(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Bytes32 endKeyHash,
      final Function<Bytes, Bytes> valueMapper) {
    return storage
        .streamFromKey(
            ACCOUNT_INFO_STATE,
            Bytes.concatenate(accountHash.getBytes(), startKeyHash).toArrayUnsafe(),
            Bytes.concatenate(accountHash.getBytes(), endKeyHash).toArrayUnsafe())
        .filter(pair -> pair.getKey().length > Bytes32.SIZE)
        .map(
            pair ->
                new Pair<>(
                    Bytes32.wrap(Bytes.wrap(pair.getKey()).slice(Bytes32.SIZE)),
                    valueMapper.apply(Bytes.wrap(pair.getValue()).trimLeadingZeros())));
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> accountsToPairStream(
      final SegmentedKeyValueStorage storage, final Bytes startKeyHash, final Bytes32 endKeyHash) {
    return storage
        .streamFromKey(ACCOUNT_INFO_STATE, startKeyHash.toArrayUnsafe(), endKeyHash.toArrayUnsafe())
        .filter(pair -> pair.getKey().length == Bytes32.SIZE)
        .map(pair -> new Pair<>(Bytes32.wrap(pair.getKey()), Bytes.wrap(pair.getValue())));
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> accountsToPairStream(
      final SegmentedKeyValueStorage storage, final Bytes startKeyHash) {
    return storage
        .streamFromKey(ACCOUNT_INFO_STATE, startKeyHash.toArrayUnsafe())
        .filter(pair -> pair.getKey().length == Bytes32.SIZE)
        .map(pair -> new Pair<>(Bytes32.wrap(pair.getKey()), Bytes.wrap(pair.getValue())));
  }
}
