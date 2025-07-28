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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.VERKLE_TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.stateless.bintrie.BytesBitSequence;
import org.hyperledger.besu.ethereum.stateless.bintrie.BytesBitSequenceFactory;
import org.hyperledger.besu.ethereum.stateless.bintrie.adapter.TrieKeyFactory;
import org.hyperledger.besu.ethereum.stateless.bintrie.factory.StoredNodeFactory;
import org.hyperledger.besu.ethereum.stateless.bintrie.hasher.StemHasher;
import org.hyperledger.besu.ethereum.stateless.bintrie.node.LeafNode;
import org.hyperledger.besu.ethereum.stateless.verkle.adapter.TrieKeyUtils;
import org.hyperledger.besu.ethereum.stateless.verkle.util.SuffixTreeDecoder;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.BinTrieAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeStorageStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.FlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Strategy for managing BinTrie accounts in the flat database using a stem-based structure.
 *
 * <p>Unlike the legacy approach of saving by account, slot, and code, this strategy saves by stem
 * and code. This allows direct access to the leaves of the Binary trie for stems, reducing
 * duplication of data.
 */
public class BinTrieStemFlatDbStrategy extends FlatDbStrategy {

  protected final Counter getAccountNotFoundInFlatDatabaseCounter;

  protected final Counter getStorageValueNotFoundInFlatDatabaseCounter;

  private static final TrieKeyFactory TRIE_KEY_FACTORY = new TrieKeyFactory(new StemHasher());
  private static final BytesBitSequenceFactory BIT_SEQ_FACTORY = new BytesBitSequenceFactory();
  private static final StoredNodeFactory<BytesBitSequence, Bytes> NODE_FACTORY =
      new StoredNodeFactory<>(
          (location, hash) -> Optional.empty(), BIT_SEQ_FACTORY, Function.identity());

  public BinTrieStemFlatDbStrategy(
      final MetricsSystem metricsSystem, final CodeStorageStrategy codeStorageStrategy) {
    super(metricsSystem, codeStorageStrategy);
    getAccountNotFoundInFlatDatabaseCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_account_missing_flat_database",
            "Number of accounts not found in the flat database");

    getStorageValueNotFoundInFlatDatabaseCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.BLOCKCHAIN,
            "get_storagevalue_missing_flat_database",
            "Number of storage slots not found in the flat database");
  }

  private Optional<List<LeafNode<BytesBitSequence, Bytes>>> fetchLeafNodes(
      final BytesBitSequence stemId, final SegmentedKeyValueStorage storage) {

    return getStem(stemId.encode(), storage)
        .map(bytes -> NODE_FACTORY.decodeStemNode(stemId, bytes).children);
  }

  public Optional<BinTrieAccount> getFlatAccount(
      final Address address,
      final PathBasedWorldView context,
      final SegmentedKeyValueStorage storage) {

    getAccountCounter.inc();

    Optional<BinTrieAccount> account =
        fetchLeafNodes(TRIE_KEY_FACTORY.getHeaderStem(address), storage)
            .flatMap(
                children -> {
                  LeafNode<BytesBitSequence, Bytes> dataLeaf = children.get(0);
                  LeafNode<BytesBitSequence, Bytes> codeLeaf = children.get(1);
                  return dataLeaf
                      .value
                      .map(Bytes32::wrap)
                      .map(
                          value ->
                              new BinTrieAccount(
                                  context,
                                  address,
                                  address.addressHash(),
                                  SuffixTreeDecoder.decodeNonce(value),
                                  Wei.of(SuffixTreeDecoder.decodeBalance(value)),
                                  SuffixTreeDecoder.decodeCodeSize(value),
                                  Hash.wrap(
                                      codeLeaf
                                          .value
                                          .map(Bytes32::wrap)
                                          .orElse(Hash.EMPTY_TRIE_HASH)),
                                  true));
                });

    if (account.isPresent()) {
      getAccountFoundInFlatDatabaseCounter.inc();
    } else {
      getAccountNotFoundInFlatDatabaseCounter.inc();
    }
    return account;
  }

  public Optional<Bytes> getFlatStorageValueByStorageSlotKey(
      final Address address,
      final StorageSlotKey storageSlotKey,
      final SegmentedKeyValueStorage storage) {

    getStorageValueCounter.inc();

    BytesBitSequence stemId =
        TRIE_KEY_FACTORY.getStorageStem(address, storageSlotKey.getSlotKey().orElseThrow());

    Optional<Bytes> value =
        fetchLeafNodes(stemId, storage)
            .flatMap(
                children -> {
                  int idx =
                      TrieKeyUtils.getStorageKeySuffix(storageSlotKey.getSlotKey().orElseThrow())
                          .toInt();
                  return children.get(idx).value.map(Bytes32::wrap);
                });

    if (value.isPresent()) {
      getStorageValueFlatDatabaseCounter.inc();
    } else {
      getStorageValueNotFoundInFlatDatabaseCounter.inc();
    }
    return value;
  }

  private Optional<Bytes> getStem(final byte[] stem, final SegmentedKeyValueStorage storage) {
    return storage.get(VERKLE_TRIE_BRANCH_STORAGE, stem).map(Bytes::wrap);
  }

  @Override
  public void putFlatAccount(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes accountValue) {
    // nothing to do with stem flat db
  }

  @Override
  public void removeFlatAccount(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash) {
    // nothing to do with stem flat db
  }

  @Override
  public void putFlatAccountStorageValueByStorageSlotHash(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash,
      final Bytes storageValue) {
    // nothing to do with stem flat db
  }

  @Override
  public void removeFlatAccountStorageValueByStorageSlotHash(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Hash slotHash) {
    // nothing to do with stem flat db
  }

  @Override
  public void clearAll(final SegmentedKeyValueStorage storage) {
    // NOOP
    // we cannot clear flatdb in binary tries as we are using directly the trie
  }

  @Override
  public void resetOnResync(final SegmentedKeyValueStorage storage) {
    // NOOP
    // not need to reset anything in full mode
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> storageToPairStream(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Function<Bytes, Bytes> valueMapper) {
    return Stream.empty();
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> storageToPairStream(
      final SegmentedKeyValueStorage storage,
      final Hash accountHash,
      final Bytes startKeyHash,
      final Bytes32 endKeyHash,
      final Function<Bytes, Bytes> valueMapper) {
    return Stream.empty();
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> accountsToPairStream(
      final SegmentedKeyValueStorage storage, final Bytes startKeyHash, final Bytes32 endKeyHash) {
    return Stream.empty();
  }

  @Override
  protected Stream<Pair<Bytes32, Bytes>> accountsToPairStream(
      final SegmentedKeyValueStorage storage, final Bytes startKeyHash) {
    return Stream.empty();
  }
}
