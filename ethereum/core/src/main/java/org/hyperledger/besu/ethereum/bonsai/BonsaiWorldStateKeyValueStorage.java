/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.bonsai;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hyperledger.besu.ethereum.util.RangeManager.generateRangeFromLocation;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.LeafNode;
import org.hyperledger.besu.ethereum.trie.MerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.StoredNodeFactory;
import org.hyperledger.besu.ethereum.trie.TrieNodeDecoder;
import org.hyperledger.besu.ethereum.worldstate.PeerTrieNodeFinder;
import org.hyperledger.besu.ethereum.worldstate.StateTrieAccountValue;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.rlp.RLP;

@SuppressWarnings({"unused", "MismatchedQueryAndUpdateOfCollection", "ModifiedButNotUsed"})
public class BonsaiWorldStateKeyValueStorage implements WorldStateStorage, AutoCloseable {
  public static final byte[] WORLD_ROOT_HASH_KEY = "worldRoot".getBytes(StandardCharsets.UTF_8);

  public static final byte[] WORLD_BLOCK_HASH_KEY =
      "worldBlockHash".getBytes(StandardCharsets.UTF_8);

  protected final KeyValueStorage accountStorage;
  protected final KeyValueStorage codeStorage;
  protected final KeyValueStorage storageStorage;
  protected final KeyValueStorage trieBranchStorage;
  protected final KeyValueStorage trieLogStorage;

  private Optional<PeerTrieNodeFinder> maybeFallbackNodeFinder;

  public BonsaiWorldStateKeyValueStorage(final StorageProvider provider) {
    this(
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE),
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.CODE_STORAGE),
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE),
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE),
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE),
        Optional.empty());
  }

  public BonsaiWorldStateKeyValueStorage(
      final KeyValueStorage accountStorage,
      final KeyValueStorage codeStorage,
      final KeyValueStorage storageStorage,
      final KeyValueStorage trieBranchStorage,
      final KeyValueStorage trieLogStorage) {
    this(
        accountStorage,
        codeStorage,
        storageStorage,
        trieBranchStorage,
        trieLogStorage,
        Optional.empty());
  }

  public BonsaiWorldStateKeyValueStorage(
      final KeyValueStorage accountStorage,
      final KeyValueStorage codeStorage,
      final KeyValueStorage storageStorage,
      final KeyValueStorage trieBranchStorage,
      final KeyValueStorage trieLogStorage,
      final Optional<PeerTrieNodeFinder> fallbackNodeFinder) {
    this.accountStorage = accountStorage;
    this.codeStorage = codeStorage;
    this.storageStorage = storageStorage;
    this.trieBranchStorage = trieBranchStorage;
    this.trieLogStorage = trieLogStorage;
    this.maybeFallbackNodeFinder = fallbackNodeFinder;
  }

  @Override
  public Optional<Bytes> getCode(final Bytes32 codeHash, final Hash accountHash) {
    return codeStorage.get(accountHash.toArrayUnsafe()).map(Bytes::wrap);
  }

  public Optional<Bytes> getAccount(final Hash accountHash) {
    final Bytes accountPath = CompactEncoding.bytesToPath(accountHash);
    Optional<Pair<Bytes, Bytes>> nearestKey = trieBranchStorage.getNearestKey(accountPath);
    Optional<Bytes> byte1 =
        nearestKey
            .map(pair -> TrieNodeDecoder.decode(pair.getKey(), pair.getValue()))
            .filter(
                leaf ->
                    leaf instanceof LeafNode
                        && Bytes.concatenate(leaf.getLocation().orElse(Bytes.EMPTY), leaf.getPath())
                            .equals(accountPath))
            .flatMap(Node::getValue);
    Optional<Bytes> byte2 = Optional.empty();
    final Optional<Bytes> worldStateRootHash = getWorldStateRootHash();
    ArrayList<String> dd = new ArrayList<>();
    if (worldStateRootHash.isPresent()) {
      byte2 =
          new StoredMerklePatriciaTrie<>(
                  new StoredNodeFactory<>(
                      (location, hash) -> {
                        Optional<Bytes> accountStateTrieNode =
                            getAccountStateTrieNode(location, hash);
                        dd.add(
                            "trie -> "
                                + accountHash
                                + " "
                                + location
                                + " "
                                + accountStateTrieNode
                                + " "
                                + hash);
                        return accountStateTrieNode;
                      },
                      Function.identity(),
                      Function.identity()),
                  Bytes32.wrap(worldStateRootHash.get()))
              .get(accountHash);

      if (!byte2.equals(byte1)) {
        System.out.println(
            "account "
                + accountHash
                + " "
                + " "
                + byte1
                + " "
                + byte2
                + " "
                + nearestKey.map(org.apache.commons.lang3.tuple.Pair::getKey));
        dd.forEach(System.out::println);
      }
    }
    return byte2;
  }

  @Override
  public Optional<Bytes> getAccountTrieNodeData(final Bytes location, final Bytes32 hash) {
    // for Bonsai trie fast sync this method should return an empty
    return Optional.empty();
  }

  @Override
  public Optional<Bytes> getAccountStateTrieNode(final Bytes location, final Bytes32 nodeHash) {
    if (nodeHash.equals(MerklePatriciaTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerklePatriciaTrie.EMPTY_TRIE_NODE);
    } else {
      final Optional<Bytes> value =
          trieBranchStorage.get(location.toArrayUnsafe()).map(Bytes::wrap);
      if (value.isPresent()) {
        return value
            .filter(b -> Hash.hash(b).equals(nodeHash))
            .or(
                () ->
                    maybeFallbackNodeFinder.flatMap(
                        finder -> finder.getAccountStateTrieNode(location, nodeHash)));
      }
      return Optional.empty();
    }
  }

  @Override
  public Optional<Bytes> getAccountStorageTrieNode(
      final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
    if (nodeHash.equals(MerklePatriciaTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerklePatriciaTrie.EMPTY_TRIE_NODE);
    } else {
      final Optional<Bytes> value =
          trieBranchStorage
              .get(Bytes.concatenate(accountHash, location).toArrayUnsafe())
              .map(Bytes::wrap);
      if (value.isPresent()) {
        return value
            .filter(b -> Hash.hash(b).equals(nodeHash))
            .or(
                () ->
                    maybeFallbackNodeFinder.flatMap(
                        finder ->
                            finder.getAccountStorageTrieNode(accountHash, location, nodeHash)));
      }
      return Optional.empty();
    }
  }

  public Optional<byte[]> getTrieLog(final Hash blockHash) {
    return trieLogStorage.get(blockHash.toArrayUnsafe());
  }

  public Optional<Bytes> getStateTrieNode(final Bytes location) {
    return trieBranchStorage.get(location.toArrayUnsafe()).map(Bytes::wrap);
  }

  public KeyValueStorage getTrieBranchStorage() {
    return trieBranchStorage;
  }

  public Optional<Bytes> getWorldStateRootHash() {
    return trieBranchStorage.get(WORLD_ROOT_HASH_KEY).map(Bytes::wrap);
  }

  public Optional<Bytes> getWorldStateBlockHash() {
    return trieBranchStorage.get(WORLD_BLOCK_HASH_KEY).map(Bytes::wrap);
  }

  public Optional<Bytes> getStorageValueBySlotHash(final Hash accountHash, final Hash slotHash) {
    final Bytes slotPath = Bytes.concatenate(accountHash, CompactEncoding.bytesToPath(slotHash));
    Optional<Pair<Bytes, Bytes>> moreClosedByPrefix = trieBranchStorage.getNearestKey(slotPath);

    Optional<Bytes> bytes1 =
        moreClosedByPrefix
            .map(
                pair ->
                    TrieNodeDecoder.decode(Bytes.wrap(pair.getKey()), Bytes.wrap(pair.getValue())))
            .filter(
                leaf ->
                    leaf instanceof LeafNode
                        && Bytes.concatenate(leaf.getLocation().orElse(Bytes.EMPTY), leaf.getPath())
                            .equals(slotPath))
            .flatMap(Node::getValue)
            .map(value -> Bytes32.leftPad(RLP.decodeValue(value)));

    Optional<Bytes> bytes2 = Optional.empty();

    Optional<Bytes> account = Optional.empty();
    final Optional<Bytes> worldStateRootHash = getWorldStateRootHash();
    if (worldStateRootHash.isPresent()) {
      account =
          new StoredMerklePatriciaTrie<>(
                  new StoredNodeFactory<>(
                      this::getAccountStateTrieNode, Function.identity(), Function.identity()),
                  Bytes32.wrap(worldStateRootHash.get()))
              .get(accountHash);
    }
    ArrayList<String> rr = new ArrayList<>();
    if (account.isPresent()) {
      final StateTrieAccountValue accountValue =
          StateTrieAccountValue.readFrom(
              org.hyperledger.besu.ethereum.rlp.RLP.input(account.get()));
      bytes2 =
          new StoredMerklePatriciaTrie<>(
                  new StoredNodeFactory<>(
                      (location, hash) -> {
                        Optional<Bytes> accountStorageTrieNode =
                            getAccountStorageTrieNode(accountHash, location, hash);
                        rr.add(
                            "trie "
                                + accountHash
                                + " "
                                + slotHash
                                + " "
                                + location
                                + " "
                                + accountStorageTrieNode);
                        return accountStorageTrieNode;
                      },
                      Function.identity(),
                      Function.identity()),
                  accountValue.getStorageRoot())
              .get(slotHash)
              .map(bytes -> Bytes32.leftPad(RLP.decodeValue(bytes)));

      final Optional<Hash> storageRoot =
          getStateTrieNode(Bytes.concatenate(accountHash, Bytes.EMPTY)).map(Hash::hash);
      if (storageRoot.isPresent()) {
        Optional<Bytes> byte3r =
            new StoredMerklePatriciaTrie<>(
                    new StoredNodeFactory<>(
                        (location, hash) -> getAccountStorageTrieNode(accountHash, location, hash),
                        Function.identity(),
                        Function.identity()),
                    storageRoot.get())
                .get(slotHash)
                .map(bytes -> Bytes32.leftPad(RLP.decodeValue(bytes)));
        if (!bytes2.equals(bytes1)) {
          System.out.println(
              "storage "
                  + accountHash
                  + " "
                  + slotHash
                  + " "
                  + moreClosedByPrefix
                      .map(pair -> Bytes.wrap(pair.getKey()) + "/" + Bytes.wrap(pair.getValue()))
                      .orElse("")
                  + " "
                  + bytes2
                  + " "
                  + byte3r
                  + " "
                  + storageRoot
                  + " "
                  + accountValue.getStorageRoot());
          rr.forEach(System.out::println);
        }
      }
    }
    return bytes2;
  }

  public static void main(final String[] args) {
    System.out.println(
        CompactEncoding.bytesToPath(
            Bytes.fromHexString(
                "0xd2caf3f2d16f035d9915e583d95f19193a5ace128c1b8439d3740ad9ea72bc10")));
  }

  @Override
  public Optional<Bytes> getNodeData(final Bytes location, final Bytes32 hash) {
    return Optional.empty();
  }

  @Override
  public boolean isWorldStateAvailable(final Bytes32 rootHash, final Hash blockHash) {
    return trieBranchStorage
            .get(WORLD_ROOT_HASH_KEY)
            .map(Bytes32::wrap)
            .filter(hash -> hash.equals(rootHash))
            .isPresent()
        || trieLogStorage.containsKey(blockHash.toArrayUnsafe());
  }

  @Override
  public void clear() {
    accountStorage.clear();
    codeStorage.clear();
    storageStorage.clear();
    trieBranchStorage.clear();
    trieLogStorage.clear();
  }

  @Override
  public void clearFlatDatabase() {
    accountStorage.clear();
    storageStorage.clear();
  }

  public void pruneAccountState(final Bytes location, final Optional<Bytes> maybeExclude) {
    final Pair<Bytes, Bytes> range = generateRangeFromLocation(Bytes.EMPTY, location);

    final BonsaiIntermediateCommitCountUpdater<BonsaiWorldStateKeyValueStorage> updater =
        new BonsaiIntermediateCommitCountUpdater<>(this, 1000);

    // cleaning the account trie node in this location
    pruneTrieNode(Bytes.EMPTY, location, maybeExclude);

    updater.close();
  }

  public void pruneStorageState(
      final Bytes accountHash, final Bytes location, final Optional<Bytes> maybeExclude) {
    final Pair<Bytes, Bytes> range = generateRangeFromLocation(accountHash, location);
    final BonsaiIntermediateCommitCountUpdater<BonsaiWorldStateKeyValueStorage> updater =
        new BonsaiIntermediateCommitCountUpdater<>(this, 1000);

    // cleaning the storage trie node in this location
    pruneTrieNode(accountHash, location, maybeExclude);

    updater.close();
  }

  public void pruneCodeState(final Bytes accountHash) {
    final KeyValueStorageTransaction transaction = codeStorage.startTransaction();
    transaction.remove(accountHash.toArrayUnsafe());
    transaction.commit();
  }

  private void pruneTrieNode(
      final Bytes accountHash, final Bytes location, final Optional<Bytes> maybeExclude) {
    final BonsaiIntermediateCommitCountUpdater<BonsaiWorldStateKeyValueStorage> updater =
        new BonsaiIntermediateCommitCountUpdater<>(this, 1000);
    trieBranchStorage
        .getByPrefix(Bytes.concatenate(accountHash, location))
        .forEach(
            (key, value) -> {
              final boolean shouldExclude =
                  maybeExclude
                      .filter(
                          bytes ->
                              bytes.commonPrefixLength(key.slice(accountHash.size()))
                                  == bytes.size())
                      .isPresent();
              if (!shouldExclude) {
                ((BonsaiWorldStateKeyValueStorage.Updater) updater.getUpdater())
                    .trieBranchStorageTransaction.remove(key.toArrayUnsafe());
              }
            });
    updater.close();
  }

  @Override
  public BonsaiUpdater updater() {
    return new Updater(
        accountStorage.startTransaction(),
        codeStorage.startTransaction(),
        storageStorage.startTransaction(),
        trieBranchStorage.startTransaction(),
        trieLogStorage.startTransaction());
  }

  @Override
  public long prune(final Predicate<byte[]> inUseCheck) {
    throw new RuntimeException("Bonsai Tries do not work with pruning.");
  }

  @Override
  public long addNodeAddedListener(final NodesAddedListener listener) {
    throw new RuntimeException("addNodeAddedListener not available");
  }

  @Override
  public void removeNodeAddedListener(final long id) {
    throw new RuntimeException("removeNodeAddedListener not available");
  }

  public Optional<PeerTrieNodeFinder> getMaybeFallbackNodeFinder() {
    return maybeFallbackNodeFinder;
  }

  public void useFallbackNodeFinder(final Optional<PeerTrieNodeFinder> maybeFallbackNodeFinder) {
    checkNotNull(maybeFallbackNodeFinder);
    this.maybeFallbackNodeFinder = maybeFallbackNodeFinder;
  }

  public void safeExecute(final Consumer<KeyValueStorage> toExec) throws Exception {
    final long id = subscribe();
    toExec.accept((KeyValueStorage) this);
    unSubscribe(id);
  }

  public long subscribe() {
    // No op because close() is not implemented for BonsaiWorldStateKeyValueStorage
    return 0;
  }

  public void unSubscribe(final long id) {
    // No op because close() is not implemented for BonsaiWorldStateKeyValueStorage
  }

  @Override
  public void close() throws Exception {
    // No need to close because BonsaiWorldStateKeyValueStorage is persistent
  }

  public interface BonsaiUpdater extends WorldStateStorage.Updater {
    BonsaiUpdater removeCode(final Hash accountHash);

    BonsaiUpdater removeAccountInfoState(final Hash accountHash);

    BonsaiUpdater putAccountInfoState(final Hash accountHash, final Bytes accountValue);

    BonsaiUpdater putStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash, final Bytes storage);

    void removeStorageValueBySlotHash(final Hash accountHash, final Hash slotHash);

    KeyValueStorageTransaction getTrieBranchStorageTransaction();

    KeyValueStorageTransaction getTrieLogStorageTransaction();
  }

  public static class Updater implements BonsaiUpdater {

    private final KeyValueStorageTransaction accountStorageTransaction;
    private final KeyValueStorageTransaction codeStorageTransaction;
    private final KeyValueStorageTransaction storageStorageTransaction;
    private final KeyValueStorageTransaction trieBranchStorageTransaction;
    private final KeyValueStorageTransaction trieLogStorageTransaction;

    public Updater(
        final KeyValueStorageTransaction accountStorageTransaction,
        final KeyValueStorageTransaction codeStorageTransaction,
        final KeyValueStorageTransaction storageStorageTransaction,
        final KeyValueStorageTransaction trieBranchStorageTransaction,
        final KeyValueStorageTransaction trieLogStorageTransaction) {

      this.accountStorageTransaction = accountStorageTransaction;
      this.codeStorageTransaction = codeStorageTransaction;
      this.storageStorageTransaction = storageStorageTransaction;
      this.trieBranchStorageTransaction = trieBranchStorageTransaction;
      this.trieLogStorageTransaction = trieLogStorageTransaction;
    }

    @Override
    public BonsaiUpdater removeCode(final Hash accountHash) {
      codeStorageTransaction.remove(accountHash.toArrayUnsafe());
      return this;
    }

    @Override
    public BonsaiUpdater putCode(final Hash accountHash, final Bytes32 codeHash, final Bytes code) {
      if (code.size() == 0) {
        // Don't save empty values
        return this;
      }
      codeStorageTransaction.put(accountHash.toArrayUnsafe(), code.toArrayUnsafe());
      return this;
    }

    @Override
    public BonsaiUpdater removeAccountInfoState(final Hash accountHash) {
      accountStorageTransaction.remove(accountHash.toArrayUnsafe());
      return this;
    }

    @Override
    public BonsaiUpdater putAccountInfoState(final Hash accountHash, final Bytes accountValue) {
      if (accountValue.size() == 0) {
        // Don't save empty values
        return this;
      }
      accountStorageTransaction.put(accountHash.toArrayUnsafe(), accountValue.toArrayUnsafe());
      return this;
    }

    @Override
    public WorldStateStorage.Updater saveWorldState(
        final Bytes blockHash, final Bytes32 nodeHash, final Bytes node) {
      trieBranchStorageTransaction.put(Bytes.EMPTY.toArrayUnsafe(), node.toArrayUnsafe());
      trieBranchStorageTransaction.put(WORLD_ROOT_HASH_KEY, nodeHash.toArrayUnsafe());
      trieBranchStorageTransaction.put(WORLD_BLOCK_HASH_KEY, blockHash.toArrayUnsafe());
      return this;
    }

    @Override
    public BonsaiUpdater putAccountStateTrieNode(
        final Bytes location, final Bytes32 nodeHash, final Bytes node) {
      if (nodeHash.equals(MerklePatriciaTrie.EMPTY_TRIE_NODE_HASH)) {
        // Don't save empty nodes
        return this;
      }
      trieBranchStorageTransaction.put(location.toArrayUnsafe(), node.toArrayUnsafe());
      return this;
    }

    @Override
    public BonsaiUpdater removeAccountStateTrieNode(final Bytes location, final Bytes32 nodeHash) {
      trieBranchStorageTransaction.remove(location.toArrayUnsafe());
      return this;
    }

    @Override
    public BonsaiUpdater putAccountStorageTrieNode(
        final Hash accountHash, final Bytes location, final Bytes32 nodeHash, final Bytes node) {
      if (nodeHash.equals(MerklePatriciaTrie.EMPTY_TRIE_NODE_HASH)) {
        // Don't save empty nodes
        return this;
      }
      trieBranchStorageTransaction.put(
          Bytes.concatenate(accountHash, location).toArrayUnsafe(), node.toArrayUnsafe());
      return this;
    }

    @Override
    public synchronized BonsaiUpdater putStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash, final Bytes storage) {
      storageStorageTransaction.put(
          Bytes.concatenate(accountHash, slotHash).toArrayUnsafe(), storage.toArrayUnsafe());
      return this;
    }

    @Override
    public synchronized void removeStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash) {
      trieBranchStorageTransaction.remove(Bytes.concatenate(accountHash, slotHash).toArrayUnsafe());
    }

    @Override
    public KeyValueStorageTransaction getTrieBranchStorageTransaction() {
      return trieBranchStorageTransaction;
    }

    @Override
    public KeyValueStorageTransaction getTrieLogStorageTransaction() {
      return trieLogStorageTransaction;
    }

    @Override
    public void commit() {
      accountStorageTransaction.commit();
      codeStorageTransaction.commit();
      storageStorageTransaction.commit();
      trieBranchStorageTransaction.commit();
      trieLogStorageTransaction.commit();
    }

    @Override
    public void rollback() {
      accountStorageTransaction.rollback();
      codeStorageTransaction.rollback();
      storageStorageTransaction.rollback();
      trieBranchStorageTransaction.rollback();
      trieLogStorageTransaction.rollback();
    }
  }
}
