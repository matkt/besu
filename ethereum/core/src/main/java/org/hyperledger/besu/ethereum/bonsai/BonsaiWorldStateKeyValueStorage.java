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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.MutableBytes;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.MerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.StoredNodeFactory;
import org.hyperledger.besu.ethereum.worldstate.PeerTrieNodeFinder;
import org.hyperledger.besu.ethereum.worldstate.StateTrieAccountValue;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.rlp.RLP;

public class BonsaiWorldStateKeyValueStorage implements WorldStateStorage {
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

    final Hash accountHash = Hash.wrap(Bytes32.fromHexString("0x47682af25e2a65349e9c6d27f5fce2b26718611ef8b7de9ad8cfaacc4f5b2ae0"));
    final Optional<Bytes> account = getAccount(accountHash);
    final Optional<Bytes> worldStateRootHash = getWorldStateRootHash();
    if (account.isPresent() && worldStateRootHash.isPresent()) {
      final StateTrieAccountValue accountValue =
              StateTrieAccountValue.readFrom(
                      org.hyperledger.besu.ethereum.rlp.RLP.input(account.get()));
      Optional<Bytes> res =
              new StoredMerklePatriciaTrie<>(
                      new StoredNodeFactory<>(
                              (location, hash) -> {
                                Optional<Bytes> node  = getAccountStorageTrieNode(accountHash, location, hash);
                                System.out.println("node "+accountHash+" "+location+" "+hash+" "+node.orElse(Bytes.EMPTY));
                                return node;
                              },
                              Function.identity(),
                              Function.identity()),
                      accountValue.getStorageRoot())
                      .get(Bytes.fromHexString("0x1b6847dc741a1b0cd08d278845f9d819d87b734759afb55fe2de5cb82a9ae672"))
                      .map(bytes -> Bytes32.leftPad(RLP.decodeValue(bytes)));
      System.out.println("resultat "+res.orElse(Bytes.EMPTY));
    }
  }

  @Override
  public Optional<Bytes> getCode(final Bytes32 codeHash, final Hash accountHash) {
    return codeStorage.get(accountHash.toArrayUnsafe()).map(Bytes::wrap);
  }

  public Optional<Bytes> getAccount(final Hash accountHash) {
    Optional<Bytes> response = accountStorage.get(accountHash.toArrayUnsafe()).map(Bytes::wrap);
    final Optional<Bytes> worldStateRootHash = getWorldStateRootHash();
    Optional<Bytes> response2 = Optional.empty();
    if (worldStateRootHash.isPresent()) {
      response2 =
              new StoredMerklePatriciaTrie<>(
                      new StoredNodeFactory<>(
                              this::getAccountStateTrieNode, Function.identity(), Function.identity()),
                      Bytes32.wrap(worldStateRootHash.get()))
                      .get(accountHash);
    }
    if(!response.equals(response2)){
      System.out.println("dismatch "+response+" "+response2+" "+accountHash+" "+response.map(Hash::hash).orElse(Hash.EMPTY)+" "+response2.map(Hash::hash).orElse(Hash.EMPTY));
    }
    return response;
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
      return trieBranchStorage
          .get(location.toArrayUnsafe())
          .map(Bytes::wrap)
          .filter(b -> Hash.hash(b).equals(nodeHash));
    }
  }

  @Override
  public Optional<Bytes> getAccountStorageTrieNode(
      final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
    if (nodeHash.equals(MerklePatriciaTrie.EMPTY_TRIE_NODE_HASH)) {
      return Optional.of(MerklePatriciaTrie.EMPTY_TRIE_NODE);
    } else {
      return trieBranchStorage
          .get(Bytes.concatenate(accountHash, location).toArrayUnsafe())
          .map(Bytes::wrap)
          .filter(b -> Hash.hash(b).equals(nodeHash));
    }
  }

  public Optional<byte[]> getTrieLog(final Hash blockHash) {
    return trieLogStorage.get(blockHash.toArrayUnsafe());
  }

  public Optional<Bytes> getStateTrieNode(final Bytes location) {
    return trieBranchStorage.get(location.toArrayUnsafe()).map(Bytes::wrap);
  }

  public Optional<Bytes> getWorldStateRootHash() {
    return trieBranchStorage.get(WORLD_ROOT_HASH_KEY).map(Bytes::wrap);
  }

  public Optional<Bytes> getWorldStateBlockHash() {
    return trieBranchStorage.get(WORLD_BLOCK_HASH_KEY).map(Bytes::wrap);
  }

  public Optional<Bytes> getStorageValueBySlotHash(final Hash accountHash, final Hash slotHash) {
    Optional<Bytes> response =
        storageStorage
            .get(Bytes.concatenate(accountHash, slotHash).toArrayUnsafe())
            .map(Bytes::wrap);
    Optional<Bytes> response2 = Optional.empty();
    // after a snapsync/fastsync we only have the trie branches.
    final Optional<Bytes> account = getAccount(accountHash);
    final Optional<Bytes> worldStateRootHash = getWorldStateRootHash();
    if (account.isPresent() && worldStateRootHash.isPresent()) {
      final StateTrieAccountValue accountValue =
              StateTrieAccountValue.readFrom(
                      org.hyperledger.besu.ethereum.rlp.RLP.input(account.get()));
      response2 =
              new StoredMerklePatriciaTrie<>(
                      new StoredNodeFactory<>(
                              (location, hash) -> getAccountStorageTrieNode(accountHash, location, hash),
                              Function.identity(),
                              Function.identity()),
                      accountValue.getStorageRoot())
                      .get(slotHash)
                      .map(bytes -> Bytes32.leftPad(RLP.decodeValue(bytes)));

    }
    if(!response.equals(response2)){
      System.out.println("dismatch "+response+" "+response2+" "+accountHash+" "+slotHash+" "+response.map(Hash::hash).orElse(Hash.EMPTY)+" "+response2.map(Hash::hash).orElse(Hash.EMPTY));
    }
    return response;
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

  public void clearAccountFlatDatabaseInRange(final int index, final Bytes location, final List<Bytes> excludedLocation, final Bytes data) {
    final Pair<Bytes,Bytes> range = generateRangeFromLocation(Bytes.EMPTY, location);
    //KeyValueStorageTransaction keyValueStorageTransaction = accountStorage.startTransaction();
    accountStorage
            .getInRange(range.getLeft(), range.getRight())
            .forEach(
                    (key, value)-> {
                      final Bytes filteredLocation = CompactEncoding.bytesToPath(key);
                      final boolean shouldExclude = excludedLocation.stream().anyMatch(bytes -> filteredLocation.commonPrefixLength(bytes) == bytes.size());
                      if(!shouldExclude) {
                        System.out.println("found with method "+index+" to remove "+key+" from "+range.getLeft()+" to "+range.getRight()+" for data "+data+" and location "+location+" ");
                        //keyValueStorageTransaction.remove(key.toArrayUnsafe());
                      }
                    });
    //keyValueStorageTransaction.commit();
  }

  public void clearStorageFlatDatabaseInRange(final int index, final Bytes accountHash, final Bytes location, final List<Bytes> excludedLocation, final Bytes data) {
    final Pair<Bytes,Bytes> range = generateRangeFromLocation(accountHash, location);
    //final AtomicInteger eltRemoved = new AtomicInteger();
    //final AtomicReference<KeyValueStorageTransaction> nodeUpdaterTmp =
      //      new AtomicReference<>(storageStorage.startTransaction());

    storageStorage
            .getInRange(range.getLeft(), range.getRight())
            .forEach(
                    (key,value) -> {
                      final Bytes filteredLocation = CompactEncoding.bytesToPath(key).slice(accountHash.size() * 2);
                      final boolean shouldExclude = excludedLocation.stream().anyMatch(bytes -> filteredLocation.commonPrefixLength(bytes) == bytes.size());
                      if(!shouldExclude) {
                        System.out.println("found with method " + index + " to remove accountHash " + accountHash + " " + key + " from " + range.getLeft() + " to " + range.getRight() + " for data " + data + " and location " + location);
                        /*nodeUpdaterTmp.get().remove(key.toArrayUnsafe());
                        if (eltRemoved.getAndIncrement() % 100 == 0) {
                          nodeUpdaterTmp.get().commit();
                          nodeUpdaterTmp.set(storageStorage.startTransaction());
                        }*/
                      }
                    });
    //nodeUpdaterTmp.get().commit();
  }

  public static void main(final String[] args) {
    System.out.println(Bytes.fromHexString("0x01020304050607").slice(4));
  }

  public static Pair<Bytes, Bytes> generateRangeFromLocation(
          final Bytes prefix, final Bytes location) {

    int size = Bytes32.SIZE*2;

    final MutableBytes mutableBytes = MutableBytes.create(size+1);
    mutableBytes.fill((byte)0x00);
    mutableBytes.set(0, location);
    mutableBytes.set(size, (byte) 0x10);

    final Bytes left = Bytes.concatenate(prefix,CompactEncoding.pathToBytes(mutableBytes));

    mutableBytes.fill((byte)0x0f);
    mutableBytes.set(0, location);
    mutableBytes.set(size, (byte) 0x10);

    final Bytes right = Bytes.concatenate(prefix,CompactEncoding.pathToBytes(mutableBytes));

    return Pair.of(left,right);
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
    public BonsaiUpdater putStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash, final Bytes storage) {
      storageStorageTransaction.put(
          Bytes.concatenate(accountHash, slotHash).toArrayUnsafe(), storage.toArrayUnsafe());
      return this;
    }

    @Override
    public void removeStorageValueBySlotHash(final Hash accountHash, final Hash slotHash) {
      storageStorageTransaction.remove(Bytes.concatenate(accountHash, slotHash).toArrayUnsafe());
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
