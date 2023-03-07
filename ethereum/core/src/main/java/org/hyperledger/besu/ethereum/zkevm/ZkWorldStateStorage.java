package org.hyperledger.besu.ethereum.zkevm;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Predicate;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class ZkWorldStateStorage implements WorldStateStorage, AutoCloseable {

  public static final byte[] WORLD_ROOT_HASH_KEY = "zkStateRoot".getBytes(StandardCharsets.UTF_8);

  public static final byte[] WORLD_BLOCK_HASH_KEY =
      "worldBlockHash".getBytes(StandardCharsets.UTF_8);

  private final KeyValueStorage trieBranch;
  private final KeyValueStorage trieLog;
  private final KeyValueStorage account;
  private final KeyValueStorage storage;
  private final KeyValueStorage code;

  public ZkWorldStateStorage(final StorageProvider provider) {
    this(
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE),
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE),
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE),
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE),
        provider.getStorageBySegmentIdentifier(KeyValueSegmentIdentifier.CODE_STORAGE));
  }

  public ZkWorldStateStorage(
      final KeyValueStorage trieBranch,
      final KeyValueStorage trieLog,
      final KeyValueStorage account,
      final KeyValueStorage storage,
      final KeyValueStorage code) {
    this.trieBranch = trieBranch;
    this.trieLog = trieLog;
    this.account = account;
    this.storage = storage;
    this.code = code;
  }

  public boolean isEmptyWorldStateStorage() {
    return trieBranch.stream().findFirst().isEmpty();
  }

  public Optional<Bytes> getWorldStateRootHash() {
    return trieBranch.get(WORLD_ROOT_HASH_KEY).map(Bytes::wrap);
  }

  public Optional<Bytes> getWorldStateBlockHash() {
    return trieBranch.get(WORLD_BLOCK_HASH_KEY).map(Bytes::wrap);
  }

  public Optional<Bytes> getAccount(final Hash hkey) {
    return account.get(hkey.toArrayUnsafe()).map(Bytes::wrap);
  }

  public Optional<Bytes> getStorageValueBySlotHash(final Hash hkey, final Hash slotHash) {
    return storage.get(Bytes.concatenate(hkey, slotHash).toArrayUnsafe()).map(Bytes::wrap);
  }

  @Override
  public Optional<Bytes> getCode(final Bytes32 codeHash, final Hash accountHash) {
    return code.get(codeHash.toArrayUnsafe()).map(Bytes::wrap);
  }

  @Override
  public Optional<Bytes> getAccountTrieNodeData(final Bytes location, final Bytes32 nodeHash) {
    return getAccountStateTrieNode(location, nodeHash);
  }

  @Override
  public Optional<Bytes> getAccountStorageTrieNode(
      final Hash accountHash, final Bytes location, final Bytes32 nodeHash) {
    return getAccountStateTrieNode(location, nodeHash);
  }

  @Override
  public Optional<Bytes> getAccountStateTrieNode(final Bytes location, final Bytes32 nodeHash) {
    return trieBranch.get(nodeHash.toArrayUnsafe()).map(Bytes::wrap);
  }

  @Override
  public Optional<Bytes> getNodeData(final Bytes location, final Bytes32 nodeHash) {
    return getAccountStateTrieNode(location, nodeHash);
  }

  @Override
  public boolean isWorldStateAvailable(final Bytes32 rootHash, final Hash blockHash) {
    return getAccountStateTrieNode(Bytes.EMPTY, rootHash).isPresent();
  }

  @Override
  public void clear() {
    account.clear();
    storage.clear();
    code.clear();
    trieLog.clear();
    trieBranch.clear();
  }

  @Override
  public void clearTrieLog() {
    trieLog.clear();
  }

  @Override
  public void clearFlatDatabase() {
    // no need to clear
  }

  @Override
  public Updater updater() {
    return new Updater(
        account.startTransaction(),
        code.startTransaction(),
        storage.startTransaction(),
        trieBranch.startTransaction(),
        trieLog.startTransaction());
  }

  @Override
  public long prune(final Predicate<byte[]> inUseCheck) {
    throw new RuntimeException("ZkEvm Tries do not work with pruning.");
  }

  @Override
  public long addNodeAddedListener(final NodesAddedListener listener) {
    throw new RuntimeException("addNodeAddedListener not available");
  }

  @Override
  public void removeNodeAddedListener(final long id) {
    throw new RuntimeException("removeNodeAddedListener not available");
  }

  @Override
  public void close() throws Exception {
    // No need to close or notify because ZkWorldStateStorage is persistent
  }

  public static class Updater implements WorldStateStorage.Updater {

    private final KeyValueStorageTransaction accountTransaction;
    private final KeyValueStorageTransaction codeTransaction;
    private final KeyValueStorageTransaction storageTransaction;
    private final KeyValueStorageTransaction trieBranchTransaction;
    private final KeyValueStorageTransaction trieLogTransaction;

    public Updater(
        final KeyValueStorageTransaction accountTransaction,
        final KeyValueStorageTransaction codeTransaction,
        final KeyValueStorageTransaction storageTransaction,
        final KeyValueStorageTransaction trieBranchTransaction,
        final KeyValueStorageTransaction trieLogTransaction) {

      this.accountTransaction = accountTransaction;
      this.codeTransaction = codeTransaction;
      this.storageTransaction = storageTransaction;
      this.trieBranchTransaction = trieBranchTransaction;
      this.trieLogTransaction = trieLogTransaction;
    }

    public WorldStateStorage.Updater removeCode(final Hash accountHash) {
      codeTransaction.remove(accountHash.toArrayUnsafe());
      return this;
    }

    @Override
    public WorldStateStorage.Updater putCode(
        final Hash accountHash, final Bytes32 codeHash, final Bytes code) {
      if (code.size() == 0) {
        // Don't save empty values
        return this;
      }
      codeTransaction.put(codeHash.toArrayUnsafe(), code.toArrayUnsafe());
      return this;
    }

    public WorldStateStorage.Updater removeAccountInfoState(final Hash accountHash) {
      accountTransaction.remove(accountHash.toArrayUnsafe());
      return this;
    }

    public WorldStateStorage.Updater putAccountInfoState(
        final Hash accountHash, final Bytes accountValue) {
      if (accountValue.size() == 0) {
        // Don't save empty values
        return this;
      }
      accountTransaction.put(accountHash.toArrayUnsafe(), accountValue.toArrayUnsafe());
      return this;
    }

    @Override
    public WorldStateStorage.Updater saveWorldState(
        final Bytes blockHash, final Bytes32 nodeHash, final Bytes node) {
      trieBranchTransaction.put(Bytes.EMPTY.toArrayUnsafe(), node.toArrayUnsafe());
      trieBranchTransaction.put(WORLD_ROOT_HASH_KEY, nodeHash.toArrayUnsafe());
      trieBranchTransaction.put(WORLD_BLOCK_HASH_KEY, blockHash.toArrayUnsafe());
      return this;
    }

    @Override
    public WorldStateStorage.Updater putAccountStateTrieNode(
        final Bytes location, final Bytes32 nodeHash, final Bytes node) {
      if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        // Don't save empty nodes
        return this;
      }
      trieBranchTransaction.put(nodeHash.toArrayUnsafe(), node.toArrayUnsafe());
      return this;
    }

    @Override
    public WorldStateStorage.Updater removeAccountStateTrieNode(
        final Bytes location, final Bytes32 nodeHash) {
      trieBranchTransaction.remove(nodeHash.toArrayUnsafe());
      return this;
    }

    @Override
    public WorldStateStorage.Updater putAccountStorageTrieNode(
        final Hash accountHash, final Bytes location, final Bytes32 nodeHash, final Bytes node) {
      if (nodeHash.equals(MerkleTrie.EMPTY_TRIE_NODE_HASH)) {
        // Don't save empty nodes
        return this;
      }
      trieBranchTransaction.put(nodeHash.toArrayUnsafe(), node.toArrayUnsafe());
      return this;
    }

    public WorldStateStorage.Updater putStorageValueBySlotHash(
        final Hash accountHash, final Hash slotHash, final Bytes storage) {
      storageTransaction.put(
          Bytes.concatenate(accountHash, slotHash).toArrayUnsafe(), storage.toArrayUnsafe());
      return this;
    }

    public void removeStorageValueBySlotHash(final Hash accountHash, final Hash slotHash) {
      storageTransaction.remove(Bytes.concatenate(accountHash, slotHash).toArrayUnsafe());
    }

    @Override
    public void commit() {
      accountTransaction.commit();
      codeTransaction.commit();
      storageTransaction.commit();
      trieBranchTransaction.commit();
      trieLogTransaction.commit();
    }

    @Override
    public void rollback() {
      accountTransaction.rollback();
      codeTransaction.rollback();
      storageTransaction.rollback();
      trieBranchTransaction.rollback();
      trieLogTransaction.rollback();
    }
  }
}
