package org.hyperledger.besu.ethereum.zkevm;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;

import java.util.Optional;
import java.util.function.Predicate;

public class ZkWorldStateStorage implements WorldStateStorage, AutoCloseable {

  private final KeyValueStorage trieStorage;
  private final KeyValueStorage trieLogStorage;
  private final KeyValueStorage accountStorage;
  private final KeyValueStorage storageStorage;

  public ZkWorldStateStorage(KeyValueStorage trieStorage, KeyValueStorage trieLogStorage, KeyValueStorage accountStorage, KeyValueStorage storageStorage) {
    this.trieStorage = trieStorage;
    this.trieLogStorage = trieLogStorage;
    this.accountStorage = accountStorage;
    this.storageStorage = storageStorage;
  }

  @Override
  public Optional<Bytes> getCode(Bytes32 codeHash, Hash accountHash) {
    //TODO: implement
    return Optional.empty();
  }

  @Override
  public Optional<Bytes> getAccountTrieNodeData(Bytes location, Bytes32 hash) {
    //TODO: implement
    return Optional.empty();
  }

  @Override
  public Optional<Bytes> getAccountStateTrieNode(Bytes location, Bytes32 nodeHash) {
    //TODO: implement
    return Optional.empty();
  }

  @Override
  public Optional<Bytes> getAccountStorageTrieNode(Hash accountHash, Bytes location, Bytes32 nodeHash) {
    //TODO: implement
    return Optional.empty();
  }

  @Override
  public Optional<Bytes> getNodeData(Bytes location, Bytes32 hash) {
    //TODO: implement
    return Optional.empty();
  }

  @Override
  public boolean isWorldStateAvailable(Bytes32 rootHash, Hash blockHash) {
    //TODO: implement
    return false;
  }

  @Override
  public void clear() {
    //TODO: implement
  }

  @Override
  public void clearTrieLog() {
    //TODO: implement
  }

  @Override
  public void clearFlatDatabase() {
  }

  @Override
  public Updater updater() {
    return null;
  }

  @Override
  public long prune(Predicate<byte[]> inUseCheck) {
    //TODO: implement?
    return 0;
  }

  @Override
  public long addNodeAddedListener(NodesAddedListener listener) {
    //TODO: implement
    return 0;
  }

  @Override
  public void removeNodeAddedListener(long id) {
    //TODO: implement
  }

  @Override
  public void close() throws Exception {
    //TODO: defer to worldstate storage close
  }
}
