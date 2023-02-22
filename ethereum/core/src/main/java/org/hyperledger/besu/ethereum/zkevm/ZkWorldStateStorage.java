package org.hyperledger.besu.ethereum.zkevm;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;

import java.util.Optional;
import java.util.function.Predicate;

public class ZkWorldStateStorage implements WorldStateStorage, AutoCloseable {

  @Override
  public Optional<Bytes> getCode(Bytes32 codeHash, Hash accountHash) {
    return Optional.empty();
  }

  @Override
  public Optional<Bytes> getAccountTrieNodeData(Bytes location, Bytes32 hash) {
    return Optional.empty();
  }

  @Override
  public Optional<Bytes> getAccountStateTrieNode(Bytes location, Bytes32 nodeHash) {
    return Optional.empty();
  }

  @Override
  public Optional<Bytes> getAccountStorageTrieNode(Hash accountHash, Bytes location, Bytes32 nodeHash) {
    return Optional.empty();
  }

  @Override
  public Optional<Bytes> getNodeData(Bytes location, Bytes32 hash) {
    return Optional.empty();
  }

  @Override
  public boolean isWorldStateAvailable(Bytes32 rootHash, Hash blockHash) {
    return false;
  }

  @Override
  public void clear() {

  }

  @Override
  public void clearTrieLog() {

  }

  @Override
  public void clearFlatDatabase() {
    //TODO: sub
  }

  @Override
  public Updater updater() {
    return null;
  }

  @Override
  public long prune(Predicate<byte[]> inUseCheck) {
    return 0;
  }

  @Override
  public long addNodeAddedListener(NodesAddedListener listener) {
    return 0;
  }

  @Override
  public void removeNodeAddedListener(long id) {

  }

  @Override
  public void close() throws Exception {

  }
}
