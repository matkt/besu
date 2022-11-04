package org.hyperledger.besu.ethereum.bonsai;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.CompactEncoding;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.RemoveVisitor;
import org.hyperledger.besu.ethereum.trie.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.StoredNodeFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.function.Function;

import kotlin.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class BonsaiInMemoryCalculateRootHashTask
    extends RecursiveTask<StoredMerklePatriciaTrie<Bytes, Bytes>> {

  private final StoredMerklePatriciaTrie<Bytes, Bytes> trie;
  private final Bytes location;
  private final List<Pair<Address, BonsaiValue<BonsaiAccount>>> updatedAccounts;

  private final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage;

  public BonsaiInMemoryCalculateRootHashTask(
      final StoredMerklePatriciaTrie<Bytes, Bytes> trie,
      final Bytes location,
      final List<Pair<Address, BonsaiValue<BonsaiAccount>>> updatedAccounts,
      final BonsaiWorldStateKeyValueStorage worldStateKeyValueStorage) {
    this.trie = trie;
    this.location = location;
    this.updatedAccounts = updatedAccounts;
    this.worldStateKeyValueStorage = worldStateKeyValueStorage;
  }

  @Override
  protected StoredMerklePatriciaTrie<Bytes, Bytes> compute() {
    if (updatedAccounts.size() == 1) {
      final Pair<Address, BonsaiValue<BonsaiAccount>> firstAccount = updatedAccounts.get(0);
      final Bytes path = CompactEncoding.bytesToPath(Hash.hash(firstAccount.getFirst()));
      final BonsaiAccount bonsaiAccount = firstAccount.getSecond().getUpdated();
      if (bonsaiAccount == null) {
          trie.removePath(path.slice(location.size()), new RemoveVisitor<>());
      } else {
        trie.putWithPath(path.slice(location.size()), bonsaiAccount.serializeAccount());
      }
    } else {
      final StoredNodeFactory<Bytes> nodeFactory =
          new StoredNodeFactory<>(
              worldStateKeyValueStorage::getAccountStateTrieNode,
              Function.identity(),
              Function.identity());
      final List<BonsaiInMemoryCalculateRootHashTask> tasks = createTasks();
      // execute all the tasks and wait for the results
      ForkJoinTask.invokeAll(tasks)
          .forEach(
              subTask -> {
                  trie.putWithPath(
                      subTask.location.slice(location.size()),
                      nodeFactory.decode(subTask.location, subTask.trie.getRoot().getRlp()));
              });
    }
    return trie;
  }

  public List<BonsaiInMemoryCalculateRootHashTask> createTasks() {
    final List<BonsaiInMemoryCalculateRootHashTask> tasks = new ArrayList<>();
    final Map<Bytes, List<Pair<Address, BonsaiValue<BonsaiAccount>>>> multimap = new HashMap<>();
    updatedAccounts.forEach(
        entry -> {
          final Bytes path = CompactEncoding.bytesToPath(Hash.hash(entry.getFirst()));
          final Bytes prefix = Bytes.of(path.slice(location.size()).get(0));
          multimap.computeIfAbsent(prefix, bytes -> new ArrayList<>()).add(entry);
        });
    multimap.forEach(
        (key, value) -> {
          final Bytes rootLocation = Bytes.concatenate(location, key);
          final Node<Bytes> nodeForPath = trie.getNodeForPath(key);
          final StoredMerklePatriciaTrie<Bytes, Bytes> subAccountTrie =
              new StoredMerklePatriciaTrie<>(
                  (loc, hash) -> worldStateKeyValueStorage.getAccountStateTrieNode(Bytes.concatenate(rootLocation,loc), hash),
                  nodeForPath,
                  Function.identity(),
                  Function.identity());
          tasks.add(
              new BonsaiInMemoryCalculateRootHashTask(
                  subAccountTrie, rootLocation, value, worldStateKeyValueStorage));
        });
    return tasks;
  }
}