package org.hyperledger.besu.ethereum.zkevm;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.sparse.EmptyLeafNode;
import org.hyperledger.besu.ethereum.trie.sparse.StoredNodeFactory;
import org.hyperledger.besu.ethereum.trie.zkevm.ZKTrie;
import org.hyperledger.besu.ethereum.worldstate.StateTrieAccountValue;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorage;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

public class ZkWorldState implements MutableWorldState, ZkWorldView {

  private final ZkWorldStateStorage zkWorldStateStorage;

  protected final ZkEvmAccumulator accumulator;
  private boolean isFrozen = false;

  public ZkWorldState(final ZkWorldStateStorage zkWorldStateStorage) {
    this.zkWorldStateStorage = zkWorldStateStorage;
    this.accumulator = new ZkEvmAccumulator(this);
    initWorldState();
  }

  public void initWorldState() {
    if (zkWorldStateStorage.isEmptyWorldStateStorage()) {
      final WorldStateStorage.Updater updater = zkWorldStateStorage.updater();
      final Node<Bytes> rootNode = ZKTrie.initWorldState((location, hash, value) -> updater.putAccountStateTrieNode(null, hash, value));
      updater.saveWorldState(
              Bytes.EMPTY, rootNode.getHash(), rootNode.getEncodedBytes()); // TODO pass the genesis block hash
      updater.commit();
    }

    ZKTrie merkleTrie =
        new ZKTrie(
            zkWorldStateStorage.getWorldStateRootHash().map(Bytes32::wrap).orElseThrow(),
            key -> {
              if(key.isEmpty())return Optional.of(BigInteger.ZERO);
              return Optional.of(BigInteger.ONE);
            },
            zkWorldStateStorage::getAccountStateTrieNode);
    System.out.println(merkleTrie.get(Bytes.EMPTY));
    merkleTrie.put(Bytes.EMPTY, RLP.encode(new StateTrieAccountValue(2, Wei.ONE, Hash.EMPTY, Hash.EMPTY)::writeTo));
    System.out.println(merkleTrie.get(Bytes.EMPTY));
    merkleTrie.put(Bytes.of(1), RLP.encode(new StateTrieAccountValue(3, Wei.ONE, Hash.EMPTY, Hash.EMPTY)::writeTo));
    System.out.println(merkleTrie.get(Bytes.of(1)));
    merkleTrie.commit(new NodeUpdater() {
      @Override
      public void store(final Bytes location, final Bytes32 hash, final Bytes value) {
        System.out.println("commit "+location+" "+hash+" "+value);
      }
    });
  }

  @Override
  public void persist(final BlockHeader blockHeader) {
    if (isFrozen) { // no commit when the state is frozen
      return;
    }
  }

  @Override
  public WorldUpdater updater() {
    return accumulator;
  }

  @Override
  public Stream<StreamableAccount> streamAccounts(final Bytes32 startKeyHash, final int limit) {
    throw new RuntimeException("Bonsai Tries do not provide account streaming.");
  }

  @Override
  public Account get(final Address address) {
    return zkWorldStateStorage
        .getAccount(Hash.hash(address))
        .map(bytes -> ZkAccount.fromRLP(accumulator, address, bytes, true))
        .orElse(null);
  }

  @Override
  public Optional<Bytes> getCode(final Address address, final Hash codeHash) {
    return Optional.empty();
  }

  @Override
  public UInt256 getStorageValue(final Address address, final UInt256 storageKey) {
    return getStorageValueBySlotHash(address, Hash.hash(storageKey)).orElse(UInt256.ZERO);
  }

  @Override
  public Optional<UInt256> getStorageValueBySlotHash(final Address address, final Hash slotHash) {
    return zkWorldStateStorage
        .getStorageValueBySlotHash(Hash.hash(address), slotHash)
        .map(UInt256::fromBytes);
  }

  @Override
  public UInt256 getPriorStorageValue(final Address address, final UInt256 storageKey) {
    return getStorageValue(address, storageKey);
  }

  @Override
  public Map<Bytes32, Bytes> getAllAccountStorage(final Address address, final Hash rootHash) {
    throw new UnsupportedOperationException("getAllAccountStorage is not available for zkevm");
  }

  @Override
  public Hash rootHash() {
    return zkWorldStateStorage
        .getWorldStateRootHash()
        .map(Bytes32::wrap)
        .map(Hash::wrap)
        .orElseThrow();
  }

  @Override
  public Hash frontierRootHash() {
    throw new UnsupportedOperationException("frontierRootHash is not available for zkevm");
  }

  public Hash blockHash() {
    return zkWorldStateStorage
        .getWorldStateBlockHash()
        .map(Bytes32::wrap)
        .map(Hash::wrap)
        .orElseThrow();
  }

  public void freeze() {
    isFrozen = true;
  }

  public static MutableWorldState cloneWorldState(final ZkWorldState zkWorldState) {
    final ZkWorldState ws = new ZkWorldState(zkWorldState.zkWorldStateStorage);
    ws.freeze();
    return ws;
  }

  @Override
  public MutableWorldState copy() {
    throw new UnsupportedOperationException("deprecated method");
  }

  @Override
  public boolean isPersistable() {
    throw new UnsupportedOperationException("deprecated method");
  }
}
