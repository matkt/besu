package org.hyperledger.besu.ethereum.zkevm;

import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.stream.Stream;

public class ZkWorldState implements MutableWorldState {
  private final ZkWorldStateStorage zkWorldStateStorage;

  public ZkWorldState (ZkWorldStateStorage zkWorldStateStorage) {
    this.zkWorldStateStorage = zkWorldStateStorage;
  }

  @Override
  public MutableWorldState copy() {
    return null;
  }

  @Override
  public void persist(BlockHeader blockHeader) {

  }

  @Override
  public WorldUpdater updater() {
    return null;
  }

  @Override
  public Hash rootHash() {
    return null;
  }

  @Override
  public Hash frontierRootHash() {
    return null;
  }

  @Override
  public Stream<StreamableAccount> streamAccounts(Bytes32 startKeyHash, int limit) {
    return null;
  }

  @Override
  public Account get(Address address) {
    return null;
  }

}
