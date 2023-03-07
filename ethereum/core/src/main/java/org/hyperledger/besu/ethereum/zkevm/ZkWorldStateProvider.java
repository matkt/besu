package org.hyperledger.besu.ethereum.zkevm;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.worldstate.WorldState;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public class ZkWorldStateProvider implements WorldStateArchive {

  private final ZkWorldState persistedState;

  public ZkWorldStateProvider(final ZkWorldStateStorage worldStateStorage) {
    this.persistedState = new ZkWorldState(worldStateStorage);
  }

  @Override
  public boolean isWorldStateAvailable(final Hash rootHash, final Hash blockHash) {
    return persistedState.rootHash().equals(rootHash);
  }

  @Override
  public Optional<WorldState> get(final Hash rootHash, final Hash blockHash) {
    if (persistedState.rootHash().equals(rootHash)) {
      return Optional.of(persistedState);
    }
    return Optional.empty();
  }

  @Override
  public Optional<MutableWorldState> getMutable(
      final Hash rootHash, final Hash blockHash, final boolean isPersistingState) {
    if (isWorldStateAvailable(rootHash, blockHash)) {
      if (isPersistingState) {
        return Optional.of(persistedState);
      }
      return Optional.of(ZkWorldState.cloneWorldState(persistedState));
    }
    return Optional.empty();
  }

  @Override
  public Optional<MutableWorldState> getMutable(final Hash rootHash, final Hash blockHash) {
    return getMutable(rootHash, blockHash, false);
  }

  @Override
  public MutableWorldState getMutable() {
    return persistedState;
  }

  @Override
  public void setArchiveStateUnSafe(final BlockHeader blockHeader) {
    // not available
  }

  @Override
  public Optional<Bytes> getNodeData(final Hash hash) {
    return Optional.empty();
  }

  @Override
  public Optional<WorldStateProof> getAccountProof(
      final Hash worldStateRoot,
      final Address accountAddress,
      final List<UInt256> accountStorageKeys) {
    return Optional.empty();
  }
}
