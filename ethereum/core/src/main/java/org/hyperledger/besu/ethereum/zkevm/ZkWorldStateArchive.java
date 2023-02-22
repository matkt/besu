package org.hyperledger.besu.ethereum.zkevm;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.worldstate.WorldState;

import java.util.List;
import java.util.Optional;

public class ZkWorldStateArchive implements WorldStateArchive {
  @Override
  public Optional<WorldState> get(Hash rootHash, Hash blockHash) {
    return Optional.empty();
  }

  @Override
  public boolean isWorldStateAvailable(Hash rootHash, Hash blockHash) {
    return false;
  }

  @Override
  public Optional<MutableWorldState> getMutable(Hash rootHash, Hash blockHash, boolean isPersistingState) {
    return Optional.empty();
  }

  @Override
  public Optional<MutableWorldState> getMutable(Hash rootHash, Hash blockHash) {
    return Optional.empty();
  }

  @Override
  public MutableWorldState getMutable() {
    return null;
  }

  @Override
  public void setArchiveStateUnSafe(BlockHeader blockHeader) {

  }

  @Override
  public Optional<Bytes> getNodeData(Hash hash) {
    return Optional.empty();
  }

  @Override
  public Optional<WorldStateProof> getAccountProof(Hash worldStateRoot, Address accountAddress, List<UInt256> accountStorageKeys) {
    return Optional.empty();
  }
}
