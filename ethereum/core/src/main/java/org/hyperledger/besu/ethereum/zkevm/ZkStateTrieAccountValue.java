package org.hyperledger.besu.ethereum.zkevm;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.worldstate.StateTrieAccountValue;

public class ZkStateTrieAccountValue<T extends Hash> extends StateTrieAccountValue {

  private final T zkCodeHash;
  private final T priorLeaf;
  private final T nextLeaf;
  private final T parentBranchNode;

  public Hash getZkCodeHash() {
    return zkCodeHash;
  }

  public Hash getKeccakCodeHash() {
    return getCodeHash();
  }

  public ZkStateTrieAccountValue(
        final long nonce,
        final Wei balance,
        final T storageRoot,
        final Hash keccakCodeHash,
        final T zkCodeHash,
        final T priorLeaf,
        final T nextLeaf,
        final T parentBranchNode) {
      super(nonce, balance, storageRoot, keccakCodeHash);
      this.zkCodeHash = zkCodeHash;
      this.nextLeaf = nextLeaf;
      this.priorLeaf = priorLeaf;
      this.parentBranchNode = parentBranchNode;
    }

}
