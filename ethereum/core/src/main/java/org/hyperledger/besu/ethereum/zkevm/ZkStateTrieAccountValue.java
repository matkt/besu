package org.hyperledger.besu.ethereum.zkevm;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.worldstate.StateTrieAccountValue;

@SuppressWarnings("unused")
public class ZkStateTrieAccountValue extends StateTrieAccountValue {

  private final MimcHash zkCodeHash;
  private final MimcHash priorLeaf;
  private final MimcHash nextLeaf;
  private final MimcHash parentBranchNode;

  public MimcHash getZkCodeHash() {
    return zkCodeHash;
  }

  public Hash getKeccakCodeHash() {
    return getCodeHash();
  }

  public ZkStateTrieAccountValue(
      final long nonce,
      final Wei balance,
      final MimcHash storageRoot,
      final Hash keccakCodeHash,
      final MimcHash zkCodeHash,
      final MimcHash priorLeaf,
      final MimcHash nextLeaf,
      final MimcHash parentBranchNode) {
    super(nonce, balance, storageRoot, keccakCodeHash);
    this.zkCodeHash = zkCodeHash;
    this.nextLeaf = nextLeaf;
    this.priorLeaf = priorLeaf;
    this.parentBranchNode = parentBranchNode;
  }
}
