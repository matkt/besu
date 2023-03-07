package org.hyperledger.besu.ethereum.zkevm;

import org.hyperledger.besu.datatypes.Hash;

import java.util.HashMap;
import java.util.Map;

import org.apache.tuweni.units.bigints.UInt256;

@SuppressWarnings("unused")
public class ZkTrieLog {

  private final Map<Hash, ZkValue<ZkStateTrieAccountValue>> accountsChanged;
  private final Map<Hash, ZkValue<UInt256>> storageChanged;

  public ZkTrieLog() {
    this.accountsChanged = new HashMap<>();
    this.storageChanged = new HashMap<>();
  }
}
