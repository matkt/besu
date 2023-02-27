package org.hyperledger.besu.ethereum.zkevm;

import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.datatypes.Hash;

public class MimcHash extends Hash {
  private MimcHash(final Bytes32 bytes) {
    super(bytes);
  }

  public static MimcHash of(final Bytes32 value) {
    //TODO: use MiMC hash function once available
    return new MimcHash(value);
  }
}
