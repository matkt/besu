package org.hyperledger.besu.ethereum.api.jsonrpc.methods;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApi;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;

import java.util.Collections;
import java.util.Map;

public class ConsensusJsonRpcMethods extends ApiGroupJsonRpcMethods {
  @Override
  protected RpcApi getApiGroup() {
    return RpcApis.CONSENSUS;
  }

  @Override
  protected Map<String, JsonRpcMethod> create() {
    return Collections.emptyMap();
  }
}
