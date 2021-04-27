/*
 * Copyright ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.api.jsonrpc.methods;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApi;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConsensusAssembleBlock;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConsensusFinalizeBlock;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConsensusNewBlock;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConsensusSetHead;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResultFactory;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;

import java.util.Map;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

public class ConsensusJsonRpcMethods extends ApiGroupJsonRpcMethods {

  private final BlockResultFactory blockResultFactory = new BlockResultFactory();

  private final MutableBlockchain blockchain;
  private final MiningCoordinator miningCoordinator;

  ConsensusJsonRpcMethods(
      final MutableBlockchain blockchain, final MiningCoordinator miningCoordinator) {
    this.blockchain = blockchain;
    this.miningCoordinator = miningCoordinator;
  }

  @Override
  protected RpcApi getApiGroup() {
    return RpcApis.CONSENSUS;
  }

  @Override
  protected Map<String, JsonRpcMethod> create() {
    Vertx syncVertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(1));
    return mapOf(
        new ConsensusAssembleBlock(syncVertx, blockResultFactory, blockchain, miningCoordinator),
        new ConsensusFinalizeBlock(syncVertx),
        new ConsensusNewBlock(syncVertx, blockchain),
        new ConsensusSetHead(syncVertx));
  }
}
