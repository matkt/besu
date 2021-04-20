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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.BlockParameterOrBlockHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.proof.GetProofResult;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.Address;
import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.core.Wei;
import org.hyperledger.besu.ethereum.core.WorldState;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.tuweni.units.bigints.UInt256;

public class EthGetProof extends AbstractBlockParameterOrBlockHashMethod {
  public EthGetProof(final BlockchainQueries blockchain) {
    super(blockchain);
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_GET_PROOF.getMethodName();
  }

  @Override
  protected BlockParameterOrBlockHash blockParameterOrBlockHash(
      final JsonRpcRequestContext request) {
    return request.getRequiredParameter(2, BlockParameterOrBlockHash.class);
  }

  @Override
  protected Object resultByBlockHash(
      final JsonRpcRequestContext requestContext, final Hash blockHash) {



    final Address address = requestContext.getRequiredParameter(0, Address.class);
    final List<UInt256> storageKeys = getStorageKeys(requestContext);

    final Optional<WorldState> worldState = getBlockchainQueries().getWorldState(blockHash);

    if (worldState.isPresent()) {
      Optional<WorldStateProof> proofOptional =
          getBlockchainQueries()
              .getWorldStateArchive()
              .getAccountProof(worldState.get().rootHash(), address, storageKeys);

      String[] add = new String[]{
              "0x4cd99a1fc780d874a34008fdc6da2961d540fe64","0xfb4384bc0b9246adb3897a059ac02bb79bbf00a1","0x13b86ce11f24199afdea058789ec6c9ed654343b","0x01e8338b7931d21755586f119726a70cd7805bc7","0x743f2204a4b289183434261f2c6c3e7df25c2376","0x72d052c4b4e60cb6c66bbf640b1e8fb65f1c7999","0x920173254831280124dc513039c8292b1dffb635","0x0ad71aa8b01a77abc7a8f40cc323b592256c3664","0x6eb4ac5ec08dbb0a8586468877fb49b72d83a35d","0xbb1e92cafee547be9491724932be28b974f9dc27","0x353abc416e98045d7d3f5b1723d93cda62f56627","0x509decc74727b7f0842ccac8123f948214e42eea","0x01a6f69a0b2479dc6c076b64a76e2a24e8c0649d","0xddcebf99bb8084da1056d7d2c2e8668a83cd53bf","0x361aebbf105048833d29e7d76fa2b2acf012024c","0x7a250d5630b4cf539739df2c5dacb4c659f2488d","0xbf4ab43de442f808f71c2fbb959e88d8b493acfd","0x308354456d4a3d594a0db0b154e1a52ed3312067","0x0000000000000000000000000000000000000005","0xbaf0294bca1d35be6bfc300543da620cf9f720cc","0xd559f376dcfa974dc9e0712f20474154f7214f2e","0xc877ec4f22317a30ba04a17796d4497490a76e22","0x1948c20cc492539968bb9b041f96d6556b4b7001","0x34d4e8ffe35e1ef13c052ba5e32e1d13bd0ffdd6","0x9b85c57222826d82dd106e8455d3918846b507d5","0xdea6f16e104503a5a481f89d437c97f5b15a2c7c","0x54dead93dac7b216923f769e0fd7bcb444c513b4","0x283af0b28c62c092c9727f1ee09c02ca627eb7f5","0x90a3acf5c49e64a28b438ebc50c23c6df453b1cc","0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85","0x62071429eab59c9da0c7bb11a5658aee5ed065d9","0x71c46ed333c35e4e6c62d32dc7c8f00d125b4fee","0xefc96d6dc2d6870447dd8875f0dd3942822c8454","0xf0ec41069a89595adf5f27a4a90ff2df30d83d2e","0x864f0bb8ce26a5157c247a8466d5b8cb0677aa1a","0x04b156131d534f0d00b36845edb483cde165e526","0x507d00ad947db8cf1368c877e065cb9aba4cdc54","0x81a0f96f45eb0c4358dcc22f7311aed9d5300fad","0x65e92e892fbb489ea263c8e52bb11d1c9b67c54d","0xd3dfcdbd54c328b6f3002ebe30ecc4d65bb31571","0xd3f777e7f492d7783635701f5a698214c6bc1d04","0x8468b2bdce073a157e560aa4d9ccf6db1db98507","0xb975a9996993d02d76cd5c04ff45404ce8917afa"
      };
      for (String ad: add ) {
        if(proofOptional.isPresent()) {
          GetProofResult getProofResult = GetProofResult.buildGetProofResult(Address.fromHexString(ad), proofOptional.get());
          System.out.println("AccountState{"
                  + "address="
                  + getProofResult.getAddress()
                  + ", nonce="
                  + getProofResult.getNonce2()
                  + ", balance="
                  + Wei.fromHexString(getProofResult.getBalance())
                  + ", storageRoot="
                  + getProofResult.getStorageHash()
                  + ", codeHash="
                  + getProofResult.getCodeHash()
                  + ", version="
                  + 0
                  + '}');
        }
      }
      return proofOptional
          .map(
              proof ->
                  (JsonRpcResponse)
                      new JsonRpcSuccessResponse(
                          requestContext.getRequest().getId(),
                          GetProofResult.buildGetProofResult(address, proof)))
          .orElse(
              new JsonRpcErrorResponse(
                  requestContext.getRequest().getId(), JsonRpcError.NO_ACCOUNT_FOUND));
    }

    return new JsonRpcErrorResponse(
        requestContext.getRequest().getId(), JsonRpcError.WORLD_STATE_UNAVAILABLE);
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    return (JsonRpcResponse) handleParamTypes(requestContext);
  }

  private List<UInt256> getStorageKeys(final JsonRpcRequestContext request) {
    return Arrays.stream(request.getRequiredParameter(1, String[].class))
        .map(UInt256::fromHexString)
        .collect(Collectors.toList());
  }
}
