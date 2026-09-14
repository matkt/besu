/*
 * Copyright contributors to Besu.
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

import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.BINARY_TRIE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.PbtMigrationIntrospection;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.PbtMigrator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

import java.util.Optional;

/**
 * {@code debug_shadowStateRoot} — returns the migration's recorded shadow root for a block, or
 * {@code null} when none exists.
 */
public class DebugShadowStateRoot implements JsonRpcMethod {

  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;
  private final Optional<PbtMigrator> pbtMigrator;

  public DebugShadowStateRoot(
      final ProtocolContext protocolContext, final ProtocolSchedule protocolSchedule) {
    this(protocolContext, protocolSchedule, Optional.empty());
  }

  public DebugShadowStateRoot(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final PbtMigrator pbtMigrator) {
    this(protocolContext, protocolSchedule, Optional.of(pbtMigrator));
  }

  private DebugShadowStateRoot(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final Optional<PbtMigrator> pbtMigrator) {
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
    this.pbtMigrator = pbtMigrator;
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_SHADOW_STATE_ROOT.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final Hash blockHash;
    try {
      blockHash = requestContext.getRequiredParameter(0, Hash.class);
    } catch (final JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid block hash parameter (index 0)", RpcErrorType.INVALID_BLOCK_HASH_PARAMS, e);
    }

    // Unknown block: null, matching geth (no error).
    if (protocolContext.getBlockchain().getBlockHeader(blockHash).isEmpty()) {
      return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), null);
    }

    final WorldStateArchive archive = protocolContext.getWorldStateArchive();
    if (!(archive instanceof BonsaiWorldStateProvider bonsai)) {
      return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), null);
    }

    final Hash root =
        PbtMigrationIntrospection.shadowStateRoot(
                bonsai.getWorldStateKeyValueStorage(),
                protocolContext.getBlockchain(),
                protocolSchedule.milestoneFor(BINARY_TRIE),
                blockHash,
                pbtMigrator)
            .orElse(null);
    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), root);
  }
}
