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

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.MigrationProgressResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.PbtMigrationIntrospection;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;

/** {@code debug_migrationProgress} — reports where the binary-tree migration stands. */
public class DebugMigrationProgress implements JsonRpcMethod {

  private final ProtocolContext protocolContext;

  public DebugMigrationProgress(final ProtocolContext protocolContext) {
    this.protocolContext = protocolContext;
  }

  @Override
  public String getName() {
    return RpcMethod.DEBUG_MIGRATION_PROGRESS.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final WorldStateArchive archive = protocolContext.getWorldStateArchive();
    final MigrationProgressResult result;
    if (archive instanceof BonsaiWorldStateProvider bonsai) {
      result =
          PbtMigrationIntrospection.progress(
              bonsai.getWorldStateKeyValueStorage(),
              protocolContext.getBlockchain(),
              bonsai.getBinaryTrieMilestone());
    } else {
      result = MigrationProgressResult.inactive();
    }
    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), result);
  }
}
