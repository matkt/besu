/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.services;

import static org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.trie.pathbased.common.cache.PathBasedCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.worldstate.WorldView;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.WorldStateService;

import java.util.Optional;

/**
 * Implementation of the {@link WorldStateService} that provides access to Besu's world state.
 *
 * <p>This implementation delegates world state operations to the underlying {@link
 * WorldStateArchive}.
 */
@Unstable
public class WorldStateServiceImpl implements WorldStateService {
  private final BlockHeaderFunctions blockHeaderFunctions;
  private final WorldStateArchive worldStateArchive;
  private final Blockchain blockchain;

  /**
   * Constructs a new WorldStateServiceImpl.
   *
   * @param protocolSchedule The protocol schedule used
   * @param worldStateArchive The world state archive that provides access to world state data
   * @param blockchain The blockchain instance used to retrieve block headers
   */
  public WorldStateServiceImpl(
      final ProtocolSchedule protocolSchedule,
      final WorldStateArchive worldStateArchive,
      final Blockchain blockchain) {
    this.worldStateArchive = worldStateArchive;
    this.blockchain = blockchain;
    this.blockHeaderFunctions = ScheduleBasedBlockHeaderFunctions.create(protocolSchedule);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns a view of the current world state by delegating to the underlying world state
   * archive.
   *
   * @return A view of the current world state
   */
  @Override
  public WorldView getWorldView() {
    return worldStateArchive.getWorldState();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns a view of the world state at the specified block hash by retrieving the block header
   * and then fetching the corresponding world state from the archive.
   *
   * @param blockHash The hash of the block for which to retrieve the world view
   * @return An optional containing the world view if the block exists, or empty if it does not
   */
  @Override
  public Optional<WorldView> getWorldView(final Hash blockHash) {
    return blockchain
        .getBlockHeader(blockHash)
        .flatMap(
            header -> worldStateArchive.getWorldState(withBlockHeaderAndNoUpdateNodeHead(header)));
  }

  @Override
  public void cacheWorldView(final BlockHeader blockHeader, final WorldView worldView) {
    if (worldStateArchive instanceof PathBasedWorldStateProvider pathBasedWorldStateProvider) {
      PathBasedCachedWorldStorageManager cachedWorldStorageManager =
          pathBasedWorldStateProvider.getCachedWorldStorageManager();
      org.hyperledger.besu.ethereum.core.BlockHeader header =
          org.hyperledger.besu.ethereum.core.BlockHeader.convertPluginBlockHeader(
              blockHeader, blockHeaderFunctions);
      cachedWorldStorageManager.removeCachedLayer(header);
      cachedWorldStorageManager.addCachedLayer(
          header, header.getStateRoot(), (PathBasedWorldState) worldView);
    }
  }
}
