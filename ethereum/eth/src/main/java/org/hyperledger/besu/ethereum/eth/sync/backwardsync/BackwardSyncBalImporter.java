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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBlockAccessListsFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads block access lists over eth/71 and imports backward-sync blocks. Fetches BALs in small
 * windows just-in-time so import is not blocked on a soft-limited full-batch response.
 */
public class BackwardSyncBalImporter {

  private static final Logger LOG = LoggerFactory.getLogger(BackwardSyncBalImporter.class);
  private static final int BAL_REQUEST_WINDOW = 16;

  private final BackwardSyncContext context;

  public BackwardSyncBalImporter(final BackwardSyncContext context) {
    this.context = context;
  }

  /**
   * Loads a locally stored BAL when the header advertises one. Skips storage lookups for pre-BAL
   * headers.
   */
  public Optional<BlockAccessList> lookupStoredBal(final BlockHeader header) {
    if (header.getBalHash().isEmpty()) {
      return Optional.empty();
    }
    return context.getProtocolContext().getBlockchain().getBlockAccessList(header.getHash());
  }

  /**
   * Imports {@code blocks} in order. Before each BAL-enabled block, fetches a small window of
   * missing BALs when needed; missing entries fall back to reconstruction during execution.
   */
  public CompletableFuture<Void> importBlocks(final List<Block> blocks) {
    return context
        .getEthContext()
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              if (blocks.isEmpty()) {
                context.halveBatchSize();
                LOG.debug("No blocks to save, reducing batch size to {}", context.getBatchSize());
                return CompletableFuture.completedFuture(null);
              }

              final Map<Hash, BlockAccessList> bals = new HashMap<>();
              for (int i = 0; i < blocks.size(); i++) {
                final Block block = blocks.get(i);
                maybeFetchBals(blocks, i, bals);
                if (!saveBlock(block, Optional.ofNullable(bals.get(block.getHash())))) {
                  return CompletableFuture.completedFuture(null);
                }
              }

              if (blocks.size() == context.getBatchSize()) {
                context.resetBatchSize();
              }
              return CompletableFuture.completedFuture(null);
            });
  }

  private void maybeFetchBals(
      final List<Block> blocks, final int index, final Map<Hash, BlockAccessList> bals) {
    final BlockHeader header = blocks.get(index).getHeader();
    if (header.getBalHash().isEmpty() || bals.containsKey(header.getHash())) {
      return;
    }

    final List<BlockHeader> window = nextMissingBalHeaders(blocks, index, bals);
    if (window.isEmpty()) {
      return;
    }

    LOG.atInfo()
        .setMessage("Requesting {} BAL(s) at block {} ({}/{} already cached)")
        .addArgument(window::size)
        .addArgument(header::getNumber)
        .addArgument(bals::size)
        .addArgument(blocks::size)
        .log();

    final int before = bals.size();
    bals.putAll(fetchBals(window));
    LOG.atInfo()
        .setMessage("BAL download: +{} this round, {}/{} cached")
        .addArgument(bals.size() - before)
        .addArgument(bals::size)
        .addArgument(blocks::size)
        .log();
  }

  private List<BlockHeader> nextMissingBalHeaders(
      final List<Block> blocks, final int startIndex, final Map<Hash, BlockAccessList> bals) {
    final List<BlockHeader> window = new ArrayList<>(BAL_REQUEST_WINDOW);
    for (int i = startIndex; i < blocks.size() && window.size() < BAL_REQUEST_WINDOW; i++) {
      final BlockHeader header = blocks.get(i).getHeader();
      if (header.getBalHash().isPresent() && !bals.containsKey(header.getHash())) {
        window.add(header);
      }
    }
    return window;
  }

  private Map<Hash, BlockAccessList> fetchBals(final List<BlockHeader> headers) {
    try {
      final PeerTaskExecutorResult<List<Optional<BlockAccessList>>> result =
          context
              .getEthContext()
              .getPeerTaskExecutor()
              .execute(new GetBlockAccessListsFromPeerTask(headers));
      if (result.responseCode() != PeerTaskExecutorResponseCode.SUCCESS
          || result.result().isEmpty()) {
        LOG.atInfo()
            .setMessage("BAL download unsuccessful ({}), continuing without")
            .addArgument(result::responseCode)
            .log();
        return Map.of();
      }
      final Map<Hash, BlockAccessList> fetched = new HashMap<>();
      final List<Optional<BlockAccessList>> downloaded = result.result().get();
      final int count = Math.min(headers.size(), downloaded.size());
      for (int i = 0; i < count; i++) {
        final Optional<BlockAccessList> maybeBal = downloaded.get(i);
        if (maybeBal.isPresent()) {
          fetched.put(headers.get(i).getHash(), maybeBal.get());
        }
      }
      return fetched;
    } catch (final RuntimeException e) {
      LOG.atInfo()
          .setMessage("BAL download failed ({}), continuing without")
          .addArgument(e::toString)
          .log();
      return Map.of();
    }
  }

  private boolean saveBlock(final Block block, final Optional<BlockAccessList> bal) {
    final Optional<BlockHeader> parent =
        context
            .getProtocolContext()
            .getBlockchain()
            .getBlockHeader(block.getHeader().getParentHash());
    if (parent.isEmpty()) {
      context.halveBatchSize();
      LOG.atDebug()
          .setMessage(
              "Parent block {} not found, while saving block {}, reducing batch size to {}")
          .addArgument(block.getHeader().getParentHash())
          .addArgument(block::toLogString)
          .addArgument(context::getBatchSize)
          .log();
      return false;
    }
    context.saveBlock(block, bal);
    return true;
  }
}
