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
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBlockAccessListsFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForwardSyncStep {

  private static final Logger LOG = LoggerFactory.getLogger(ForwardSyncStep.class);
  /** Small windows keep eth soft-limited responses useful and let import start sooner. */
  private static final int DEFAULT_BAL_REQUEST_WINDOW = 16;
  /** Stalled rounds before importing the next block without a BAL. */
  private static final int DEFAULT_BAL_STALL_ATTEMPTS = 2;
  private static final Duration DEFAULT_BAL_PEER_WAIT_TIMEOUT = Duration.ofSeconds(2);

  private final BackwardSyncContext context;
  private final BackwardChain backwardChain;

  public ForwardSyncStep(final BackwardSyncContext context, final BackwardChain backwardChain) {
    this.context = context;
    this.backwardChain = backwardChain;
  }

  public CompletableFuture<Void> executeAsync() {
    return CompletableFuture.supplyAsync(
            () -> backwardChain.getFirstNAncestorHeaders(context.getBatchSize()))
        .thenCompose(this::possibleRequestBodies);
  }

  @VisibleForTesting
  public CompletableFuture<Void> possibleRequestBodies(final List<BlockHeader> blockHeaders) {
    if (blockHeaders.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    } else {
      LOG.atDebug()
          .setMessage("Requesting {} blocks {}->{} ({})")
          .addArgument(blockHeaders::size)
          .addArgument(() -> blockHeaders.getFirst().getNumber())
          .addArgument(() -> blockHeaders.getLast().getNumber())
          .addArgument(() -> blockHeaders.getFirst().getHash().getBytes().toHexString())
          .log();
      return requestBodies(blockHeaders)
          .thenCompose(this::downloadBalsAndSaveBlocks)
          .exceptionally(
              throwable -> {
                context.halveBatchSize();
                LOG.atDebug()
                    .setMessage(
                        "Getting {} blocks from peers failed with reason {}, reducing batch size to {}")
                    .addArgument(blockHeaders::size)
                    .addArgument(throwable::getMessage)
                    .addArgument(context::getBatchSize)
                    .log();
                return null;
              });
    }
  }

  @VisibleForTesting
  protected CompletableFuture<List<Block>> requestBodies(final List<BlockHeader> blockHeaders) {
    return context
        .getEthContext()
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              GetBodiesFromPeerTask task =
                  new GetBodiesFromPeerTask(
                      blockHeaders,
                      context.getProtocolSchedule(),
                      context.getEthContext().getEthPeers().peerCount());
              PeerTaskExecutorResult<List<Block>> taskResult =
                  context.getEthContext().getPeerTaskExecutor().execute(task);
              if (taskResult.responseCode() == PeerTaskExecutorResponseCode.SUCCESS
                  && taskResult.result().isPresent()) {
                return CompletableFuture.completedFuture(taskResult.result().get());
              } else {
                return CompletableFuture.failedFuture(
                    new RuntimeException(taskResult.responseCode().toString()));
              }
            })
        .thenApply(
            blocks -> {
              LOG.debug("Got {} blocks from peers", blocks.size());
              blocks.sort(Comparator.comparing(block -> block.getHeader().getNumber()));
              return blocks;
            });
  }

  /**
   * Pipelines BAL download with import: fetch a small window of BALs, import every consecutive
   * block that already has one (or needs none), and only stall briefly before falling back to
   * reconstruction. This avoids waiting for a full soft-limited 200-BAL download before importing.
   */
  @VisibleForTesting
  protected CompletableFuture<Void> downloadBalsAndSaveBlocks(final List<Block> blocks) {
    if (blocks.isEmpty()) {
      context.halveBatchSize();
      LOG.debug("No blocks to save, reducing batch size to {}", context.getBatchSize());
      return CompletableFuture.completedFuture(null);
    }
    return downloadBalsAndSaveBlocks(blocks, 0, new HashMap<>(), balStallAttempts());
  }

  private CompletableFuture<Void> downloadBalsAndSaveBlocks(
      final List<Block> blocks,
      final int startIndex,
      final Map<Hash, BlockAccessList> collected,
      final int stallAttemptsRemaining) {
    return context
        .getEthContext()
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              int index = startIndex;
              int stallLeft = stallAttemptsRemaining;

              // Import any ready prefix before spending another peer round.
              final int importedBeforeFetch = importReadyPrefix(blocks, index, collected);
              if (importedBeforeFetch < 0) {
                return CompletableFuture.completedFuture(null);
              }
              index += importedBeforeFetch;
              if (index >= blocks.size()) {
                finishBatch(blocks.size());
                return CompletableFuture.completedFuture(null);
              }

              final List<BlockHeader> pendingWindow = nextBalWindow(blocks, index, collected);
              if (pendingWindow.isEmpty()) {
                // Remaining blocks do not advertise a BAL hash — import the rest.
                while (index < blocks.size()) {
                  if (!saveOneBlock(blocks.get(index), Optional.empty())) {
                    return CompletableFuture.completedFuture(null);
                  }
                  index++;
                }
                finishBatch(blocks.size());
                return CompletableFuture.completedFuture(null);
              }

              final long windowStartNumber = blocks.get(index).getHeader().getNumber();
              final int indexForRetry = index;
              final int stallForRetry = stallLeft;
              LOG.atInfo()
                  .setMessage(
                      "Requesting {} BAL(s) for import window at block {} ({}/{} bals cached, {} stall attempts left)")
                  .addArgument(pendingWindow::size)
                  .addArgument(windowStartNumber)
                  .addArgument(collected::size)
                  .addArgument(blocks::size)
                  .addArgument(stallForRetry)
                  .log();

              final Optional<Map<Hash, BlockAccessList>> fetched = fetchBalWindow(pendingWindow);
              if (fetched.isEmpty()) {
                // No eth/71 peer (or hard failure): wait briefly once, then import without BALs.
                if (stallForRetry > 1) {
                  return waitForEth71Peer()
                      .handle((peer, error) -> null)
                      .thenCompose(
                          ignored ->
                              downloadBalsAndSaveBlocks(
                                  blocks, indexForRetry, collected, stallForRetry - 1));
                }
                LOG.atInfo()
                    .setMessage(
                        "Proceeding without BALs from block {} (no eth/71 peer), reconstructing")
                    .addArgument(windowStartNumber)
                    .log();
                while (index < blocks.size()) {
                  if (!saveOneBlock(blocks.get(index), Optional.empty())) {
                    return CompletableFuture.completedFuture(null);
                  }
                  index++;
                }
                finishBatch(blocks.size());
                return CompletableFuture.completedFuture(null);
              }

              final int before = collected.size();
              collected.putAll(fetched.get());
              final int newlyFetched = collected.size() - before;
              LOG.atInfo()
                  .setMessage("BAL download round: +{} this round, {} cached for batch of {}")
                  .addArgument(newlyFetched)
                  .addArgument(collected::size)
                  .addArgument(blocks::size)
                  .log();

              final int importedAfterFetch = importReadyPrefix(blocks, index, collected);
              if (importedAfterFetch < 0) {
                return CompletableFuture.completedFuture(null);
              }
              index += importedAfterFetch;

              if (index >= blocks.size()) {
                finishBatch(blocks.size());
                return CompletableFuture.completedFuture(null);
              }

              if (importedAfterFetch > 0 || newlyFetched > 0) {
                stallLeft = balStallAttempts();
              } else {
                stallLeft--;
              }

              if (stallLeft <= 0) {
                // Next block still missing its BAL after stalled rounds — import without it.
                final Block stalledBlock = blocks.get(index);
                LOG.atDebug()
                    .setMessage(
                        "Importing block {} without BAL after stalled download, reconstructing")
                    .addArgument(stalledBlock::toLogString)
                    .log();
                if (!saveOneBlock(stalledBlock, Optional.empty())) {
                  return CompletableFuture.completedFuture(null);
                }
                index++;
                stallLeft = balStallAttempts();
              }

              if (index >= blocks.size()) {
                finishBatch(blocks.size());
                return CompletableFuture.completedFuture(null);
              }
              return downloadBalsAndSaveBlocks(blocks, index, collected, stallLeft);
            });
  }

  private List<BlockHeader> nextBalWindow(
      final List<Block> blocks,
      final int startIndex,
      final Map<Hash, BlockAccessList> collected) {
    final List<BlockHeader> window = new ArrayList<>(balRequestWindow());
    for (int i = startIndex; i < blocks.size() && window.size() < balRequestWindow(); i++) {
      final BlockHeader header = blocks.get(i).getHeader();
      if (header.getBalHash().isPresent() && !collected.containsKey(header.getHash())) {
        window.add(header);
      }
    }
    return window;
  }

  private Optional<Map<Hash, BlockAccessList>> fetchBalWindow(
      final List<BlockHeader> pendingWindow) {
    try {
      final GetBlockAccessListsFromPeerTask task =
          new GetBlockAccessListsFromPeerTask(pendingWindow);
      final PeerTaskExecutorResult<List<Optional<BlockAccessList>>> taskResult =
          context.getEthContext().getPeerTaskExecutor().execute(task);
      if (taskResult.responseCode() == PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE
          || taskResult.responseCode() != PeerTaskExecutorResponseCode.SUCCESS
          || taskResult.result().isEmpty()) {
        LOG.atInfo()
            .setMessage("BAL window download unsuccessful ({})")
            .addArgument(taskResult::responseCode)
            .log();
        return Optional.empty();
      }
      final Map<Hash, BlockAccessList> fetched = new HashMap<>();
      mergeAvailableBlockAccessLists(pendingWindow, taskResult.result().get(), fetched);
      return Optional.of(fetched);
    } catch (final RuntimeException e) {
      LOG.atInfo()
          .setMessage("BAL window download failed ({})")
          .addArgument(e::toString)
          .log();
      return Optional.empty();
    }
  }

  private int importReadyPrefix(
      final List<Block> blocks, final int startIndex, final Map<Hash, BlockAccessList> collected) {
    int imported = 0;
    for (int i = startIndex; i < blocks.size(); i++) {
      final Block block = blocks.get(i);
      final boolean needsBal = block.getHeader().getBalHash().isPresent();
      final Optional<BlockAccessList> bal =
          Optional.ofNullable(collected.get(block.getHash()));
      if (needsBal && bal.isEmpty()) {
        break;
      }
      if (!saveOneBlock(block, bal)) {
        return -1;
      }
      imported++;
    }
    return imported;
  }

  private boolean saveOneBlock(final Block block, final Optional<BlockAccessList> bal) {
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

  private void finishBatch(final int importedCount) {
    if (importedCount == context.getBatchSize()) {
      context.resetBatchSize();
    }
  }

  private CompletableFuture<?> waitForEth71Peer() {
    return context
        .getEthContext()
        .getEthPeers()
        .waitForPeer(
            attrs ->
                attrs.ethPeer().getAgreedCapabilities().stream()
                    .anyMatch(EthProtocol::isEth71Compatible))
        .orTimeout(balPeerWaitTimeout().toMillis(), TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  protected int balRequestWindow() {
    return DEFAULT_BAL_REQUEST_WINDOW;
  }

  @VisibleForTesting
  protected int balStallAttempts() {
    return DEFAULT_BAL_STALL_ATTEMPTS;
  }

  @VisibleForTesting
  protected Duration balPeerWaitTimeout() {
    return DEFAULT_BAL_PEER_WAIT_TIMEOUT;
  }

  /**
   * Best-effort download of BALs for the given blocks, used by unit tests and callers that only
   * need the map. Prefer {@link #downloadBalsAndSaveBlocks(List)} on the sync hot path.
   */
  @VisibleForTesting
  protected CompletableFuture<Map<Hash, BlockAccessList>> requestBlockAccessLists(
      final List<Block> blocks) {
    final List<BlockHeader> balHeaders =
        blocks.stream()
            .map(Block::getHeader)
            .filter(header -> header.getBalHash().isPresent())
            .toList();
    if (balHeaders.isEmpty()) {
      return CompletableFuture.completedFuture(Map.of());
    }
    return context
        .getEthContext()
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              final Map<Hash, BlockAccessList> collected = new HashMap<>();
              int stallLeft = balStallAttempts();
              List<BlockHeader> pending = balHeaders;
              while (!pending.isEmpty() && stallLeft > 0) {
                final List<BlockHeader> window =
                    pending.subList(0, Math.min(balRequestWindow(), pending.size()));
                final Optional<Map<Hash, BlockAccessList>> fetched = fetchBalWindow(window);
                if (fetched.isEmpty()) {
                  stallLeft--;
                  if (stallLeft > 0) {
                    try {
                      waitForEth71Peer().handle((p, e) -> null).get();
                    } catch (final Exception e) {
                      Thread.currentThread().interrupt();
                      break;
                    }
                  }
                  continue;
                }
                final int before = collected.size();
                collected.putAll(fetched.get());
                pending =
                    balHeaders.stream()
                        .filter(header -> !collected.containsKey(header.getHash()))
                        .toList();
                if (collected.size() > before) {
                  stallLeft = balStallAttempts();
                } else {
                  stallLeft--;
                }
              }
              return CompletableFuture.completedFuture(collected);
            });
  }

  private static void mergeAvailableBlockAccessLists(
      final List<BlockHeader> requestedHeaders,
      final List<Optional<BlockAccessList>> downloaded,
      final Map<Hash, BlockAccessList> collected) {
    final int count = Math.min(requestedHeaders.size(), downloaded.size());
    for (int i = 0; i < count; i++) {
      final Optional<BlockAccessList> maybeBal = downloaded.get(i);
      if (maybeBal.isPresent()) {
        collected.put(requestedHeaders.get(i).getHash(), maybeBal.get());
      }
    }
  }

  @VisibleForTesting
  protected Void saveBlocks(
      final Map.Entry<List<Block>, Map<Hash, BlockAccessList>> blocksAndAccessLists) {
    final List<Block> blocks = blocksAndAccessLists.getKey();
    final Map<Hash, BlockAccessList> blockAccessLists = blocksAndAccessLists.getValue();
    if (blocks.isEmpty()) {
      context.halveBatchSize();
      LOG.debug("No blocks to save, reducing batch size to {}", context.getBatchSize());
      return null;
    }

    for (final Block block : blocks) {
      if (!saveOneBlock(
          block, Optional.ofNullable(blockAccessLists.get(block.getHash())))) {
        return null;
      }
    }
    finishBatch(blocks.size());
    return null;
  }
}
