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
import java.util.Collections;
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
  private static final int DEFAULT_BAL_DOWNLOAD_MAX_ATTEMPTS = 40;
  private static final Duration DEFAULT_BAL_PEER_WAIT_TIMEOUT = Duration.ofSeconds(5);

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
          .thenCompose(
              blocks ->
                  requestBlockAccessLists(blocks)
                      .thenApply(blockAccessLists -> Map.entry(blocks, blockAccessLists)))
          .thenApply(this::saveBlocks)
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
   * Best-effort download of BALs for headers that advertise a BAL hash. Peers soft-limit responses
   * by message size, so this retries for still-missing headers until complete, attempts are
   * exhausted, or no eth/71 peer is available. Remaining gaps fall back to reconstruction at
   * import.
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
      return CompletableFuture.completedFuture(Collections.emptyMap());
    }

    LOG.atInfo()
        .setMessage("Requesting {} block access list(s) for backward sync batch")
        .addArgument(balHeaders::size)
        .log();

    return downloadBlockAccessLists(
        balHeaders, new HashMap<>(), balHeaders, balDownloadMaxAttempts());
  }

  private CompletableFuture<Map<Hash, BlockAccessList>> downloadBlockAccessLists(
      final List<BlockHeader> allBalHeaders,
      final Map<Hash, BlockAccessList> collected,
      final List<BlockHeader> pendingHeaders,
      final int attemptsRemaining) {
    return context
        .getEthContext()
        .getScheduler()
        .scheduleServiceTask(
            () -> {
              try {
                LOG.atInfo()
                    .setMessage(
                        "Requesting {} remaining BAL(s) ({}/{} already fetched, {} attempts left)")
                    .addArgument(pendingHeaders::size)
                    .addArgument(collected::size)
                    .addArgument(allBalHeaders::size)
                    .addArgument(attemptsRemaining)
                    .log();
                final GetBlockAccessListsFromPeerTask task =
                    new GetBlockAccessListsFromPeerTask(pendingHeaders);
                final PeerTaskExecutorResult<List<Optional<BlockAccessList>>> taskResult =
                    context.getEthContext().getPeerTaskExecutor().execute(task);
                if (taskResult.responseCode() == PeerTaskExecutorResponseCode.SUCCESS
                    && taskResult.result().isPresent()) {
                  final int before = collected.size();
                  mergeAvailableBlockAccessLists(
                      pendingHeaders, taskResult.result().get(), collected);
                  final int newlyFetched = collected.size() - before;
                  final List<BlockHeader> stillPending =
                      allBalHeaders.stream()
                          .filter(header -> !collected.containsKey(header.getHash()))
                          .toList();
                  LOG.atInfo()
                      .setMessage(
                          "BAL download round: +{} this round, {}/{} total ({} still pending)")
                      .addArgument(newlyFetched)
                      .addArgument(collected::size)
                      .addArgument(allBalHeaders::size)
                      .addArgument(stillPending::size)
                      .log();
                  if (stillPending.isEmpty()) {
                    return CompletableFuture.completedFuture(collected);
                  }
                  final boolean madeProgress = newlyFetched > 0;
                  if (madeProgress) {
                    // Soft-limited partial response: keep fetching remaining headers.
                    return downloadBlockAccessLists(
                        allBalHeaders, collected, stillPending, balDownloadMaxAttempts());
                  }
                  if (attemptsRemaining > 1) {
                    return downloadBlockAccessLists(
                        allBalHeaders, collected, stillPending, attemptsRemaining - 1);
                  }
                  return CompletableFuture.completedFuture(collected);
                }
                if (taskResult.responseCode() == PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE
                    && attemptsRemaining > 1) {
                  LOG.atInfo()
                      .setMessage(
                          "No eth/71 peer available for BAL download, waiting up to {}s ({} attempts left, {}/{} fetched)")
                      .addArgument(() -> balPeerWaitTimeout().toSeconds())
                      .addArgument(attemptsRemaining - 1)
                      .addArgument(collected::size)
                      .addArgument(allBalHeaders::size)
                      .log();
                  return waitForEth71Peer()
                      .handle((peer, error) -> null)
                      .thenCompose(
                          ignored ->
                              downloadBlockAccessLists(
                                  allBalHeaders,
                                  collected,
                                  pendingHeaders,
                                  attemptsRemaining - 1));
                }
                LOG.atInfo()
                    .setMessage(
                        "Block access list download unsuccessful ({}), continuing with {}/{} BALs")
                    .addArgument(taskResult::responseCode)
                    .addArgument(collected::size)
                    .addArgument(allBalHeaders::size)
                    .log();
                return CompletableFuture.completedFuture(collected);
              } catch (final RuntimeException e) {
                LOG.atInfo()
                    .setMessage(
                        "Block access list download failed ({}), continuing with {}/{} BALs")
                    .addArgument(e::toString)
                    .addArgument(collected::size)
                    .addArgument(allBalHeaders::size)
                    .log();
                return CompletableFuture.completedFuture(collected);
              }
            });
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
  protected int balDownloadMaxAttempts() {
    return DEFAULT_BAL_DOWNLOAD_MAX_ATTEMPTS;
  }

  @VisibleForTesting
  protected Duration balPeerWaitTimeout() {
    return DEFAULT_BAL_PEER_WAIT_TIMEOUT;
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

    for (Block block : blocks) {
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
        return null;
      } else {
        context.saveBlock(block, Optional.ofNullable(blockAccessLists.get(block.getHash())));
      }
    }

    if (blocks.size() == context.getBatchSize()) {
      // reset the batch size only if we got a full batch
      context.resetBatchSize();
    }
    return null;
  }
}
