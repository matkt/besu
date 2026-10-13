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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.core.SyncBlockWithReceipts;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.SyncTransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncBlockBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncReceiptsFromPeerTask;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.PeerTaskRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.StorageRetry;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads the bodies and receipts of stored headers, many ranges at a time and highest first, and
 * stores each range as soon as it arrives; the chain head is set once every block is stored, see
 * {@link MutableBlockchain#unsafeSetSyncChainHead}. A range asks about one full body response, and
 * enough ranges run to keep every eth slot of the peers busy, within a memory budget.
 */
public final class BodyDownloader {

  private static final Logger LOG = LoggerFactory.getLogger(BodyDownloader.class);
  // peers cap a body response around 2 MiB
  private static final double RANGE_BODY_BYTES = 2 * 1024 * 1024;
  private static final int MIN_RANGE_BLOCKS = 8;
  private static final double INITIAL_BLOCK_BYTES = 64 * 1024;
  private static final long MAX_MEMORY = 1L << 30;

  private final RequestSender sender;
  private final ProtocolSchedule protocolSchedule;
  private final MutableBlockchain blockchain;
  private final long floor;
  private final int maxRangeBlocks;
  private final int maxRanges;
  private final long memory = Math.min(MAX_MEMORY, Runtime.getRuntime().maxMemory() / 16);
  private final boolean indexTransactions;
  private final Executor writer;
  private final SyncTransactionReceiptEncoder receiptEncoder =
      new SyncTransactionReceiptEncoder(new SimpleNoCopyRlpEncoder());

  // guarded by this
  private final BlockRanges stored = new BlockRanges();
  private final BlockRanges fetching = new BlockRanges();
  private long lowest;
  private long highest = -1;
  private int running;
  private boolean closed;
  // averages of the stored blocks, without and with their receipts
  private double bodyBytes = INITIAL_BLOCK_BYTES;
  private double blockBytes = INITIAL_BLOCK_BYTES;

  /**
   * Creates a downloader.
   *
   * @param floor the highest block whose body is not needed, its total difficulty stored
   * @param maxRangeBlocks most blocks per range
   * @param maxRanges most ranges downloaded at once
   * @param writer runs the reads of headers and the writes of bodies
   */
  public BodyDownloader(
      final RequestSender sender,
      final ProtocolSchedule protocolSchedule,
      final MutableBlockchain blockchain,
      final long floor,
      final int maxRangeBlocks,
      final int maxRanges,
      final boolean indexTransactions,
      final Executor writer) {
    this.sender = sender;
    this.protocolSchedule = protocolSchedule;
    this.blockchain = blockchain;
    this.floor = floor;
    this.maxRangeBlocks = maxRangeBlocks;
    this.maxRanges = maxRanges;
    this.indexTransactions = indexTransactions;
    this.writer = writer;
  }

  /** Downloads what is missing from {@code from} to {@code to}, whose headers are canonical. */
  public synchronized void download(final long from, final long to) {
    lowest = Math.max(from, floor + 1);
    highest = to;
    dispatch();
  }

  /** Returns whether every block above the floor up to {@code target} is stored. */
  public synchronized boolean storedUpTo(final long target) {
    return target <= floor || stored.covers(floor + 1, target);
  }

  /** Returns the number of blocks stored. */
  public synchronized long storedBlocks() {
    return stored.size();
  }

  /** Forgets the blocks from {@code from} on, which a reorg replaced. */
  public synchronized void rewind(final long from) {
    stored.remove(from, Long.MAX_VALUE);
  }

  /** Stops downloading; a range in flight may still be stored. */
  public synchronized void close() {
    closed = true;
  }

  private void dispatch() {
    final int rangeBlocks = rangeBlocks();
    final int ranges = ranges(rangeBlocks);
    while (!closed && running < ranges) {
      final long[] range = BlockRanges.highestGap(stored, fetching, lowest, highest, rangeBlocks);
      if (range == null) {
        return;
      }
      fetching.add(range[0], range[1]);
      running++;
      fetch(range[0], range[1]);
    }
  }

  // about one full body response, so many blocks when they are small
  private int rangeBlocks() {
    final double blocks = RANGE_BODY_BYTES / Math.max(1, bodyBytes);
    return (int)
        Math.max(Math.min(MIN_RANGE_BLOCKS, maxRangeBlocks), Math.min(maxRangeBlocks, blocks));
  }

  // enough ranges to keep every eth slot of the peers busy, within the memory budget
  private int ranges(final int rangeBlocks) {
    final int slots = sender.capacity(Protocol.ETH);
    final double byMemory = memory / (rangeBlocks * Math.max(1, blockBytes));
    return (int) Math.max(1, Math.min(maxRanges, Math.min(slots + slots / 4, byMemory)));
  }

  private void measure(final List<SyncBlockWithReceipts> blocks) {
    long bodies = 0;
    long receipts = 0;
    for (final SyncBlockWithReceipts block : blocks) {
      bodies += block.getBlock().getBody().getRlp().size();
      for (final SyncTransactionReceipt receipt : block.getReceipts()) {
        receipts += receipt.getRlpBytes() == null ? 0 : receipt.getRlpBytes().size();
      }
    }
    bodyBytes = 0.8 * bodyBytes + 0.2 * bodies / blocks.size();
    blockBytes = 0.8 * blockBytes + 0.2 * (bodies + receipts) / blocks.size();
  }

  private void fetch(final long from, final long to) {
    CompletableFuture.supplyAsync(() -> headers(from, to), writer)
        .thenCompose(this::bodies)
        .thenCompose(this::receipts)
        .thenApplyAsync(
            blocks -> {
              StorageRetry.run(
                  () -> blockchain.unsafeStoreSyncBodiesAndReceipts(blocks, indexTransactions));
              return blocks;
            },
            writer)
        .whenComplete((blocks, error) -> finished(from, to, blocks, error));
  }

  private void finished(
      final long from,
      final long to,
      final List<SyncBlockWithReceipts> blocks,
      final Throwable error) {
    if (error == null) {
      release(from, to, blocks);
      return;
    }
    LOG.debug("Bodies of blocks {} to {} failed, retrying: {}", from, to, error.getMessage());
    // a failed range waits a little before being asked again
    CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS, writer)
        .execute(() -> release(from, to, null));
  }

  // a range stored for headers a reorg replaced since is downloaded again
  private synchronized void release(
      final long from, final long to, final List<SyncBlockWithReceipts> blocks) {
    fetching.remove(from, to);
    running--;
    if (blocks != null) {
      measure(blocks);
    }
    if (blocks != null && canonical(blocks)) {
      stored.add(from, to);
    }
    dispatch();
  }

  // linked end to end, so not read across a reorg, and still on the canonical chain
  private boolean canonical(final List<SyncBlockWithReceipts> blocks) {
    for (int i = 1; i < blocks.size(); i++) {
      if (!blocks.get(i).getHeader().getParentHash().equals(blocks.get(i - 1).getHash())) {
        return false;
      }
    }
    return canonical(blocks.getFirst().getHeader()) && canonical(blocks.getLast().getHeader());
  }

  private boolean canonical(final BlockHeader header) {
    return blockchain
        .getBlockHashByNumber(header.getNumber())
        .map(header.getHash()::equals)
        .orElse(false);
  }

  private List<BlockHeader> headers(final long from, final long to) {
    final List<BlockHeader> headers = new ArrayList<>((int) (to - from + 1));
    for (long number = from; number <= to; number++) {
      final long n = number;
      headers.add(
          blockchain
              .getBlockHeader(number)
              .orElseThrow(() -> new IllegalStateException("missing header of block " + n)));
    }
    return headers;
  }

  private CompletableFuture<List<SyncBlock>> bodies(final List<BlockHeader> headers) {
    return bodiesFrom(headers, new ArrayList<>(headers.size()));
  }

  private CompletableFuture<List<SyncBlock>> bodiesFrom(
      final List<BlockHeader> headers, final List<SyncBlock> collected) {
    final List<BlockHeader> remaining = headers.subList(collected.size(), headers.size());
    if (remaining.isEmpty()) {
      return CompletableFuture.completedFuture(collected);
    }
    final List<BlockHeader> request = List.copyOf(remaining);
    return sender
        .submitWithRetries(
            new PeerTaskRequest<>(
                () -> new GetSyncBlockBodiesFromPeerTask(request, protocolSchedule, 0),
                Priority.CHAIN,
                "bodies",
                request.getLast().getNumber()))
        .thenCompose(
            response -> {
              collected.addAll(response.value());
              return bodiesFrom(headers, collected);
            });
  }

  private CompletableFuture<List<SyncBlockWithReceipts>> receipts(final List<SyncBlock> blocks) {
    final Map<Hash, List<SyncTransactionReceipt>> receipts = new HashMap<>();
    final List<SyncBlock> needed = new ArrayList<>();
    for (final SyncBlock block : blocks) {
      if (block.getHeader().getReceiptsRoot().equals(Hash.EMPTY_TRIE_HASH)) {
        receipts.put(block.getHash(), List.of());
      } else {
        needed.add(block);
      }
    }
    return receiptsFrom(needed, List.of(), receipts)
        .thenApply(
            unused ->
                blocks.stream()
                    .map(block -> new SyncBlockWithReceipts(block, receipts.get(block.getHash())))
                    .toList());
  }

  private CompletableFuture<Void> receiptsFrom(
      final List<SyncBlock> remaining,
      final List<SyncTransactionReceipt> partial,
      final Map<Hash, List<SyncTransactionReceipt>> receipts) {
    if (remaining.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    final GetSyncReceiptsFromPeerTask.Request request =
        new GetSyncReceiptsFromPeerTask.Request(List.copyOf(remaining), partial);
    return sender
        .submitWithRetries(
            new PeerTaskRequest<>(
                () -> new GetSyncReceiptsFromPeerTask(request, protocolSchedule, receiptEncoder),
                Priority.CHAIN,
                "receipts",
                remaining.getLast().getHeader().getNumber()))
        .thenCompose(
            response -> {
              final GetSyncReceiptsFromPeerTask.Response value = response.value();
              value
                  .completeReceiptsByBlock()
                  .forEach((block, blockReceipts) -> receipts.put(block.getHash(), blockReceipts));
              final List<SyncBlock> left =
                  remaining.stream()
                      .filter(block -> !receipts.containsKey(block.getHash()))
                      .toList();
              return receiptsFrom(left, value.lastBlockPartialReceipts(), receipts);
            });
  }
}
