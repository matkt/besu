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
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.services.pipeline.Pipeline;
import org.hyperledger.besu.services.pipeline.PipelineBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads block bodies and receipts for stored headers and imports them in order without
 * executing them; the chain head follows the last imported block.
 */
public final class BlockImporter {

  private static final Logger LOG = LoggerFactory.getLogger(BlockImporter.class);
  private static final int ATTEMPTS = 10;

  private final RequestSender sender;
  private final ProtocolSchedule protocolSchedule;
  private final MutableBlockchain blockchain;
  private final int blocksPerRequest;
  private final int parallelism;
  private final boolean indexTransactions;
  private final SyncTransactionReceiptEncoder receiptEncoder =
      new SyncTransactionReceiptEncoder(new SimpleNoCopyRlpEncoder());
  private final LabelledMetric<Counter> pipelineCounter;
  private volatile long next;
  private volatile boolean closed;
  private final AtomicInteger generation = new AtomicInteger();
  private volatile Pipeline<List<BlockHeader>> pipeline;

  /**
   * Creates an importer.
   *
   * @param first the first block to import; its parent must be stored with its total difficulty
   */
  public BlockImporter(
      final RequestSender sender,
      final ProtocolSchedule protocolSchedule,
      final MutableBlockchain blockchain,
      final long first,
      final int blocksPerRequest,
      final int parallelism,
      final boolean indexTransactions,
      final MetricsSystem metricsSystem) {
    this.sender = sender;
    this.protocolSchedule = protocolSchedule;
    this.blockchain = blockchain;
    this.next = first;
    this.blocksPerRequest = blocksPerRequest;
    this.parallelism = parallelism;
    this.indexTransactions = indexTransactions;
    this.pipelineCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "snap2_block_pipeline_processed_total",
            "Block batches processed by the SNAP2 sync",
            "step",
            "action");
  }

  /** Returns the next block to import. */
  public long nextBlock() {
    return next;
  }

  /**
   * Imports again from the given block, after a reorg replaced the blocks above its parent. A
   * running import is aborted, as it may have read replaced headers.
   */
  public synchronized void rewindTo(final long block) {
    stop();
    next = Math.min(next, block);
  }

  /** Imports every block up to {@code target}, whose headers must be stored. */
  public synchronized CompletableFuture<Void> importUpTo(
      final long target, final ExecutorService executor) {
    if (closed) {
      return CompletableFuture.failedFuture(new CancellationException("importer closed"));
    }
    if (next > target) {
      return CompletableFuture.completedFuture(null);
    }
    LOG.info("Importing block bodies and receipts {} to {}", next, target);
    final int started = generation.get();
    pipeline =
        PipelineBuilder.createPipelineFrom(
                "headerBatches",
                new HeaderBatches(next, target),
                parallelism,
                pipelineCounter,
                false,
                "snap2-blocks")
            .thenProcessAsyncOrdered("downloadBodies", this::bodies, parallelism)
            .thenProcessAsyncOrdered("downloadReceipts", this::receipts, parallelism)
            .andFinishWith("importBlocks", batch -> importBatch(batch, started));
    return pipeline.start(executor);
  }

  /** Stops importing for good. */
  public synchronized void close() {
    closed = true;
    stop();
  }

  /** Stops importing; a batch in flight is not imported. */
  public synchronized void stop() {
    generation.incrementAndGet();
    final Pipeline<List<BlockHeader>> running = pipeline;
    if (running != null) {
      try {
        running.abort();
      } catch (final IllegalStateException notStarted) {
        // nothing to abort
      }
    }
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
                "bodies"),
            ATTEMPTS)
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
                "receipts"),
            ATTEMPTS)
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

  private synchronized void importBatch(
      final List<SyncBlockWithReceipts> batch, final int startedGeneration) {
    if (startedGeneration != generation.get()) {
      throw new IllegalStateException("import rewound by a reorg");
    }
    final SyncBlockWithReceipts first = batch.getFirst();
    if (first.getNumber() != next) {
      throw new IllegalStateException(
          "expected block " + next + " but the batch starts at " + first.getNumber());
    }
    blockchain.unsafeImportSyncBodiesAndReceipts(batch, indexTransactions);
    next = batch.getLast().getNumber() + 1;
  }

  // batches of stored headers, read just before they are needed
  private final class HeaderBatches implements Iterator<List<BlockHeader>> {
    private final long target;
    private long cursor;

    private HeaderBatches(final long from, final long target) {
      this.cursor = from;
      this.target = target;
    }

    @Override
    public boolean hasNext() {
      return cursor <= target;
    }

    @Override
    public List<BlockHeader> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final long end = Math.min(target, cursor + blocksPerRequest - 1);
      final List<BlockHeader> headers = new ArrayList<>((int) (end - cursor + 1));
      for (long number = cursor; number <= end; number++) {
        final long n = number;
        headers.add(
            blockchain
                .getBlockHeader(number)
                .orElseThrow(() -> new IllegalStateException("missing header of block " + n)));
      }
      cursor = end + 1;
      return headers;
    }
  }
}
