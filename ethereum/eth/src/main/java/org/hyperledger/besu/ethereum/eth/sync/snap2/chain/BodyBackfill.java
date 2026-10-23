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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import static com.google.common.base.Preconditions.checkArgument;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.block;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.compact;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.duration;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.eta;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.mibPerSecond;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.ProgressFormat.percent;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.core.SyncBlockWithReceipts;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.SyncTransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncBlockBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncReceiptsFromPeerTask;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.BlockRanges.Gap;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.PeerTaskRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Priority;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.LabelledSuppliedMetric;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongUnaryOperator;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads the bodies and receipts below a stored block, highest first, and records the lowest
 * block stored with every one above it. The sync runs it on the window below its pivot; after the
 * sync it runs in the background, paused while the node is behind the chain head.
 */
public final class BodyBackfill {

  private static final Logger LOG = LoggerFactory.getLogger(BodyBackfill.class);
  private static final Duration LOG_INTERVAL = Duration.ofMinutes(1);
  private static final Duration POLL_INTERVAL = Duration.ofSeconds(1);
  private static final Duration RETRY_DELAY = Duration.ofSeconds(5);
  private static final long RECORD_BYTES = 64L * 1024 * 1024;
  // peers cap a body or receipt response around 2 MiB
  private static final double RESPONSE_BYTES = 2 * 1024 * 1024;
  private static final int MIN_RANGE_BLOCKS = 8;
  private static final int MAX_RANGE_BLOCKS = 128;
  private static final double INITIAL_BLOCK_BYTES = 64 * 1024;

  /** What the back-fill is doing. */
  private enum State {
    RUNNING,
    PAUSED,
    DONE,
    STOPPED
  }

  /**
   * Tuning of the back-fill.
   *
   * @param label what the settings are for, in the logs
   * @param requests ranges downloaded at once, with one request in flight each
   * @param recordBytes bytes stored between two records of the lowest block
   * @param pollInterval how long a paused back-fill, or one waiting for its last ranges, waits
   * @param retryDelay how long a range whose download failed waits before it is asked again
   * @param maxRangeBlocks most blocks per request
   * @param indexTransactions whether the transactions of the stored blocks are indexed
   * @param background whether it runs after the sync, logging its progress and with metrics
   */
  public record Settings(
      String label,
      int requests,
      long recordBytes,
      Duration pollInterval,
      Duration retryDelay,
      int maxRangeBlocks,
      boolean indexTransactions,
      boolean background) {

    /** Returns the settings of the back-fill that runs once the node follows the chain head. */
    public static Settings background(final boolean indexTransactions) {
      return new Settings(
          "back-fill",
          8,
          RECORD_BYTES,
          POLL_INTERVAL,
          RETRY_DELAY,
          MAX_RANGE_BLOCKS,
          indexTransactions,
          true);
    }

    /** Returns the settings of the window the sync downloads before it sets the chain head. */
    public static Settings window(final boolean indexTransactions) {
      return new Settings(
          "window",
          4,
          RECORD_BYTES,
          POLL_INTERVAL,
          RETRY_DELAY,
          MAX_RANGE_BLOCKS,
          indexTransactions,
          false);
    }
  }

  private final MutableBlockchain blockchain;
  private final ProtocolSchedule protocolSchedule;
  // the SNAP2 scheduler rather than the PeerTaskExecutor, for the reasons RequestScheduler gives
  private final RequestSender sender;
  private final BooleanSupplier inSync;
  // the lowest block whose body is needed, given the chain head
  private final LongUnaryOperator lowestNeeded;
  private final Settings settings;
  private final SyncTransactionReceiptEncoder receiptEncoder =
      new SyncTransactionReceiptEncoder(new SimpleNoCopyRlpEncoder());
  private final CompletableFuture<Void> done = new CompletableFuture<>();
  private final AtomicLong blocksStored = new AtomicLong();
  private final AtomicLong bytesStored = new AtomicLong();
  private final AtomicLong lastLogNanos = new AtomicLong();
  // the lowest block at the start, which the share done counts from
  private final long initialLowest;
  private final Object sizes = new Object();

  // guarded by this
  private final BackfillRanges ranges;
  // the lowest block stored with every block above it, as last recorded
  private long persisted;
  private long bytesSinceRecord;
  // guarded by sizes: the averages of the downloaded blocks, of their bodies and their receipts
  private double bodyBytes = INITIAL_BLOCK_BYTES;
  private double receiptBytes;

  // set under the lock
  private volatile ExecutorService workers;
  private volatile boolean stopped;
  private volatile State state = State.PAUSED;
  private volatile Instant started = Instant.now();
  private volatile long loggedBytes;

  /**
   * Creates the back-fill.
   *
   * @param sender the SNAP2 scheduler, which must run until the back-fill ends
   * @param inSync whether the node follows the chain head, the back-fill pausing otherwise
   * @param lowest the lowest block whose body and receipts are stored already, the download
   *     starting below it
   * @param lowestNeeded the lowest block whose body is needed for a chain head, above the floor of
   *     the sync and, on a pruned chain, {@link
   *     org.hyperledger.besu.ethereum.chain.ChainDataPruner#lowestToBackfill}
   */
  public BodyBackfill(
      final MutableBlockchain blockchain,
      final ProtocolSchedule protocolSchedule,
      final RequestSender sender,
      final BooleanSupplier inSync,
      final long lowest,
      final LongUnaryOperator lowestNeeded,
      final Settings settings,
      final MetricsSystem metricsSystem) {
    checkArgument(settings.requests() > 0, "requests must be positive");
    checkArgument(settings.maxRangeBlocks() > 0, "maxRangeBlocks must be positive");
    this.blockchain = blockchain;
    this.protocolSchedule = protocolSchedule;
    this.sender = sender;
    this.inSync = inSync;
    this.ranges = new BackfillRanges(lowest);
    this.initialLowest = lowest;
    this.persisted = lowest;
    this.lowestNeeded = lowestNeeded;
    this.settings = settings;
    // the window shows in the progress of the sync
    if (settings.background()) {
      registerMetrics(metricsSystem);
    }
  }

  /**
   * Starts downloading below the lowest stored block, unless every needed body is stored.
   *
   * @param workers runs one range at a time on each of its threads, shut down once the back-fill
   *     ends
   */
  public void start(final ExecutorService workers) {
    // under the lock, so that a stop comes first or finds the workers started
    synchronized (this) {
      this.workers = workers;
      if (stopped) {
        workers.shutdown();
        return;
      }
      if (!reachedFloor()) {
        launch(workers);
        return;
      }
    }
    LOG.info(
        "SNAP2 {}: every block from {} has its body and receipts",
        settings.label(),
        block(lowestStored()));
    finish();
  }

  private void launch(final ExecutorService workers) {
    final long floor = floor();
    started = Instant.now();
    LOG.info(
        "SNAP2 {} started | {} blocks, {} down to {}",
        settings.label(),
        compact(lowestStored() - floor),
        block(lowestStored() - 1),
        block(floor));
    lastLogNanos.set(System.nanoTime());
    for (int i = 0; i < settings.requests(); i++) {
      workers.execute(this::run);
    }
  }

  /**
   * Stops the back-fill, recording what it stored; nothing is written afterwards. The workers are
   * interrupted, not awaited: the caller awaits them before it closes the storage they read.
   */
  public void stop() {
    end(new CancellationException("body download stopped"));
    final ExecutorService running = workers;
    // interrupts the workers that wait on a request or a delay
    if (running != null) {
      running.shutdownNow();
    }
  }

  /**
   * Completes once every needed body is stored and recorded; fails once stopped, on a missing
   * header, or when what was stored cannot be recorded.
   */
  public CompletableFuture<Void> done() {
    return done;
  }

  /**
   * Returns the lowest block whose body and receipts are stored, with those of every block above.
   */
  public synchronized long lowestStored() {
    return ranges.lowest();
  }

  /** Returns the number of blocks whose body and receipts are still to store. */
  public long blocksRemaining() {
    return Math.max(0, lowestStored() - floor());
  }

  /** Returns the number of blocks whose body and receipts were stored. */
  public long blocksStored() {
    return blocksStored.get();
  }

  private void run() {
    try {
      while (!stopped) {
        try {
          step();
        } catch (final RuntimeException e) {
          LOG.debug("Body back-fill step failed, retrying: {}", e.getMessage());
          Thread.sleep(settings.retryDelay().toMillis());
        }
      }
    } catch (final MissingHeaderException e) {
      // the sync stores every header above its floor, asking again would not bring it
      LOG.warn("SNAP2 {} stopped: {}", settings.label(), e.getMessage());
      end(e);
    } catch (final InterruptedException e) {
      // by a stop, or by the owner of the workers
      end(new CancellationException("body download interrupted"));
      Thread.currentThread().interrupt();
    }
  }

  private void step() throws InterruptedException, MissingHeaderException {
    if (settings.background()) {
      maybeLog();
    }
    if (!inSync.getAsBoolean()) {
      pause();
      Thread.sleep(settings.pollInterval().toMillis());
      return;
    }
    if (reachedFloor()) {
      finish();
      return;
    }
    final Optional<Gap> claimed = claimRange();
    if (claimed.isEmpty()) {
      // the last ranges are in flight
      Thread.sleep(settings.pollInterval().toMillis());
      return;
    }
    final Gap range = claimed.get();
    try {
      final List<SyncBlockWithReceipts> blocks = receipts(bodies(headers(range)));
      recordSizes(blocks);
      store(range, blocks);
    } catch (final RuntimeException e) {
      if (!stopped) {
        LOG.debug(
            "Blocks {} to {} failed, asked again later: {}",
            range.from(),
            range.to(),
            e.getMessage());
        // still claimed while it waits, so that no other worker asks it meanwhile
        Thread.sleep(settings.retryDelay().toMillis());
      }
    } finally {
      release(range);
    }
  }

  private long floor() {
    return lowestNeeded.applyAsLong(blockchain.getChainHeadBlockNumber());
  }

  // the lowest block only moves down over stored blocks
  private synchronized boolean reachedFloor() {
    return ranges.lowest() <= floor();
  }

  private synchronized void pause() {
    if (!stopped) {
      state = State.PAUSED;
    }
  }

  // the highest run of blocks below the lowest stored one that no other worker has claimed
  private synchronized Optional<Gap> claimRange() {
    if (stopped) {
      return Optional.empty();
    }
    final Optional<Gap> range = ranges.claim(floor(), rangeBlocks());
    range.ifPresent(gap -> state = State.RUNNING);
    return range;
  }

  private synchronized void release(final Gap range) {
    ranges.release(range);
  }

  // records the average sizes of the downloaded blocks, which size the next requests
  private void recordSizes(final List<SyncBlockWithReceipts> blocks) {
    final long bodies =
        blocks.stream().mapToLong(block -> block.getBlock().getBody().getRlp().size()).sum();
    final long receipts = bytes(blocks) - bodies;
    synchronized (sizes) {
      bodyBytes = 0.8 * bodyBytes + 0.2 * bodies / blocks.size();
      receiptBytes = 0.8 * receiptBytes + 0.2 * receipts / blocks.size();
    }
  }

  private static long bytes(final List<SyncBlockWithReceipts> blocks) {
    long bytes = 0;
    for (final SyncBlockWithReceipts block : blocks) {
      bytes += block.getBlock().getBody().getRlp().size();
      for (final SyncTransactionReceipt receipt : block.getReceipts()) {
        bytes += receipt.getRlpBytes() == null ? 0 : receipt.getRlpBytes().size();
      }
    }
    return bytes;
  }

  // blocks whose bodies fill about one response, so many when they are small
  private int rangeBlocks() {
    final int max = settings.maxRangeBlocks();
    synchronized (sizes) {
      return (int) Math.clamp(RESPONSE_BYTES / bodyBytes, Math.min(MIN_RANGE_BLOCKS, max), max);
    }
  }

  // blocks whose receipts fill about one response, all of them while their size is unknown
  private int receiptBlocks() {
    final int max = settings.maxRangeBlocks();
    synchronized (sizes) {
      return receiptBytes > 0 ? (int) Math.clamp(RESPONSE_BYTES / receiptBytes, 1, max) : max;
    }
  }

  // drops a range a reorg replaced, and the blocks a pruner would remove
  private synchronized void store(final Gap range, final List<SyncBlockWithReceipts> blocks) {
    if (stopped) {
      return;
    }
    final long floor = floor();
    final List<SyncBlockWithReceipts> kept =
        blocks.stream().filter(block -> block.getNumber() >= floor).toList();
    if (kept.isEmpty() || !canonical(kept)) {
      return;
    }
    blockchain.unsafeStoreSyncBodiesAndReceipts(kept, settings.indexTransactions());
    ranges.store(kept.getFirst().getNumber(), range.to());
    final long bytes = bytes(kept);
    blocksStored.addAndGet(kept.size());
    bytesStored.addAndGet(bytes);
    bytesSinceRecord += bytes;
    if (bytesSinceRecord >= settings.recordBytes()) {
      persist();
    }
  }

  // records the lowest block stored with every block above it, once it moved
  private synchronized void persist() {
    final long lowest = ranges.lowest();
    if (lowest < persisted) {
      blockchain.unsafeSetBodiesFrom(lowest);
      persisted = lowest;
      bytesSinceRecord = 0;
    }
  }

  // every needed body is stored
  private void finish() {
    end(null);
  }

  // ends the back-fill once, recording what it stored; done fails with the error unless null
  private void end(final Exception error) {
    final ExecutorService running;
    Exception failure = error;
    synchronized (this) {
      if (stopped) {
        return;
      }
      stopped = true;
      running = workers;
      // under the lock, so that a stop finds it recorded
      try {
        persist();
      } catch (final RuntimeException e) {
        LOG.debug("Could not record the back-filled blocks: {}", e.getMessage());
        // a back-fill that could not record what it stored did not complete
        failure = failure != null ? failure : e;
      }
      state = failure == null ? State.DONE : State.STOPPED;
    }
    if (running != null) {
      running.shutdown();
    }
    if (failure != null) {
      done.completeExceptionally(failure);
      return;
    }
    if (blocksStored.get() > 0) {
      LOG.info(
          "SNAP2 {} complete in {} | {} blocks, down to {}",
          settings.label(),
          duration(Duration.between(started, Instant.now())),
          compact(blocksStored.get()),
          block(lowestStored()));
    }
    done.complete(null);
  }

  // the headers of the range, which the sync stores above its floor
  private List<BlockHeader> headers(final Gap range) throws MissingHeaderException {
    final List<BlockHeader> headers = new ArrayList<>((int) (range.to() - range.from() + 1));
    for (long number = range.from(); number <= range.to(); number++) {
      final Optional<BlockHeader> header = blockchain.getBlockHeader(number);
      if (header.isEmpty()) {
        throw new MissingHeaderException(number);
      }
      headers.add(header.get());
    }
    return headers;
  }

  // the bodies of the headers, asked again for those a peer left out
  private List<SyncBlock> bodies(final List<BlockHeader> headers) throws InterruptedException {
    final List<SyncBlock> blocks = new ArrayList<>(headers.size());
    while (blocks.size() < headers.size()) {
      final List<BlockHeader> remaining =
          List.copyOf(headers.subList(blocks.size(), headers.size()));
      blocks.addAll(
          send(
              new PeerTaskRequest<>(
                  () -> new GetSyncBlockBodiesFromPeerTask(remaining, protocolSchedule),
                  Priority.BODIES,
                  "bodies",
                  remaining.getLast().getNumber())));
    }
    return blocks;
  }

  private List<SyncBlockWithReceipts> receipts(final List<SyncBlock> blocks)
      throws InterruptedException {
    final Map<Hash, List<SyncTransactionReceipt>> receipts = new HashMap<>();
    final List<SyncBlock> needed = new ArrayList<>();
    for (final SyncBlock block : blocks) {
      if (block.getHeader().getReceiptsRoot().equals(Hash.EMPTY_TRIE_HASH)) {
        receipts.put(block.getHash(), List.of());
      } else {
        needed.add(block);
      }
    }
    // about one response per group, a block cut by the peer completed with the next request
    for (final List<SyncBlock> group : Lists.partition(needed, receiptBlocks())) {
      List<SyncBlock> remaining = group;
      List<SyncTransactionReceipt> partial = List.of();
      while (!remaining.isEmpty()) {
        final GetSyncReceiptsFromPeerTask.Request request =
            new GetSyncReceiptsFromPeerTask.Request(List.copyOf(remaining), partial);
        final GetSyncReceiptsFromPeerTask.Response response =
            send(
                new PeerTaskRequest<>(
                    () ->
                        new GetSyncReceiptsFromPeerTask(request, protocolSchedule, receiptEncoder),
                    Priority.BODIES,
                    "receipts",
                    remaining.getLast().getHeader().getNumber()));
        // asked again at once, an answer that completes nothing would loop on the same peer
        if (response.completeReceiptsByBlock().isEmpty()
            && response.lastBlockPartialReceipts().size() <= partial.size()) {
          throw new IllegalStateException(
              "receipts from block "
                  + remaining.getFirst().getHeader().getNumber()
                  + " completed nothing");
        }
        response
            .completeReceiptsByBlock()
            .forEach((block, blockReceipts) -> receipts.put(block.getHash(), blockReceipts));
        remaining =
            remaining.stream().filter(block -> !receipts.containsKey(block.getHash())).toList();
        partial = response.lastBlockPartialReceipts();
      }
    }
    return blocks.stream()
        .map(block -> new SyncBlockWithReceipts(block, receipts.get(block.getHash())))
        .toList();
  }

  // the answer of the scheduler, which retries the request on other peers
  private <T> T send(final PeerTaskRequest<T> request) throws InterruptedException {
    if (stopped) {
      throw new CancellationException("body download stopped");
    }
    final CompletableFuture<RequestSender.Response<T>> answer = sender.submitWithRetries(request);
    try {
      return answer.get().value();
    } catch (final InterruptedException e) {
      // nobody waits for it anymore, so the scheduler stops retrying it
      answer.cancel(false);
      throw e;
    } catch (final ExecutionException e) {
      throw new CompletionException(e.getCause());
    }
  }

  // linked end to end and still on the canonical chain
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

  private void maybeLog() {
    final long now = System.nanoTime();
    final long last = lastLogNanos.get();
    if (now - last < LOG_INTERVAL.toNanos() || !lastLogNanos.compareAndSet(last, now)) {
      return;
    }
    final double seconds = (now - last) / 1e9;
    final long bytes = bytesStored.get();
    final long downloaded = initialLowest - lowestStored();
    final StringBuilder line =
        new StringBuilder("SNAP2 bodies ").append(percent(downloaded, initialLowest - floor()));
    eta((double) downloaded, (double) blocksRemaining(), Duration.between(started, Instant.now()))
        .ifPresent(left -> line.append(" | ETA ").append(left));
    line.append(" | ")
        .append(settings.label())
        .append(' ')
        .append(mibPerSecond((bytes - loggedBytes) / seconds))
        .append(" | ")
        .append(
            state == State.PAUSED
                ? "paused, node behind the head"
                : state.name().toLowerCase(Locale.ROOT));
    LOG.info("{}", line);
    loggedBytes = bytes;
  }

  private void registerMetrics(final MetricsSystem metricsSystem) {
    final BesuMetricCategory category = BesuMetricCategory.SYNCHRONIZER;
    metricsSystem.createLongGauge(
        category,
        "snap2_backfill_lowest_block",
        "Lowest block whose body and receipts the SNAP2 back-fill stored, with every block above it",
        this::lowestStored);
    metricsSystem.createLongGauge(
        category,
        "snap2_backfill_blocks_remaining",
        "Blocks whose body and receipts the SNAP2 back-fill still has to store",
        this::blocksRemaining);
    metricsSystem.createCounter(
        category,
        "snap2_backfill_bytes_total",
        "Bytes of bodies and receipts the SNAP2 back-fill stored",
        bytesStored::get);
    final LabelledSuppliedMetric states =
        metricsSystem.createLabelledSuppliedGauge(
            category,
            "snap2_backfill_state",
            "State of the SNAP2 back-fill, 1 for the current one",
            "state");
    for (final State value : State.values()) {
      states.labels(() -> state == value ? 1 : 0, value.name().toLowerCase(Locale.ROOT));
    }
  }

  /** A header the sync should have stored above its floor, which no retry brings back. */
  private static final class MissingHeaderException extends Exception {
    private MissingHeaderException(final long number) {
      super("no header stored for block " + number + ", its body cannot be back-filled");
    }
  }
}
