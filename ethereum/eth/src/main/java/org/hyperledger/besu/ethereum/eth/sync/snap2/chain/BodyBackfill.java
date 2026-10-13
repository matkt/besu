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
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.SyncBlock;
import org.hyperledger.besu.ethereum.core.SyncBlockWithReceipts;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.SyncTransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerSelector;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncBlockBodiesFromPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetSyncReceiptsFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.rlp.SimpleNoCopyRlpEncoder;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.LabelledSuppliedMetric;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongUnaryOperator;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads the bodies and receipts of the blocks below a block whose headers are stored, highest
 * first, a few requests at a time each to a different peer, and records the lowest block stored
 * with every one above it once its writes are on disk. The SNAP2 sync runs it unthrottled on the
 * window of blocks below its final pivot; afterwards, and after a restart, it back-fills the older
 * blocks in the background at the rate of its {@link BackfillMode}, paused while a block is
 * imported or the node is behind, and halved while the chain head lags the wall clock.
 */
public final class BodyBackfill implements BlockAddedObserver {

  private static final Logger LOG = LoggerFactory.getLogger(BodyBackfill.class);
  private static final Duration RETRY_DELAY = Duration.ofSeconds(5);
  private static final Duration LOG_INTERVAL = Duration.ofMinutes(1);
  // peers cap a body or receipt response around 2 MiB
  private static final double RESPONSE_BYTES = 2 * 1024 * 1024;
  private static final int MIN_RANGE_BLOCKS = 8;
  private static final int MAX_RANGE_BLOCKS = 128;
  private static final double INITIAL_BLOCK_BYTES = 64 * 1024;
  // a head older than the slot plus the allowance of the mode is lagging; the rate halves at most
  // once a slot and recovers an eighth a slot
  private static final Duration SLOT = Duration.ofSeconds(12);
  private static final double MIN_SHARE = 1.0 / 8;
  private static final double SHARE_STEP = 1.0 / 8;

  /** What the back-fill is doing. */
  public enum State {
    RUNNING,
    PAUSED,
    DONE
  }

  /**
   * Tuning of the back-fill.
   *
   * @param label what the settings are for, in the logs
   * @param bytesPerSecond most bytes of bodies and receipts stored per second, 0 for no limit
   * @param requests requests in flight at most, each to a different peer
   * @param flushBytes bytes stored between two flushes, each moving the record of the lowest block
   * @param importGrace how long to wait after a block is imported
   * @param pollInterval how often a paused back-fill checks whether it can go on
   * @param headLagAllowance how far past a slot the chain head may lag before the rate halves
   * @param maxRangeBlocks most blocks per request
   * @param indexTransactions whether the transactions of the stored blocks are indexed
   */
  public record Settings(
      String label,
      long bytesPerSecond,
      int requests,
      long flushBytes,
      Duration importGrace,
      Duration pollInterval,
      Duration headLagAllowance,
      int maxRangeBlocks,
      boolean indexTransactions) {

    /** Returns the settings of the background back-fill in a mode. */
    public static Settings of(final BackfillMode mode, final boolean indexTransactions) {
      return new Settings(
          mode.name(),
          mode.bytesPerSecond(),
          mode.requests(),
          64L * 1024 * 1024,
          mode.importGrace(),
          Duration.ofSeconds(1),
          mode.headLagAllowance(),
          MAX_RANGE_BLOCKS,
          indexTransactions);
    }

    /** Returns settings without limit or pause, for the window the sync downloads. */
    public static Settings unthrottled(final boolean indexTransactions) {
      return new Settings(
          "window",
          0,
          4,
          64L * 1024 * 1024,
          Duration.ZERO,
          Duration.ofSeconds(1),
          Duration.ZERO,
          MAX_RANGE_BLOCKS,
          indexTransactions);
    }
  }

  private final MutableBlockchain blockchain;
  private final ProtocolSchedule protocolSchedule;
  private final PeerSelector peers;
  private final PeerTaskExecutor peerTaskExecutor;
  private final BooleanSupplier inSync;
  // the lowest block whose body is needed, given the chain head
  private final LongUnaryOperator lowestNeeded;
  private final Settings settings;
  private final Clock clock;
  private final SyncTransactionReceiptEncoder receiptEncoder =
      new SyncTransactionReceiptEncoder(new SimpleNoCopyRlpEncoder());
  private final ExecutorService workers;
  private final CompletableFuture<Void> done = new CompletableFuture<>();
  private final AtomicLong blocksStored = new AtomicLong();
  private final AtomicLong bytesStored = new AtomicLong();
  private final AtomicLong backoffs = new AtomicLong();
  private final AtomicLong lastLogNanos = new AtomicLong();
  // guarded by itself: the peers with a request in flight
  private final Set<EthPeer> busyPeers = new HashSet<>();
  private final Object throttle = new Object();

  // guarded by this
  private final BlockRanges stored = new BlockRanges();
  private final BlockRanges fetching = new BlockRanges();
  // the lowest block stored with every block above it, as last recorded
  private long persisted;
  private long bytesSinceFlush;
  private long observerId = -1;
  // guarded by throttle: the next store, the averages of the stored blocks, without and with
  // their receipts, and the share of the rate left by the back-off
  private long nextStoreNanos;
  private double bodyBytes = INITIAL_BLOCK_BYTES;
  private double blockBytes = INITIAL_BLOCK_BYTES;
  private double share = 1;
  private long lastDecreaseMillis = Long.MIN_VALUE / 2;
  private long lastIncreaseMillis = Long.MIN_VALUE / 2;

  private volatile long lowest;
  private volatile boolean stopped;
  private volatile State state = State.PAUSED;
  private volatile long busyUntilNanos = System.nanoTime();
  private volatile long loggedBlocks;
  private volatile long loggedBytes;

  /**
   * Creates the back-fill.
   *
   * @param inSync whether the node follows the chain head, the back-fill pausing otherwise
   * @param lowest the lowest block whose body and receipts are stored already, the download
   *     starting below it
   * @param lowestNeeded the lowest block whose body is needed for a chain head, above the floor of
   *     the sync and the blocks a pruner would remove
   */
  public BodyBackfill(
      final MutableBlockchain blockchain,
      final ProtocolSchedule protocolSchedule,
      final PeerSelector peers,
      final PeerTaskExecutor peerTaskExecutor,
      final BooleanSupplier inSync,
      final long lowest,
      final LongUnaryOperator lowestNeeded,
      final Settings settings,
      final Clock clock,
      final MetricsSystem metricsSystem) {
    this.blockchain = blockchain;
    this.protocolSchedule = protocolSchedule;
    this.peers = peers;
    this.peerTaskExecutor = peerTaskExecutor;
    this.inSync = inSync;
    this.lowest = lowest;
    this.persisted = lowest;
    this.lowestNeeded = lowestNeeded;
    this.settings = settings;
    this.clock = clock;
    this.workers =
        Executors.newFixedThreadPool(
            settings.requests(), Thread.ofPlatform().name("snap2-backfill-", 0).daemon().factory());
    registerMetrics(metricsSystem);
  }

  /** Starts downloading below the lowest stored block, unless every needed body is stored. */
  public void start() {
    if (lowest <= lowestNeeded.applyAsLong(blockchain.getChainHeadBlockNumber())) {
      finish();
      return;
    }
    lastLogNanos.set(System.nanoTime());
    synchronized (this) {
      observerId = blockchain.observeBlockAdded(this);
    }
    for (int i = 0; i < settings.requests(); i++) {
      workers.execute(this::run);
    }
  }

  /** Stops the back-fill, recording what it stored; nothing is written afterwards. */
  public void stop() {
    synchronized (this) {
      if (stopped) {
        return;
      }
      stopped = true;
    }
    unobserve();
    workers.shutdownNow();
    synchronized (this) {
      try {
        persist();
      } catch (final RuntimeException e) {
        LOG.debug("Could not record the back-filled blocks on stop: {}", e.getMessage());
      }
    }
    done.completeExceptionally(new CancellationException("body download stopped"));
  }

  /** Completes once every needed body is stored and recorded, or fails once stopped. */
  public CompletableFuture<Void> done() {
    return done;
  }

  public State state() {
    return state;
  }

  /**
   * Returns the lowest block whose body and receipts are stored, with those of every block above.
   */
  public long lowestStored() {
    return lowest;
  }

  /** Returns the number of blocks whose body and receipts are still to store. */
  public long blocksRemaining() {
    return Math.max(0, lowest - lowestNeeded.applyAsLong(blockchain.getChainHeadBlockNumber()));
  }

  public long blocksStored() {
    return blocksStored.get();
  }

  public long bytesStored() {
    return bytesStored.get();
  }

  /** Returns the rate in force, once the back-off took its share, 0 without limit. */
  public long bytesPerSecond() {
    synchronized (throttle) {
      return (long) (settings.bytesPerSecond() * share);
    }
  }

  /** Returns whether the rate is halved because the chain head lags the wall clock. */
  public boolean backedOff() {
    synchronized (throttle) {
      return share < 1;
    }
  }

  // a block import keeps the back-fill away for a while
  @Override
  public void onBlockAdded(final BlockAddedEvent event) {
    busyUntilNanos = System.nanoTime() + settings.importGrace().toNanos();
  }

  private void run() {
    while (!stopped) {
      try {
        step();
      } catch (final InterruptedException e) {
        return;
      } catch (final RuntimeException e) {
        LOG.debug("Body back-fill step failed, retrying: {}", e.getMessage());
        try {
          Thread.sleep(RETRY_DELAY.toMillis());
        } catch (final InterruptedException interrupted) {
          return;
        }
      }
    }
  }

  private void step() throws InterruptedException {
    maybeLog();
    if (!inSync.getAsBoolean() || busy()) {
      state = State.PAUSED;
      Thread.sleep(settings.pollInterval().toMillis());
      return;
    }
    adapt();
    final long[] range = nextRange();
    if (range == null) {
      if (idle()) {
        finish();
      } else {
        Thread.sleep(settings.pollInterval().toMillis());
      }
      return;
    }
    state = State.RUNNING;
    List<SyncBlockWithReceipts> blocks = null;
    try {
      blocks = download(range[0], range[1]);
    } finally {
      if (blocks == null) {
        release(range);
      }
    }
    if (blocks == null) {
      Thread.sleep(RETRY_DELAY.toMillis());
      return;
    }
    throttle(measure(blocks));
    store(range, blocks);
  }

  private boolean busy() {
    return System.nanoTime() - busyUntilNanos < 0;
  }

  // halves the rate while the chain head lags the wall clock, an import or a late block being
  // under way, and recovers it step by step once the head is fresh again
  private void adapt() {
    if (settings.bytesPerSecond() <= 0) {
      return;
    }
    final long now = clock.millis();
    final long lag = now - blockchain.getChainHeadHeader().getTimestamp() * 1000;
    final long slot = SLOT.toMillis();
    synchronized (throttle) {
      if (lag > slot + settings.headLagAllowance().toMillis()) {
        if (now - lastDecreaseMillis >= slot) {
          share = Math.max(MIN_SHARE, share / 2);
          lastDecreaseMillis = now;
          backoffs.incrementAndGet();
        }
      } else if (share < 1
          && now - lastDecreaseMillis >= slot
          && now - lastIncreaseMillis >= slot) {
        share = Math.min(1, share + SHARE_STEP);
        lastIncreaseMillis = now;
      }
    }
  }

  // the highest run of blocks below the lowest stored one that is neither stored nor in flight
  private synchronized long[] nextRange() {
    if (stopped) {
      return null;
    }
    final long floor = lowestNeeded.applyAsLong(blockchain.getChainHeadBlockNumber());
    final long[] range = BlockRanges.highestGap(stored, fetching, floor, lowest - 1, rangeBlocks());
    if (range != null) {
      fetching.add(range[0], range[1]);
    }
    return range;
  }

  private synchronized boolean idle() {
    return fetching.size() == 0;
  }

  private synchronized void release(final long[] range) {
    fetching.remove(range[0], range[1]);
  }

  // records the sizes of downloaded blocks and returns their total bytes
  private long measure(final List<SyncBlockWithReceipts> blocks) {
    long bodies = 0;
    long receipts = 0;
    for (final SyncBlockWithReceipts block : blocks) {
      bodies += block.getBlock().getBody().getRlp().size();
      for (final SyncTransactionReceipt receipt : block.getReceipts()) {
        receipts += receipt.getRlpBytes() == null ? 0 : receipt.getRlpBytes().size();
      }
    }
    synchronized (throttle) {
      bodyBytes = 0.8 * bodyBytes + 0.2 * bodies / blocks.size();
      blockBytes = 0.8 * blockBytes + 0.2 * (bodies + receipts) / blocks.size();
    }
    return bodies + receipts;
  }

  // blocks whose bodies fill about one response, so many when they are small
  private int rangeBlocks() {
    final int max = settings.maxRangeBlocks();
    synchronized (throttle) {
      return (int)
          Math.max(Math.min(MIN_RANGE_BLOCKS, max), Math.min(max, RESPONSE_BYTES / bodyBytes));
    }
  }

  // blocks whose receipts fill about one response
  private int receiptBlocks() {
    final int max = settings.maxRangeBlocks();
    synchronized (throttle) {
      final double receiptBytes = blockBytes - bodyBytes;
      return (int)
          Math.max(1, receiptBytes > 0 ? Math.min(max, RESPONSE_BYTES / receiptBytes) : max);
    }
  }

  // sleeps so that the stored bytes stay under the rate in force, with a burst of one range
  private void throttle(final long bytes) throws InterruptedException {
    final long rate = bytesPerSecond();
    if (rate <= 0) {
      return;
    }
    final long wait;
    synchronized (throttle) {
      final long now = System.nanoTime();
      final long start = Math.max(now, nextStoreNanos);
      nextStoreNanos = start + bytes * TimeUnit.SECONDS.toNanos(1) / rate;
      wait = start - now;
    }
    if (wait > 0) {
      TimeUnit.NANOSECONDS.sleep(wait);
    }
  }

  // a range whose headers a reorg replaced, or that a pruner would remove, is dropped
  private synchronized void store(final long[] range, final List<SyncBlockWithReceipts> blocks) {
    fetching.remove(range[0], range[1]);
    if (stopped) {
      return;
    }
    final long floor = lowestNeeded.applyAsLong(blockchain.getChainHeadBlockNumber());
    final List<SyncBlockWithReceipts> kept =
        blocks.stream().filter(block -> block.getNumber() >= floor).toList();
    if (kept.isEmpty() || !canonical(kept)) {
      return;
    }
    blockchain.unsafeStoreSyncBodiesAndReceipts(kept, settings.indexTransactions());
    stored.add(kept.getFirst().getNumber(), range[1]);
    final long bytes = measure(kept);
    blocksStored.addAndGet(kept.size());
    bytesStored.addAndGet(bytes);
    bytesSinceFlush += bytes;
    // every block from the start of the stored run below the lowest block is stored
    final long from = stored.startOf(lowest - 1);
    if (from >= 0) {
      stored.remove(from, lowest - 1);
      lowest = from;
    }
    if (bytesSinceFlush >= settings.flushBytes()) {
      persist();
    }
  }

  // records the lowest block once the writes it covers are flushed; holders of the lock only
  private void persist() {
    if (lowest < persisted) {
      blockchain.unsafeSetBodiesFrom(lowest);
      persisted = lowest;
    }
    bytesSinceFlush = 0;
  }

  private void finish() {
    synchronized (this) {
      if (stopped) {
        return;
      }
      stopped = true;
      persist();
    }
    state = State.DONE;
    unobserve();
    LOG.info("Bodies and receipts of every block from {} are stored", lowest);
    workers.shutdown();
    done.complete(null);
  }

  private void unobserve() {
    final long id;
    synchronized (this) {
      id = observerId;
      observerId = -1;
    }
    if (id >= 0) {
      blockchain.removeObserver(id);
    }
  }

  private List<SyncBlockWithReceipts> download(final long from, final long to) {
    final List<BlockHeader> headers = headers(from, to);
    if (headers == null) {
      return null;
    }
    final List<SyncBlock> bodies = bodies(headers);
    return bodies == null ? null : receipts(bodies);
  }

  // null when a header is missing, which the sync never leaves behind above its floor
  private List<BlockHeader> headers(final long from, final long to) {
    final List<BlockHeader> headers = new ArrayList<>((int) (to - from + 1));
    for (long number = from; number <= to; number++) {
      final Optional<BlockHeader> header = blockchain.getBlockHeader(number);
      if (header.isEmpty()) {
        LOG.warn("No header stored for block {}, its body cannot be back-filled", number);
        return null;
      }
      headers.add(header.get());
    }
    return headers;
  }

  // null when peers do not answer, the range being asked again later
  private List<SyncBlock> bodies(final List<BlockHeader> headers) {
    final List<SyncBlock> blocks = new ArrayList<>(headers.size());
    while (blocks.size() < headers.size()) {
      if (stopped) {
        return null;
      }
      final List<BlockHeader> remaining =
          List.copyOf(headers.subList(blocks.size(), headers.size()));
      final PeerTaskExecutorResult<List<SyncBlock>> result =
          request(
              new GetSyncBlockBodiesFromPeerTask(remaining, protocolSchedule, 0) {
                @Override
                public int getRetriesWithSamePeer() {
                  return 0;
                }
              });
      if (result.responseCode() != PeerTaskExecutorResponseCode.SUCCESS) {
        return null;
      }
      blocks.addAll(result.result().orElseThrow());
    }
    return blocks;
  }

  private List<SyncBlockWithReceipts> receipts(final List<SyncBlock> blocks) {
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
        if (stopped) {
          return null;
        }
        final PeerTaskExecutorResult<GetSyncReceiptsFromPeerTask.Response> result =
            request(
                new GetSyncReceiptsFromPeerTask(
                    new GetSyncReceiptsFromPeerTask.Request(List.copyOf(remaining), partial),
                    protocolSchedule,
                    receiptEncoder) {
                  @Override
                  public int getRetriesWithSamePeer() {
                    return 0;
                  }
                });
        if (result.responseCode() != PeerTaskExecutorResponseCode.SUCCESS) {
          return null;
        }
        final GetSyncReceiptsFromPeerTask.Response response = result.result().orElseThrow();
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

  // one request at a time per peer, so that the back-fill takes a small share of each
  private <T> PeerTaskExecutorResult<T> request(final PeerTask<T> task) {
    final Optional<EthPeer> peer;
    synchronized (busyPeers) {
      peer =
          peers.getPeer(
              task.getPeerRequirementFilter()
                  .and(candidate -> !busyPeers.contains(candidate.ethPeer())));
      peer.ifPresent(busyPeers::add);
    }
    if (peer.isEmpty()) {
      return new PeerTaskExecutorResult<>(
          Optional.empty(), PeerTaskExecutorResponseCode.NO_PEER_AVAILABLE, List.of());
    }
    try {
      return peerTaskExecutor.executeAgainstPeer(task, peer.get());
    } finally {
      synchronized (busyPeers) {
        busyPeers.remove(peer.get());
      }
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

  private int requestsInFlight() {
    synchronized (busyPeers) {
      return busyPeers.size();
    }
  }

  private void maybeLog() {
    final long now = System.nanoTime();
    final long last = lastLogNanos.get();
    if (now - last < LOG_INTERVAL.toNanos() || !lastLogNanos.compareAndSet(last, now)) {
      return;
    }
    final double seconds = (now - last) / 1e9;
    final long blocks = blocksStored.get();
    final long bytes = bytesStored.get();
    LOG.info(
        "Body back-fill {} ({}): blocks {} to the head stored, {} remaining, {} blocks/s, {} MiB/s of {} MiB/s{}",
        state.name().toLowerCase(Locale.ROOT),
        settings.label(),
        lowest,
        blocksRemaining(),
        String.format("%.1f", (blocks - loggedBlocks) / seconds),
        String.format("%.2f", (bytes - loggedBytes) / seconds / (1024 * 1024)),
        String.format("%.1f", bytesPerSecond() / (1024.0 * 1024)),
        backedOff() ? ", backed off while the chain head lags" : "");
    loggedBlocks = blocks;
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
    metricsSystem.createLongGauge(
        category,
        "snap2_backfill_rate_bytes_per_second",
        "Rate of the SNAP2 back-fill in force, once backed off, 0 without limit",
        this::bytesPerSecond);
    metricsSystem.createLongGauge(
        category,
        "snap2_backfill_requests_in_flight",
        "Requests of the SNAP2 back-fill waiting for a peer to answer",
        this::requestsInFlight);
    metricsSystem.createCounter(
        category,
        "snap2_backfill_blocks_total",
        "Blocks whose body and receipts the SNAP2 back-fill stored",
        blocksStored::get);
    metricsSystem.createCounter(
        category,
        "snap2_backfill_bytes_total",
        "Bytes of bodies and receipts the SNAP2 back-fill stored",
        bytesStored::get);
    metricsSystem.createCounter(
        category,
        "snap2_backfill_backoffs_total",
        "Times the SNAP2 back-fill halved its rate because the chain head lagged",
        backoffs::get);
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
}
