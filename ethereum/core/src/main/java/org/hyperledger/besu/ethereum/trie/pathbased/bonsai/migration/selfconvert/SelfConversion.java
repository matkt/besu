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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.selfconvert;

import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.selfconvert.FlatState.RANGES;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.TrieNodeCodec;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.AscendingCollapseBinaryTrie;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.Leaf;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.convert.Eip8347SortedLeaves;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter.Entry;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347Pipelines;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347Progress;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.selfconvert.StateLeaves.Converted;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappableKeyValueStorage;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the PBT of a finalized block (the target) from the node's flat database, the preimage file
 * of an earlier block (the anchor) and the BALs since. Block import goes on meanwhile.
 */
public final class SelfConversion {

  private static final Logger LOG = LoggerFactory.getLogger(SelfConversion.class);

  /** Max entries read from one snapshot: a few milliseconds of reads. */
  static final int DEFAULT_BATCH_ENTRIES = 10_000;

  /** Partitions of the sorted leaves, deleted one by one during the build to bound the disk. */
  private static final int LEAF_PARTITIONS = 16;

  /** What the conversion reads from the chain. */
  public interface Chain {

    /** The header of a block, if the node has it. */
    Optional<BlockHeader> header(Hash blockHash);

    /** The canonical header at {@code number}; throws when the node has none. */
    BlockHeader canonicalHeader(long number);

    /** The trie log of a block, if the node has it. */
    Optional<TrieLog> trieLog(Hash blockHash);

    /** The BALs of consecutive canonical blocks; throws if one is missing. One caller at a time. */
    List<BlockAccessList> bals(List<BlockHeader> blocks);
  }

  /** What the conversion wrote: the PBT root, its leaf count and the number of distinct codes. */
  public record Result(Bytes32 pbtRoot, long leafCount, long codes) {}

  private final BonsaiWorldStateKeyValueStorage storage;
  private final Chain chain;
  private final Path workDir;
  private final long sortBufferBytes;
  private final ForkJoinPool pool;
  private final int batchEntries;

  /**
   * @param workDir where the external sorts spill
   * @param sortBufferBytes heap buffer of each external sort
   * @param pool runs the CPU work; its parallelism is also the number of ranges read at once
   */
  public SelfConversion(
      final BonsaiWorldStateKeyValueStorage storage,
      final Chain chain,
      final Path workDir,
      final long sortBufferBytes,
      final ForkJoinPool pool) {
    this(storage, chain, workDir, sortBufferBytes, pool, DEFAULT_BATCH_ENTRIES);
  }

  /** Same, with {@code batchEntries} per snapshot. */
  @VisibleForTesting
  SelfConversion(
      final BonsaiWorldStateKeyValueStorage storage,
      final Chain chain,
      final Path workDir,
      final long sortBufferBytes,
      final ForkJoinPool pool,
      final int batchEntries) {
    this.storage = storage;
    this.chain = chain;
    this.workDir = workDir;
    this.sortBufferBytes = sortBufferBytes;
    this.pool = pool;
    this.batchEntries = batchEntries;
  }

  /**
   * Writes the PBT of {@code target} and the codes' reference counts to {@code nodes}, no cursor.
   *
   * @throws Eip8347ArtifactVerificationException when a key has no preimage
   */
  public Result convert(
      final Path preimagesPath,
      final BlockHeader anchor,
      final BlockHeader target,
      final NodeUpdater nodes)
      throws IOException {
    if (!(storage.getComposedWorldStateStorage() instanceof SnappableKeyValueStorage flat)) {
      throw new UnsupportedOperationException("the flat database cannot take snapshots");
    }
    final Path sortDir =
        Files.createTempDirectory(Files.createDirectories(workDir), "eip8347-self-");
    try (final Eip8347ExternalSorter balPreimages =
            Eip8347ExternalSorter.distinctKeys(sortDir, "bal-preimages", sortBufferBytes, pool);
        final Eip8347ExternalSorter leaves =
            Eip8347ExternalSorter.partitioned(
                sortDir, "leaves", sortBufferBytes, pool, LEAF_PARTITIONS);
        final Eip8347ExternalSorter codeRefs = sorter(sortDir, "code-refs");
        final Eip8347ExternalSorter misses = sorter(sortDir, "misses");
        final Eip8347ExternalSorter referenceCounts = sorter(sortDir, "reference-counts")) {
      final CompletableFuture<long[]> starts =
          CompletableFuture.supplyAsync(() -> rangeStarts(preimagesPath), pool);
      final Steps steps = new Steps();
      // BALs first: a missing one fails before the long read
      LOG.info(
          "PBT self-conversion: preimages from the BALs of the {} blocks after block {}",
          target.getNumber() - anchor.getNumber(),
          anchor.getNumber());
      BalPreimages.collect(anchor, target, chain, balPreimages);
      steps.ended("BAL preimages");

      LOG.info(
          "PBT self-conversion: reading the flat database at block {} ({}), {} ranges at a time",
          target.getNumber(),
          target.getBlockHash(),
          pool.getParallelism());
      scan(flat, preimagesPath, Eip8347Pipelines.await(starts), target, leaves, codeRefs, misses);
      steps.ended("flat database");

      LOG.info("PBT self-conversion: keys created after block {}", anchor.getNumber());
      resolveMisses(misses.sorted(), balPreimages.sorted(), leaves, codeRefs);
      misses.close();
      balPreimages.close();
      steps.ended("new keys");

      LOG.info("PBT self-conversion: codes of {} contracts", codeRefs.added());
      final long codes =
          addCodeLeaves(flat, codeRefs.sorted(), codeRefs.added(), leaves, referenceCounts);
      codeRefs.close();
      steps.ended("codes");

      LOG.info("PBT self-conversion: building the PBT from {} leaves", leaves.added());
      final AscendingCollapseBinaryTrie pbt = new AscendingCollapseBinaryTrie(nodes);
      final StepProgress building = new StepProgress("building the PBT", leaves.added());
      final long leafCount =
          Eip8347SortedLeaves.build(
              leaves.sorted(), pbt, leaf -> building.add(1), pool, pool.getParallelism());
      final Bytes32 root = pbt.rootHash();
      // after every node: the counts' keys sort after the nodes' locations
      final Iterator<Entry> counts = referenceCounts.sorted();
      while (counts.hasNext()) {
        final Entry count = counts.next();
        nodes.store(
            TrieNodeCodec.codeRefCountKey(Bytes32.wrap(count.key())),
            null,
            Bytes32.wrap(count.value()));
      }
      steps.ended("PBT");
      LOG.info("PBT self-conversion done in {}", steps.summary());
      return new Result(root, leafCount, codes);
    } finally {
      Eip8347ExternalSorter.deleteRecursively(sortDir);
    }
  }

  /** Reads the ranges in parallel, each at the target and joined with its preimages. */
  private void scan(
      final SnappableKeyValueStorage flat,
      final Path preimagesPath,
      final long[] starts,
      final BlockHeader target,
      final Eip8347ExternalSorter leaves,
      final Eip8347ExternalSorter codeRefs,
      final Eip8347ExternalSorter misses)
      throws IOException {
    final TargetPriors priors =
        new TargetPriors(target, chain::header, chain::trieLog, TargetPriors.DEFAULT_MAX_PRIORS);
    final AtomicInteger rangesDone = new AtomicInteger();
    final StepProgress progress = new StepProgress("reading the flat database", RANGES);
    final int threads = pool.getParallelism();
    Eip8347Pipelines.run(
        Eip8347Pipelines.from("eip8347-self-ranges", IntStream.range(0, RANGES).iterator())
            .thenFlatMapInParallel(
                "eip8347-self-read-ranges",
                range -> {
                  try {
                    return Streams.stream(
                        new RangeScan(
                            range,
                            new FlatState.Batches(flat, range, batchEntries),
                            priors,
                            Eip8347PreimageFile.section(
                                preimagesPath, starts[range], starts[range + 1]),
                            rangesDone::incrementAndGet));
                  } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                  }
                },
                threads,
                2 * threads)
            .andFinishWith(
                "eip8347-self-sort-state",
                converted -> {
                  sort(converted, leaves, codeRefs, misses);
                  // the ranges finished since the previous batch
                  progress.add(rangesDone.getAndSet(0));
                }));
  }

  /** Converts the keys created after the anchor, with their BAL preimages. */
  private void resolveMisses(
      final Iterator<Entry> sortedMisses,
      final Iterator<Entry> sortedPreimages,
      final Eip8347ExternalSorter leaves,
      final Eip8347ExternalSorter codeRefs)
      throws IOException {
    Eip8347Pipelines.run(
        Eip8347Pipelines.fromLists(
                "eip8347-self-misses",
                BalPreimages.join(sortedMisses, sortedPreimages),
                known -> 1,
                2 * pool.getParallelism())
            .thenProcessInParallel(
                "eip8347-self-miss-leaves",
                keys -> {
                  final StateLeaves converted = new StateLeaves();
                  keys.forEach(converted::add);
                  return converted.converted();
                },
                pool.getParallelism())
            .andFinishWith(
                "eip8347-self-sort-misses", converted -> sort(converted, leaves, codeRefs, null)));
  }

  /** Adds the leaves of the contracts; returns how many codes were given a reference count. */
  private long addCodeLeaves(
      final SnappableKeyValueStorage flat,
      final Iterator<Entry> sortedRefs,
      final long contracts,
      final Eip8347ExternalSorter leaves,
      final Eip8347ExternalSorter referenceCounts)
      throws IOException {
    final AtomicInteger codes = new AtomicInteger();
    final StepProgress progress = new StepProgress("contract codes", contracts);
    Eip8347Pipelines.run(
        Eip8347Pipelines.fromLists(
                "eip8347-self-codes",
                CodeLeaves.groups(sortedRefs),
                CodeLeaves::weight,
                2 * pool.getParallelism())
            .thenProcessInParallel(
                "eip8347-self-code-leaves",
                groups ->
                    CodeLeaves.leaves(
                        flat, storage.getFlatDbStrategy().getCodeStorageStrategy(), groups),
                pool.getParallelism())
            .andFinishWith(
                "eip8347-self-sort-codes",
                coded -> {
                  try {
                    for (final Leaf leaf : coded.leaves()) {
                      Eip8347SortedLeaves.add(leaves, leaf);
                    }
                    for (final Entry count : coded.referenceCounts()) {
                      referenceCounts.add(count.key(), count.value());
                    }
                    codes.addAndGet(coded.referenceCounts().size());
                    progress.add(coded.contracts());
                  } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                  }
                }));
    return codes.get();
  }

  /** Sorts what a conversion step gave; {@code misses} null when it leaves none. */
  private static void sort(
      final Converted converted,
      final Eip8347ExternalSorter leaves,
      final Eip8347ExternalSorter codeRefs,
      final Eip8347ExternalSorter misses) {
    try {
      for (final Leaf leaf : converted.leaves()) {
        Eip8347SortedLeaves.add(leaves, leaf);
      }
      for (final Entry ref : converted.codeRefs()) {
        codeRefs.add(ref.key(), ref.value());
      }
      for (final Entry miss : converted.misses()) {
        misses.add(miss.key(), miss.value());
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static long[] rangeStarts(final Path preimagesPath) {
    try {
      return RangeScan.starts(preimagesPath);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Eip8347ExternalSorter sorter(final Path sortDir, final String name) {
    return new Eip8347ExternalSorter(sortDir, name, sortBufferBytes, pool);
  }

  /** How long each step of a conversion took, for its last log line. */
  private static final class Steps {
    private final long startMillis = System.currentTimeMillis();
    private final StringJoiner durations = new StringJoiner(", ");
    private long lastMillis = startMillis;

    /** Records {@code step}, which ran since the previous one ended. */
    void ended(final String step) {
      final long now = System.currentTimeMillis();
      durations.add(step + " " + words(now - lastMillis));
      lastMillis = now;
    }

    /** The total time, then each step's. */
    String summary() {
      return words(lastMillis - startMillis) + " (" + durations + ")";
    }

    private static String words(final long millis) {
      return DurationFormatUtils.formatDurationWords(millis, true, true);
    }
  }

  /** Logs how far a step is, at most once per {@link Eip8347Progress#INTERVAL}. */
  private static final class StepProgress {
    private final String step;
    private final long total;
    private final long checkEvery;
    private final Eip8347Progress progress = new Eip8347Progress(System.currentTimeMillis());
    private long done;
    private long nextCheck;

    StepProgress(final String step, final long total) {
      this.step = step;
      this.total = total;
      // the clock is read about 10,000 times per step, not once per item
      this.checkEvery = Math.max(1, total / 10_000);
    }

    /** Counts {@code count} more items done; one thread at a time. */
    void add(final long count) {
      done += count;
      if (done >= nextCheck) {
        nextCheck = done + checkEvery;
        progress
            .line(done, total, System.currentTimeMillis())
            .ifPresent(line -> LOG.info("PBT self-conversion in progress, {}: {}", step, line));
      }
    }
  }
}
