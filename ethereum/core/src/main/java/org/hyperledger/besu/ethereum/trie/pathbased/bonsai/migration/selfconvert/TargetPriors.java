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

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter.Entry;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.tuweni.units.bigints.UInt256;

/**
 * Puts entries read at later blocks back to their value at the target, from the trie logs. Keeps
 * only the keys their range has not read yet, so it shrinks as the ranges move on. Thread-safe.
 */
final class TargetPriors {

  /** Max keys kept (about 200 bytes each); more means finality stalled for too long. */
  static final int DEFAULT_MAX_PRIORS = 2_000_000;

  /** The value of a key that did not exist at the target. */
  private static final byte[] ABSENT = new byte[0];

  private final BlockHeader target;
  private final Function<Hash, Optional<BlockHeader>> headers;
  private final Function<Hash, Optional<TrieLog>> trieLogs;
  private final int maxPriors;
  private final Set<Hash> recorded = new HashSet<>();
  private final NavigableMap<byte[], byte[]> priors = new TreeMap<>(Arrays::compareUnsigned);

  /** Per range, the last key read: a range has read every key up to it. */
  private final byte[][] reached = new byte[RANGES][];

  private final boolean[] finished = new boolean[RANGES];

  TargetPriors(
      final BlockHeader target,
      final Function<Hash, Optional<BlockHeader>> headers,
      final Function<Hash, Optional<TrieLog>> trieLogs,
      final int maxPriors) {
    this.target = target;
    this.headers = headers;
    this.trieLogs = trieLogs;
    this.maxPriors = maxPriors;
    for (int range = 0; range < RANGES; range++) {
      reached[range] = new byte[] {(byte) range};
    }
  }

  /** Records the changes of the blocks from the target to {@code block}, oldest first. */
  synchronized void advanceTo(final Hash block) {
    final Deque<BlockHeader> newBlocks = new ArrayDeque<>();
    for (Hash hash = block;
        !hash.equals(target.getBlockHash()) && !recorded.contains(hash);
        hash = newBlocks.getFirst().getParentHash()) {
      final Hash wanted = hash;
      final BlockHeader header =
          headers
              .apply(wanted)
              .orElseThrow(() -> new IllegalStateException("no header for block " + wanted));
      if (header.getNumber() <= target.getNumber()) {
        throw new IllegalStateException(
            "block " + header.getNumber() + " does not descend from block " + target.getNumber());
      }
      newBlocks.push(header);
    }
    for (final BlockHeader header : newBlocks) {
      record(
          trieLogs
              .apply(header.getBlockHash())
              .orElseThrow(
                  () -> new IllegalStateException("no trie log for block " + header.getNumber())));
      recorded.add(header.getBlockHash());
    }
  }

  /** {@code batch}, read at a block passed to {@link #advanceTo}, as it was at the target. */
  List<Entry> atTarget(final int range, final List<Entry> batch) {
    return batch.isEmpty() ? batch : merge(batch, take(range, batch.getLast().key()));
  }

  /** The keys of {@code range} past its last batch that were deleted after the target. */
  List<Entry> rest(final int range) {
    return merge(List.of(), takeRest(range));
  }

  /** Removes the values of {@code range} up to {@code last}, which the range reached. */
  private synchronized List<Entry> take(final int range, final byte[] last) {
    final List<Entry> taken = remove(priors.subMap(reached[range], false, last, true));
    reached[range] = last;
    return taken;
  }

  private synchronized List<Entry> takeRest(final int range) {
    final List<Entry> taken =
        remove(
            range + 1 < RANGES
                ? priors.subMap(reached[range], false, new byte[] {(byte) (range + 1)}, false)
                : priors.tailMap(reached[range], false));
    finished[range] = true;
    return taken;
  }

  /** Copies then clears {@code values}: TreeMap reuses a removed entry for another key. */
  private static List<Entry> remove(final NavigableMap<byte[], byte[]> values) {
    final List<Entry> removed = new ArrayList<>();
    values.forEach((key, value) -> removed.add(new Entry(key, value)));
    values.clear();
    return removed;
  }

  /** Merges {@code changed} into {@code batch}, both in key order; {@code changed} wins. */
  private static List<Entry> merge(final List<Entry> batch, final List<Entry> changed) {
    if (changed.isEmpty()) {
      return batch;
    }
    final List<Entry> merged = new ArrayList<>(batch.size() + changed.size());
    int next = 0;
    for (final Entry entry : batch) {
      // in the table only: deleted since the target
      while (next < changed.size()
          && Arrays.compareUnsigned(changed.get(next).key(), entry.key()) < 0) {
        addIfPresent(merged, changed.get(next++));
      }
      if (next < changed.size() && Arrays.equals(changed.get(next).key(), entry.key())) {
        addIfPresent(merged, changed.get(next++));
      } else {
        merged.add(entry);
      }
    }
    while (next < changed.size()) {
      addIfPresent(merged, changed.get(next++));
    }
    return merged;
  }

  private static void addIfPresent(final List<Entry> entries, final Entry prior) {
    if (prior.value() != ABSENT) {
      entries.add(prior);
    }
  }

  private void record(final TrieLog trieLog) {
    trieLog
        .getAccountChanges()
        .forEach((address, change) -> keep(FlatState.account(address), account(change.getPrior())));
    trieLog
        .getStorageChanges()
        .forEach(
            (address, slots) -> {
              final byte[] accountKey = FlatState.account(address);
              slots.forEach(
                  (slot, change) ->
                      keep(
                          FlatState.slot(accountKey, slot.getSlotHash()), slot(change.getPrior())));
            });
  }

  /** Keeps the first value seen for a key its range has not read yet. */
  private void keep(final byte[] key, final byte[] prior) {
    final int range = FlatState.rangeOf(key);
    if (finished[range] || Arrays.compareUnsigned(key, reached[range]) <= 0) {
      return;
    }
    priors.putIfAbsent(key, prior);
    if (priors.size() > maxPriors) {
      throw new IllegalStateException(
          "more than "
              + maxPriors
              + " keys changed since block "
              + target.getNumber()
              + " are still to be read");
    }
  }

  /** An account encoded as the flat database stores it. */
  private static byte[] account(final AccountValue prior) {
    return prior == null ? ABSENT : RLP.encode(prior::writeTo).toArrayUnsafe();
  }

  /** A slot value as the flat database stores it: 32 bytes, none for zero. */
  private static byte[] slot(final UInt256 prior) {
    return prior == null || prior.isZero() ? ABSENT : prior.toArrayUnsafe();
  }
}
