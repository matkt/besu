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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.FLAT_DB_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter.Entry;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SnappableKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes32;

/**
 * The flat database as the self-conversion reads it. Accounts are keyed by address hash and slots
 * by address hash then slot hash, so an account sorts just before its slots.
 */
final class FlatState {

  /** Key ranges, by first byte, converted independently. */
  static final int RANGES = 256;

  private FlatState() {}

  /** Hashes directly: the shared cache of {@code addressHash()} would only add cost here. */
  static byte[] account(final Address address) {
    return Hash.hash(address.getBytes()).getBytes().toArrayUnsafe();
  }

  static byte[] slot(final byte[] accountKey, final Hash slotHash) {
    final byte[] key = Arrays.copyOf(accountKey, 2 * Bytes32.SIZE);
    System.arraycopy(slotHash.getBytes().toArrayUnsafe(), 0, key, Bytes32.SIZE, Bytes32.SIZE);
    return key;
  }

  static boolean isAccount(final byte[] key) {
    return key.length == Bytes32.SIZE;
  }

  /** Whether {@code key} is a slot of the account keyed {@code accountKey}. */
  static boolean isSlotOf(final byte[] key, final byte[] accountKey) {
    return key.length == 2 * Bytes32.SIZE
        && Arrays.equals(key, 0, Bytes32.SIZE, accountKey, 0, Bytes32.SIZE);
  }

  static int rangeOf(final byte[] key) {
    return key[0] & 0xFF;
  }

  /** The smallest key after {@code key}. */
  static byte[] after(final byte[] key) {
    return Arrays.copyOf(key, key.length + 1);
  }

  /** The next entries of a range at {@code block}; {@code last} when the range has no more. */
  record Batch(Hash block, List<Entry> entries, boolean last) {}

  /**
   * Reads the accounts and slots of one range in key order, a batch at a time. Each batch comes
   * from a short-lived snapshot, and no read fills the block cache or Besu's caches.
   */
  static final class Batches {
    private final SnappableKeyValueStorage storage;
    private final byte range;
    private final int batchEntries;
    private byte[] from;

    Batches(final SnappableKeyValueStorage storage, final int range, final int batchEntries) {
      this.storage = storage;
      this.range = (byte) range;
      this.batchEntries = batchEntries;
      this.from = new byte[] {this.range};
    }

    /** The next batch; call again until it is the {@code last}. */
    Batch next() throws IOException {
      try (final SnappedKeyValueStorage snapshot = storage.takeSnapshot();
          final Stream<Pair<byte[], byte[]>> accountStream = read(snapshot, ACCOUNT_INFO_STATE);
          final Stream<Pair<byte[], byte[]>> slotStream = read(snapshot, ACCOUNT_STORAGE_STORAGE)) {
        final PeekingIterator<Pair<byte[], byte[]>> accounts =
            Iterators.peekingIterator(accountStream.iterator());
        final PeekingIterator<Pair<byte[], byte[]>> slots =
            Iterators.peekingIterator(slotStream.iterator());
        final List<Entry> entries = new ArrayList<>(batchEntries);
        while (entries.size() < batchEntries && (accounts.hasNext() || slots.hasNext())) {
          final boolean accountFirst =
              !slots.hasNext()
                  || (accounts.hasNext()
                      && Arrays.compareUnsigned(accounts.peek().getKey(), slots.peek().getKey())
                          < 0);
          final Pair<byte[], byte[]> next = accountFirst ? accounts.next() : slots.next();
          entries.add(new Entry(next.getKey(), next.getValue()));
        }
        // both columns resume after this key; keys created behind it are not read
        if (!entries.isEmpty()) {
          from = after(entries.getLast().key());
        }
        return new Batch(blockOf(snapshot), entries, !accounts.hasNext() && !slots.hasNext());
      }
    }

    private Stream<Pair<byte[], byte[]>> read(
        final SnappedKeyValueStorage snapshot, final SegmentIdentifier segment) {
      return snapshot
          .streamFromKeyUncached(segment, from)
          .takeWhile(entry -> entry.getKey()[0] == range);
    }
  }

  /** The block whose state the flat database holds, committed with that state. */
  private static Hash blockOf(final SnappedKeyValueStorage snapshot) {
    return snapshot
        .getUncached(TRIE_BRANCH_STORAGE, FLAT_DB_BLOCK_HASH_KEY)
        .or(() -> snapshot.getUncached(TRIE_BRANCH_STORAGE, WORLD_BLOCK_HASH_KEY))
        .map(bytes -> Hash.wrap(Bytes32.wrap(bytes)))
        .orElseThrow(() -> new IllegalStateException("the flat database has no block"));
  }
}
