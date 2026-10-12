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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Flat state kept in two segments: accounts keyed by hash and slots keyed by both hashes. */
abstract class AbstractSyncStateStorage implements SyncStateStorage {

  private static final byte[] ZEROS = new byte[32];
  private static final byte[] ONES = Keys.MAX.toArray();

  protected final SegmentedKeyValueStorage flat;
  protected final SegmentIdentifier accountSegment;
  protected final SegmentIdentifier storageSegment;

  protected AbstractSyncStateStorage(
      final SegmentedKeyValueStorage flat,
      final SegmentIdentifier accountSegment,
      final SegmentIdentifier storageSegment) {
    this.flat = flat;
    this.accountSegment = accountSegment;
    this.storageSegment = storageSegment;
  }

  @Override
  public Optional<Bytes> getAccount(final Bytes32 accountHash) {
    return flat.get(accountSegment, accountHash.toArrayUnsafe()).map(Bytes::wrap);
  }

  @Override
  public Optional<Bytes> getStorage(final Bytes32 accountHash, final Bytes32 slotHash) {
    return flat.get(storageSegment, slotKey(accountHash, slotHash)).map(Bytes::wrap);
  }

  @Override
  public void scan(final Bytes32 from, final Bytes32 to, final ScanVisitor visitor) {
    try (Stream<Pair<byte[], byte[]>> accounts =
            flat.streamFromKey(accountSegment, from.toArrayUnsafe(), to.toArrayUnsafe());
        Stream<Pair<byte[], byte[]>> slots =
            flat.streamFromKey(
                storageSegment,
                concat(from.toArrayUnsafe(), ZEROS),
                concat(to.toArrayUnsafe(), ONES))) {
      final Iterator<Pair<byte[], byte[]>> accountIterator = accounts.iterator();
      final PeekingIterator<Pair<byte[], byte[]>> slotIterator =
          Iterators.peekingIterator(slots.iterator());
      while (accountIterator.hasNext()) {
        final Pair<byte[], byte[]> account = accountIterator.next();
        final byte[] accountKey = account.getKey();
        visitSlots(slotIterator, accountKey, false, visitor);
        visitor.onAccount(Bytes32.wrap(accountKey), Bytes.wrap(account.getValue()));
        visitSlots(slotIterator, accountKey, true, visitor);
      }
      visitSlots(slotIterator, ONES, true, visitor);
    }
  }

  @Override
  public void forEachSlot(final Bytes32 accountHash, final Consumer<Bytes32> slotHashes) {
    final byte[] prefix = accountHash.toArrayUnsafe();
    try (Stream<Pair<byte[], byte[]>> slots =
        flat.streamFromKey(storageSegment, concat(prefix, ZEROS), concat(prefix, ONES))) {
      slots.forEach(
          slot -> slotHashes.accept(Bytes32.wrap(Arrays.copyOfRange(slot.getKey(), 32, 64))));
    }
  }

  // visits slots of accounts before the given one, or of the given account when inclusive
  private static void visitSlots(
      final PeekingIterator<Pair<byte[], byte[]>> slots,
      final byte[] accountKey,
      final boolean inclusive,
      final ScanVisitor visitor) {
    while (slots.hasNext()) {
      final byte[] key = slots.peek().getKey();
      final int cmp = Arrays.compareUnsigned(key, 0, 32, accountKey, 0, 32);
      if (cmp > 0 || (cmp == 0 && !inclusive)) {
        return;
      }
      final Pair<byte[], byte[]> slot = slots.next();
      visitor.onSlot(
          Bytes32.wrap(Arrays.copyOfRange(key, 0, 32)),
          Bytes32.wrap(Arrays.copyOfRange(key, 32, 64)),
          Bytes.wrap(slot.getValue()));
    }
  }

  protected static byte[] slotKey(final Bytes32 accountHash, final Bytes32 slotHash) {
    return concat(accountHash.toArrayUnsafe(), slotHash.toArrayUnsafe());
  }

  private static byte[] concat(final byte[] a, final byte[] b) {
    final byte[] out = new byte[a.length + b.length];
    System.arraycopy(a, 0, out, 0, a.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }
}
