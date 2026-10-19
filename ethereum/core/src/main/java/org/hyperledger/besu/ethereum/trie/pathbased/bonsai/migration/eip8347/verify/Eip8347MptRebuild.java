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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.verify;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter.Entry;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347Pipelines;
import org.hyperledger.besu.ethereum.trie.patricia.AscendingCollapsePatriciaTrie;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Check 2: rebuilds the MPT root from the preimages and the matched values sorted by ordinal,
 * building storage tries and ranges of keys on workers (see the EIP-8347 README, step 4).
 */
final class Eip8347MptRebuild {

  /** Most slots an account can have to be built as one unit. */
  static final int UNIT_SLOTS = 4096;

  /** A range past this size (keys ground to share a prefix) is inserted key by key. */
  static final int MAX_RANGE_KEYS = 4 * UNIT_SLOTS;

  /** Keys cut into ranges average at least this many per range. */
  private static final int RANGE_KEYS = 256;

  /** An item as read. */
  private sealed interface Read {
    int weight();
  }

  /** An item once its paths are hashed. */
  private sealed interface Hashed {}

  /** An item once large storages are cut into ranges. */
  private sealed interface Cut {}

  /** An item once the ranges of large storages are built. */
  private sealed interface Built {}

  /** An account and all its slots. */
  private record Account(Address address, List<Bytes32> slotKeys, List<Bytes> values, byte[] fields)
      implements Read {
    @Override
    public int weight() {
      return 1 + slotKeys.size();
    }
  }

  /** Consecutive slots of a large account. */
  private record SlotChunk(LargeStorage storage, List<Bytes32> slotKeys, List<Bytes> values)
      implements Read {
    @Override
    public int weight() {
      return slotKeys.size();
    }
  }

  /** A large account, after all its slots. */
  private record LargeAccount(Address address, LargeStorage storage, byte[] fields)
      implements Read {
    @Override
    public int weight() {
      return 1;
    }
  }

  /** The end of the preimages: the last range of accounts can be closed. */
  private record End() implements Read, Hashed, Cut, Built {
    @Override
    public int weight() {
      return 1;
    }
  }

  /** An account leaf. */
  private record Leaf(Bytes addressHash, Bytes value) implements Hashed, Cut, Built {}

  /** Consecutive slots of a large account, with their paths and RLP values. */
  private record HashedSlots(LargeStorage storage, List<Bytes> keyHashes, List<Bytes> values)
      implements Hashed {}

  /** A large account, whose leaf needs its storage root once every slot is attached. */
  private record LargeLeaf(
      Bytes addressHash, LargeStorage storage, Eip8347AnchorJoin.AccountFields fields)
      implements Hashed, Cut, Built {}

  /** A range of a large account's slots. */
  private record StorageRange(LargeStorage storage, Range range) implements Cut {}

  /** A range of a large account's slots, built. */
  private record BuiltStorageRange(LargeStorage storage, BuiltRange range) implements Built {}

  /** The storage trie of a large account, and its slot count to size its ranges. */
  private record LargeStorage(AscendingCollapsePatriciaTrie trie, int slots) {}

  /**
   * Consecutive keys sharing their first nibbles: a complete subtree when {@code subtree}, else
   * keys to insert one by one.
   */
  private record Range(List<Bytes> keys, List<Bytes> values, boolean subtree) {}

  /** A range, built as a subtree unless its keys are to be inserted one by one. */
  private record BuiltRange(Range range, AscendingCollapsePatriciaTrie.Subtree subtree) {}

  private final AscendingCollapsePatriciaTrie accountTrie = new AscendingCollapsePatriciaTrie();
  private final PrefixCutter accountRanges;
  private Bytes lastAttachedAccount;

  // The large storage being cut.
  private LargeStorage cutStorage;
  private PrefixCutter storageRanges;
  private Bytes lastCutSlot;

  private Eip8347MptRebuild(final long accounts) {
    this.accountRanges = new PrefixCutter(nibbles(accounts));
  }

  /**
   * Rebuilds the MPT root. The join matched every preimage entry exactly once, so {@code values}
   * are exactly the ordinals {@code 0..n-1}.
   *
   * @param accounts accounts in the preimage file, to size the ranges of the account trie
   * @throws Eip8347ArtifactVerificationException if the preimages are not in keccak order
   */
  static Bytes32 rootHash(
      final Path preimagesPath, final Iterator<Entry> values, final long accounts)
      throws IOException {
    final Eip8347MptRebuild rebuild = new Eip8347MptRebuild(accounts);
    try (final Eip8347PreimageFile preimages =
        Eip8347PreimageFile.withoutOrderCheck(preimagesPath)) {
      Eip8347Pipelines.run(
          Eip8347Pipelines.fromLists(
                  "eip8347-verify-mpt-read", new Reader(preimages, values), Read::weight)
              .thenProcessAsyncOrdered(
                  "eip8347-verify-mpt-hash",
                  Eip8347Pipelines.async(
                      items ->
                          rejecting(() -> items.stream().map(Eip8347MptRebuild::hash).toList())),
                  Eip8347Pipelines.PARALLELISM)
              .thenProcess("eip8347-verify-mpt-cut-storage", rebuild::cutStorage)
              .thenProcessAsyncOrdered(
                  "eip8347-verify-mpt-storage-ranges",
                  Eip8347Pipelines.async(
                      items ->
                          rejecting(() -> items.stream().map(Eip8347MptRebuild::build).toList())),
                  Eip8347Pipelines.PARALLELISM)
              .thenProcess(
                  "eip8347-verify-mpt-storage",
                  items -> rejecting(() -> rebuild.attachStorage(items)))
              .thenProcessAsyncOrdered(
                  "eip8347-verify-mpt-account-ranges",
                  Eip8347Pipelines.async(
                      ranges ->
                          rejecting(() -> ranges.stream().map(rebuild::buildAccounts).toList())),
                  Eip8347Pipelines.PARALLELISM)
              .andFinishWith(
                  "eip8347-verify-mpt",
                  ranges ->
                      rejecting(
                          () -> {
                            ranges.forEach(rebuild::attachAccounts);
                            return ranges;
                          })));
    }
    return rebuild.accountTrie.rootHash();
  }

  /** Hashes an item's paths, and builds a whole account's leaf. */
  private static Hashed hash(final Read item) {
    return switch (item) {
      case Account account -> {
        final List<Bytes> keyHashes = slotPaths(account.slotKeys());
        final List<Bytes> slotValues = slotValues(account.values());
        final AscendingCollapsePatriciaTrie storage = new AscendingCollapsePatriciaTrie();
        for (int i = 0; i < keyHashes.size(); i++) {
          storage.insert(keyHashes.get(i), slotValues.get(i));
        }
        final Hash storageRoot = Hash.wrap(storage.rootHash());
        yield new Leaf(
            Hash.hash(account.address().getBytes()).getBytes(),
            leaf(Eip8347AnchorJoin.AccountFields.decode(account.fields()), storageRoot));
      }
      case SlotChunk chunk ->
          new HashedSlots(chunk.storage(), slotPaths(chunk.slotKeys()), slotValues(chunk.values()));
      case LargeAccount account ->
          new LargeLeaf(
              Hash.hash(account.address().getBytes()).getBytes(),
              account.storage(),
              Eip8347AnchorJoin.AccountFields.decode(account.fields()));
      case End end -> end;
    };
  }

  /** Cuts the slots of large storages into ranges, checking their order between chunks. */
  private List<Cut> cutStorage(final List<Hashed> items) {
    final List<Cut> cut = new ArrayList<>();
    for (final Hashed item : items) {
      switch (item) {
        case HashedSlots slots -> {
          if (!slots.storage().equals(cutStorage)) {
            cutStorage = slots.storage();
            storageRanges = new PrefixCutter(nibbles(cutStorage.slots()));
            lastCutSlot = null;
          }
          Eip8347PreimageFile.checkSlotOrder(lastCutSlot, slots.keyHashes().getFirst());
          lastCutSlot = slots.keyHashes().getLast();
          final LargeStorage storage = cutStorage;
          for (int i = 0; i < slots.keyHashes().size(); i++) {
            storageRanges.add(
                slots.keyHashes().get(i),
                slots.values().get(i),
                range -> cut.add(new StorageRange(storage, range)));
          }
        }
        case LargeLeaf leaf -> {
          storageRanges.finish(range -> cut.add(new StorageRange(leaf.storage(), range)));
          cut.add(leaf);
          cutStorage = null;
          storageRanges = null;
        }
        case Leaf leaf -> cut.add(leaf);
        case End end -> cut.add(end);
      }
    }
    return cut;
  }

  /** Builds a range of a large storage off the attaching thread. */
  private static Built build(final Cut item) {
    return switch (item) {
      case StorageRange slots ->
          new BuiltStorageRange(slots.storage(), build(slots.storage().trie(), slots.range()));
      case Leaf leaf -> leaf;
      case LargeLeaf leaf -> leaf;
      case End end -> end;
    };
  }

  /** Joins the ranges of large storages, in order, and cuts the account leaves into ranges. */
  private List<Range> attachStorage(final List<Built> items) {
    final List<Range> closed = new ArrayList<>();
    for (final Built item : items) {
      switch (item) {
        case BuiltStorageRange slots -> attach(slots.storage().trie(), slots.range());
        case Leaf leaf -> accountRanges.add(leaf.addressHash(), leaf.value(), closed::add);
        case LargeLeaf leaf ->
            accountRanges.add(
                leaf.addressHash(),
                leaf(leaf.fields(), Hash.wrap(leaf.storage().trie().rootHash())),
                closed::add);
        case End end -> accountRanges.finish(closed::add);
      }
    }
    return closed;
  }

  /** Builds a range of accounts, after checking their keccak order inside it. */
  private BuiltRange buildAccounts(final Range range) {
    Bytes previous = null;
    for (final Bytes key : range.keys()) {
      Eip8347PreimageFile.checkAccountOrder(previous, key);
      previous = key;
    }
    return build(accountTrie, range);
  }

  /** Attaches a range of accounts, after checking it starts past the previous one. */
  private void attachAccounts(final BuiltRange range) {
    Eip8347PreimageFile.checkAccountOrder(lastAttachedAccount, range.range().keys().getFirst());
    lastAttachedAccount = range.range().keys().getLast();
    attach(accountTrie, range);
  }

  /** Builds a range as a subtree, unless its keys are to be inserted one by one. */
  private static BuiltRange build(final AscendingCollapsePatriciaTrie trie, final Range range) {
    return new BuiltRange(
        range, range.subtree() ? trie.prepare(range.keys(), range.values()) : null);
  }

  private static void attach(final AscendingCollapsePatriciaTrie trie, final BuiltRange range) {
    if (range.subtree() != null) {
      trie.insert(range.subtree());
      return;
    }
    for (int i = 0; i < range.range().keys().size(); i++) {
      trie.insert(range.range().keys().get(i), range.range().values().get(i));
    }
  }

  /** Runs {@code step}, turning a trie's {@link IllegalArgumentException} into a rejection. */
  private static <T> T rejecting(final Supplier<T> step) {
    try {
      return step.get();
    } catch (final IllegalArgumentException e) {
      throw new Eip8347ArtifactVerificationException(e.getMessage(), e);
    }
  }

  /** The keccak paths of {@code slotKeys}, which must be in ascending order. */
  private static List<Bytes> slotPaths(final List<Bytes32> slotKeys) {
    final List<Bytes> paths = new ArrayList<>(slotKeys.size());
    for (final Bytes32 slotKey : slotKeys) {
      final Bytes path = Hash.hash(slotKey).getBytes();
      Eip8347PreimageFile.checkSlotOrder(paths.isEmpty() ? null : paths.getLast(), path);
      paths.add(path);
    }
    return paths;
  }

  private static Bytes leaf(final Eip8347AnchorJoin.AccountFields fields, final Hash storageRoot) {
    return RLP.encode(
        new PatriciaTrieAccountValue(
                fields.nonce(), fields.balance(), storageRoot, fields.codeHash())
            ::writeTo);
  }

  private static List<Bytes> slotValues(final List<Bytes> values) {
    return values.stream().map(value -> RLP.encode(out -> out.writeBytes(value))).toList();
  }

  /**
   * Nibbles to cut {@code keys} keys on: ranges average from {@link #RANGE_KEYS} to 16 times it.
   */
  private static int nibbles(final long keys) {
    final long ranges = keys / RANGE_KEYS;
    return ranges == 0 ? 0 : (63 - Long.numberOfLeadingZeros(ranges)) / 4;
  }

  /**
   * Cuts keys arriving in order into ranges sharing their first {@code nibbles} nibbles. A range
   * that grows past {@link #MAX_RANGE_KEYS} is no longer a whole prefix: it and the rest of its
   * prefix are emitted as keys to insert one by one.
   */
  private static final class PrefixCutter {
    private final int nibbles;
    private int prefix = -1;
    private boolean plain;
    private List<Bytes> keys = new ArrayList<>();
    private List<Bytes> values = new ArrayList<>();

    PrefixCutter(final int nibbles) {
      this.nibbles = nibbles;
    }

    /** Adds a key, handing the range it closes, if any, to {@code closed}. */
    void add(final Bytes key, final Bytes value, final Consumer<Range> closed) {
      final int keyPrefix = nibbles == 0 ? 0 : key.getInt(0) >>> (Integer.SIZE - 4 * nibbles);
      if (keyPrefix != prefix && !keys.isEmpty()) {
        close(closed);
        plain = false;
      }
      prefix = keyPrefix;
      keys.add(key);
      values.add(value);
      if (keys.size() >= MAX_RANGE_KEYS) {
        plain = true;
        close(closed);
      }
    }

    void finish(final Consumer<Range> closed) {
      close(closed);
    }

    private void close(final Consumer<Range> closed) {
      if (!keys.isEmpty()) {
        closed.accept(new Range(keys, values, !plain));
        keys = new ArrayList<>();
        values = new ArrayList<>();
      }
    }
  }

  /**
   * Zips the preimage file with the values sorted by ordinal: an account with its slots, or a large
   * account's slots in chunks. Hashes nothing.
   */
  private static final class Reader implements Iterator<Read> {
    private final Eip8347PreimageFile preimages;
    private final Iterator<Entry> values;
    private final Deque<Read> pending = new ArrayDeque<>();
    private long ordinal;
    private boolean exhausted;

    // The large account being read.
    private Address largeAddress;
    private LargeStorage large;
    private int largeSlotsLeft;

    Reader(final Eip8347PreimageFile preimages, final Iterator<Entry> values) {
      this.preimages = preimages;
      this.values = values;
    }

    @Override
    public boolean hasNext() {
      try {
        while (pending.isEmpty() && !exhausted) {
          read();
        }
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
      return !pending.isEmpty();
    }

    @Override
    public Read next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return pending.poll();
    }

    /** Reads one more account, or the next chunk of a large one. */
    private void read() throws IOException {
      if (large != null) {
        final int count = Math.min(largeSlotsLeft, Eip8347Pipelines.LIST_WEIGHT);
        final List<Bytes32> slotKeys = new ArrayList<>(count);
        final List<Bytes> slotValues = new ArrayList<>(count);
        readSlots(count, slotKeys, slotValues);
        pending.add(new SlotChunk(large, slotKeys, slotValues));
        largeSlotsLeft -= count;
        if (largeSlotsLeft == 0) {
          pending.add(new LargeAccount(largeAddress, large, nextValue()));
          large = null;
        }
        return;
      }
      final Eip8347PreimageFile.Account account = preimages.nextAccount();
      if (account == null) {
        exhausted = true;
        if (values.hasNext()) {
          throw new IllegalStateException("MPT value beyond the last preimage entry");
        }
        pending.add(new End());
        return;
      }
      if (account.slotCount() > UNIT_SLOTS) {
        largeAddress = account.address();
        large = new LargeStorage(new AscendingCollapsePatriciaTrie(), account.slotCount());
        largeSlotsLeft = account.slotCount();
        return;
      }
      final List<Bytes32> slotKeys = new ArrayList<>(account.slotCount());
      final List<Bytes> slotValues = new ArrayList<>(account.slotCount());
      readSlots(account.slotCount(), slotKeys, slotValues);
      pending.add(new Account(account.address(), slotKeys, slotValues, nextValue()));
    }

    private void readSlots(
        final int count, final List<Bytes32> slotKeys, final List<Bytes> slotValues)
        throws IOException {
      for (int i = 0; i < count; i++) {
        slotKeys.add(preimages.nextSlot().key());
        slotValues.add(Bytes.wrap(nextValue()));
      }
    }

    private byte[] nextValue() {
      final Entry entry = values.hasNext() ? values.next() : null;
      if (entry == null || ByteBuffer.wrap(entry.key()).getLong() != ordinal) {
        throw new IllegalStateException("no matched value for preimage entry #" + ordinal);
      }
      ordinal++;
      return entry.value();
    }
  }
}
