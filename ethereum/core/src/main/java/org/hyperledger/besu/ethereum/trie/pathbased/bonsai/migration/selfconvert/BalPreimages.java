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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListChanges.AccountFinalChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListChanges.StorageFinalChange;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter.Entry;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.selfconvert.StateLeaves.Known;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.stream.LongStream;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Preimages of the keys written after the anchor, up to the target, from the blocks' BALs. They
 * cover every key created after the anchor, which the preimage file lacks.
 */
final class BalPreimages {

  /** Blocks whose BALs are asked for together. */
  private static final int BLOCKS_PER_REQUEST = 128;

  private BalPreimages() {}

  /** Adds the preimages of the blocks after {@code anchor}, up to {@code target}, to the sorter. */
  static void collect(
      final BlockHeader anchor,
      final BlockHeader target,
      final SelfConversion.Chain chain,
      final Eip8347ExternalSorter sorter)
      throws IOException {
    for (long first = anchor.getNumber() + 1;
        first <= target.getNumber();
        first += BLOCKS_PER_REQUEST) {
      // the node is closing
      if (Thread.currentThread().isInterrupted()) {
        throw new CancellationException("BAL preimages interrupted");
      }
      final List<BlockHeader> blocks =
          LongStream.rangeClosed(
                  first, Math.min(target.getNumber(), first + BLOCKS_PER_REQUEST - 1))
              .mapToObj(chain::canonicalHeader)
              .toList();
      for (final BlockAccessList bal : chain.bals(blocks)) {
        add(bal, sorter);
      }
    }
  }

  private static void add(final BlockAccessList bal, final Eip8347ExternalSorter sorter)
      throws IOException {
    for (final AccountFinalChanges changes : BlockAccessListChanges.latestChanges(bal)) {
      if (changes.isEmpty()) {
        continue;
      }
      final byte[] accountKey = FlatState.account(changes.address());
      sorter.add(accountKey, changes.address().getBytes().toArrayUnsafe());
      for (final StorageFinalChange slot : changes.storageChanges()) {
        if (!slot.value().isZero()) {
          sorter.add(
              FlatState.slot(accountKey, slot.slot().getSlotHash()),
              slot.slot().getSlotKey().orElseThrow().toArrayUnsafe());
        }
      }
    }
  }

  /**
   * Joins the keys missing from the preimage file with the BAL preimages, both sorted by flat key.
   * Fails on a key without preimage.
   */
  static Iterator<Known> join(final Iterator<Entry> misses, final Iterator<Entry> preimages) {
    return new Join(misses, preimages);
  }

  private static final class Join implements Iterator<Known> {
    private final Iterator<Entry> misses;
    private final PeekingIterator<Entry> preimages;

    /** The last account preimage passed: the address of the slots that follow. */
    private byte[] accountKey;

    private Address address;

    Join(final Iterator<Entry> misses, final Iterator<Entry> preimages) {
      this.misses = misses;
      this.preimages = Iterators.peekingIterator(preimages);
    }

    @Override
    public boolean hasNext() {
      return misses.hasNext();
    }

    @Override
    public Known next() {
      final Entry miss = misses.next();
      final byte[] preimage = preimageOf(miss.key());
      if (FlatState.isAccount(miss.key())) {
        return new Known(Address.wrap(Bytes.wrap(preimage)), null, miss.value());
      }
      if (accountKey == null || !FlatState.isSlotOf(miss.key(), accountKey)) {
        throw missing(miss.key());
      }
      return new Known(address, Bytes32.wrap(preimage), miss.value());
    }

    /** The preimage of {@code key}, passing those before it. */
    private byte[] preimageOf(final byte[] key) {
      while (preimages.hasNext() && Arrays.compareUnsigned(preimages.peek().key(), key) <= 0) {
        final Entry preimage = preimages.next();
        if (FlatState.isAccount(preimage.key())) {
          accountKey = preimage.key();
          address = Address.wrap(Bytes.wrap(preimage.value()));
        }
        if (Arrays.equals(preimage.key(), key)) {
          return preimage.value();
        }
      }
      throw missing(key);
    }

    private static Eip8347ArtifactVerificationException missing(final byte[] key) {
      return new Eip8347ArtifactVerificationException(
          "no preimage for flat key "
              + Bytes.wrap(key).toHexString()
              + ": neither the preimage file nor the BALs after its anchor have it");
    }
  }
}
