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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter.Entry;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogLayer;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/** Entries read after the target, block 1, put back with the trie logs of blocks 2 and 3. */
class TargetPriorsTest {

  private static final Address ACCOUNT = Address.fromHexString("0x01");
  private static final byte[] ACCOUNT_KEY = FlatState.account(ACCOUNT);
  private static final int RANGE = FlatState.rangeOf(ACCOUNT_KEY);

  /** Slots of the account, in key order. */
  private static final List<UInt256> SLOTS =
      IntStream.range(0, 4)
          .mapToObj(UInt256::valueOf)
          .sorted(Comparator.comparing(TargetPriorsTest::key, Arrays::compareUnsigned))
          .toList();

  private final TestChain chain = new TestChain(3);
  private final TargetPriors priors = priors(TargetPriors.DEFAULT_MAX_PRIORS);

  @Test
  void aKeyChangedTwiceGetsItsValueBeforeItsFirstChange() {
    change(2, 0, 10, 11);
    change(3, 0, 11, 12);

    priors.advanceTo(chain.get(3).getHash());

    assertThat(pairs(priors.atTarget(RANGE, List.of(slot(0, 12)))))
        .isEqualTo(pairs(List.of(slot(0, 10))));
  }

  @Test
  void aKeyDeletedSinceComesBackAndOneCreatedSinceIsLeftOut() {
    change(2, 0, 10, 0);
    change(2, 1, 0, 11);
    change(2, 3, 13, 0);

    priors.advanceTo(chain.get(2).getHash());

    assertThat(pairs(priors.atTarget(RANGE, List.of(slot(1, 11)))))
        .isEqualTo(pairs(List.of(slot(0, 10))));
    // past the range's last batch
    assertThat(pairs(priors.rest(RANGE))).isEqualTo(pairs(List.of(slot(3, 13))));
  }

  @Test
  void aChangeToAKeyTheRangeHasReadIsDropped() {
    priors.advanceTo(chain.get(1).getHash());
    assertThat(pairs(priors.atTarget(RANGE, List.of(slot(0, 10)))))
        .isEqualTo(pairs(List.of(slot(0, 10))));
    change(2, 0, 10, 0);
    change(2, 2, 12, 22);

    priors.advanceTo(chain.get(2).getHash());

    assertThat(pairs(priors.atTarget(RANGE, List.of(slot(1, 11), slot(2, 22)))))
        .isEqualTo(pairs(List.of(slot(1, 11), slot(2, 12))));
    assertThat(priors.rest(RANGE)).isEmpty();
  }

  @Test
  void aBatchReadOnAnotherForkIsPutBackToo() {
    change(2, 0, 10, 11);
    // a sibling of block 2, which changes slot 1
    final BlockHeader sibling =
        new BlockHeaderTestFixture()
            .number(2)
            .timestamp(99)
            .parentHash(chain.get(1).getHash())
            .buildHeader();
    final TrieLogLayer siblingChanges = new TrieLogLayer();
    siblingChanges.addStorageChange(
        ACCOUNT, new StorageSlotKey(SLOTS.get(1)), UInt256.valueOf(11), UInt256.valueOf(21));
    final TargetPriors forked =
        new TargetPriors(
            chain.get(1),
            hash -> hash.equals(sibling.getHash()) ? Optional.of(sibling) : chain.header(hash),
            hash ->
                hash.equals(sibling.getHash()) ? Optional.of(siblingChanges) : chain.trieLog(hash),
            TargetPriors.DEFAULT_MAX_PRIORS);

    forked.advanceTo(chain.get(2).getHash());
    assertThat(pairs(forked.atTarget(RANGE, List.of(slot(0, 11)))))
        .isEqualTo(pairs(List.of(slot(0, 10))));
    // the flat database moved to the sibling
    forked.advanceTo(sibling.getHash());
    assertThat(pairs(forked.atTarget(RANGE, List.of(slot(1, 21), slot(2, 12)))))
        .isEqualTo(pairs(List.of(slot(1, 11), slot(2, 12))));
  }

  @Test
  void aBlockNotDescendingFromTheTargetIsRejected() {
    final BlockHeader fork =
        new BlockHeaderTestFixture().number(2).parentHash(chain.get(0).getHash()).buildHeader();
    final TargetPriors forked =
        new TargetPriors(
            chain.get(1),
            hash -> hash.equals(fork.getHash()) ? Optional.of(fork) : chain.header(hash),
            chain::trieLog,
            TargetPriors.DEFAULT_MAX_PRIORS);

    assertThatThrownBy(() -> forked.advanceTo(fork.getHash()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("block 0 does not descend from block 1");
  }

  @Test
  void tooManyKeysChangedSinceTheTargetStopTheConversion() {
    change(2, 0, 10, 11);
    change(2, 1, 11, 12);

    assertThatThrownBy(() -> priors(1).advanceTo(chain.get(2).getHash()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("more than 1 keys changed since block 1 are still to be read");
  }

  private TargetPriors priors(final int maxPriors) {
    return new TargetPriors(chain.get(1), chain::header, chain::trieLog, maxPriors);
  }

  /** A change of the slot at {@code index} in {@code block}'s trie log; 0 is no value. */
  private void change(final int block, final int index, final long prior, final long updated) {
    chain
        .trieLogOf(block)
        .addStorageChange(
            ACCOUNT,
            new StorageSlotKey(SLOTS.get(index)),
            prior == 0 ? null : UInt256.valueOf(prior),
            updated == 0 ? null : UInt256.valueOf(updated));
  }

  /** The slot at {@code index} as the flat database stores it. */
  private static Entry slot(final int index, final long value) {
    return new Entry(key(SLOTS.get(index)), UInt256.valueOf(value).toArrayUnsafe());
  }

  private static byte[] key(final UInt256 slot) {
    return FlatState.slot(ACCOUNT_KEY, Hash.hash(Bytes32.leftPad(slot)));
  }

  private static List<Map.Entry<Bytes, Bytes>> pairs(final List<Entry> entries) {
    return entries.stream()
        .map(entry -> Map.entry(Bytes.wrap(entry.key()), Bytes.wrap(entry.value())))
        .toList();
  }
}
