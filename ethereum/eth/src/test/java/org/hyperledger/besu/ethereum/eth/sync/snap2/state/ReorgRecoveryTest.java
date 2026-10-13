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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.TestWorld.hash;
import static org.hyperledger.besu.ethereum.eth.sync.snap2.TestWorld.slotHash;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.trie.RangeManager;

import java.util.List;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/** The A/B/C/D cases of a reorg below the pivot, per field and per slot. */
class ReorgRecoveryTest {

  private static final Address ALICE = Address.fromHexString("0xa1");
  private static final Address BOB = Address.fromHexString("0xb0");
  private static final Address CHARLIE = Address.fromHexString("0xc4");
  private static final Address DAVE = Address.fromHexString("0xda");
  private static final Address GRACE = Address.fromHexString("0x96");
  private static final Address CONTRACT = Address.fromHexString("0xcc");
  private static final UInt256 SLOT_BOTH = UInt256.valueOf(1);
  private static final UInt256 SLOT_ORPHANED = UInt256.valueOf(2);
  private static final UInt256 SLOT_OUTSIDE = UInt256.valueOf(3);

  @Test
  void refetchesWhatOnlyTheOrphanedForkChanged() {
    final StateCoverage coverage = new StateCoverage();
    coverage.accounts().add(RangeManager.MIN_RANGE, RangeManager.MAX_RANGE);
    // the contract's storage is downloaded up to SLOT_BOTH, whose hash is between the others
    final StorageTask task = new StorageTask(hash(CONTRACT), Bytes32.ZERO);
    task.cover(RangeManager.MIN_RANGE, slotHash(SLOT_BOTH), 0);
    coverage.addStorageTask(task);
    assertThat(task.covers(slotHash(SLOT_ORPHANED))).isTrue();
    assertThat(task.covers(slotHash(SLOT_OUTSIDE))).isFalse();

    final StateDiff orphaned = new StateDiff();
    orphaned.add(
        new BlockAccessList(
            List.of(
                // A: changed by both forks
                account(ALICE, List.of(new BalanceChange(1, Wei.of(50))), List.of(), List.of()),
                // B: nonce changed by the orphaned fork only, balance by both
                account(
                    CHARLIE,
                    List.of(new BalanceChange(1, Wei.of(3))),
                    List.of(new NonceChange(1, 7)),
                    List.of()),
                // B: changed by the orphaned fork only
                account(DAVE, List.of(new BalanceChange(1, Wei.of(60))), List.of(), List.of()),
                account(
                    CONTRACT,
                    List.of(),
                    List.of(),
                    List.of(slot(SLOT_BOTH, 1), slot(SLOT_ORPHANED, 2), slot(SLOT_OUTSIDE, 3))))));
    final StateDiff canonical = new StateDiff();
    canonical.add(
        new BlockAccessList(
            List.of(
                account(ALICE, List.of(new BalanceChange(1, Wei.of(80))), List.of(), List.of()),
                account(CHARLIE, List.of(new BalanceChange(1, Wei.of(4))), List.of(), List.of()),
                // C: changed by the canonical fork only
                account(GRACE, List.of(new BalanceChange(1, Wei.of(50))), List.of(), List.of()),
                account(CONTRACT, List.of(), List.of(), List.of(slot(SLOT_BOTH, 9))))));

    final ReorgRecovery.Plan plan = ReorgRecovery.plan(orphaned, canonical, coverage);

    assertThat(plan.accounts())
        .containsExactlyInAnyOrder(hash(CHARLIE), hash(DAVE), hash(CONTRACT));
    // the slot both forks wrote comes from the canonical lists, the one not downloaded yet from
    // the download
    assertThat(plan.slots()).containsOnlyKeys(hash(CONTRACT));
    assertThat(plan.slots().get(hash(CONTRACT))).containsExactly(slotHash(SLOT_ORPHANED));
  }

  @Test
  void marksTheStorageRootsLearnedOnTheOrphanedForkStale() {
    final StateCoverage coverage = new StateCoverage();
    coverage.accounts().add(RangeManager.MIN_RANGE, RangeManager.MAX_RANGE);
    final StorageTask written = new StorageTask(hash(CONTRACT), Bytes32.ZERO);
    final StorageTask untouched = new StorageTask(hash(BOB), Bytes32.ZERO);
    coverage.addStorageTask(written);
    coverage.addStorageTask(untouched);
    final StateDiff orphaned = new StateDiff();
    orphaned.add(
        new BlockAccessList(
            List.of(
                account(BOB, List.of(new BalanceChange(1, Wei.of(5))), List.of(), List.of()),
                account(CONTRACT, List.of(), List.of(), List.of(slot(SLOT_ORPHANED, 2))))));

    ReorgRecovery.markStaleRoots(orphaned, coverage);

    assertThat(written.isRootStale()).isTrue();
    assertThat(untouched.isRootStale()).isFalse();
  }

  @Test
  void ignoresWhatIsNotDownloadedYet() {
    final StateCoverage coverage = new StateCoverage();
    final StateDiff orphaned = new StateDiff();
    orphaned.add(
        new BlockAccessList(
            List.of(
                account(DAVE, List.of(new BalanceChange(1, Wei.of(60))), List.of(), List.of()))));

    final ReorgRecovery.Plan plan = ReorgRecovery.plan(orphaned, new StateDiff(), coverage);

    assertThat(plan.accounts()).isEmpty();
    assertThat(plan.slots()).isEmpty();
  }

  private static AccountChanges account(
      final Address address,
      final List<BalanceChange> balances,
      final List<NonceChange> nonces,
      final List<SlotChanges> slots) {
    return new AccountChanges(address, slots, List.of(), balances, nonces, List.of());
  }

  private static SlotChanges slot(final UInt256 key, final long value) {
    return new SlotChanges(
        new StorageSlotKey(key), List.of(new StorageChange(1, UInt256.valueOf(value))));
  }
}
