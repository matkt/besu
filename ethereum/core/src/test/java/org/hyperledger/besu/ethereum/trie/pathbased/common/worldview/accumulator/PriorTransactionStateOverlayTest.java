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
package org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.List;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class PriorTransactionStateOverlayTest {

  @Test
  void fromBlockAccessList_respectsBalIndex() {
    final Address a = Address.fromHexString("0x1000000000000000000000000000000000000001");
    final StorageSlotKey slot = new StorageSlotKey(UInt256.valueOf(1));
    final BlockAccessList bal =
        new BlockAccessList(
            List.of(
                new BlockAccessList.AccountChanges(
                    a,
                    List.of(
                        new BlockAccessList.SlotChanges(
                            slot,
                            List.of(
                                new BlockAccessList.StorageChange(0L, UInt256.valueOf(10)),
                                new BlockAccessList.StorageChange(2L, UInt256.valueOf(20))))),
                    List.of(),
                    List.of(new BlockAccessList.BalanceChange(0L, Wei.of(3))),
                    List.of(),
                    List.of())));

    final PriorTransactionStateOverlay forTx2 =
        PriorTransactionStateOverlay.fromBlockAccessList(bal, 2L);
    assertThat(forTx2.accountEntry(a)).isPresent();
    assertThat(forTx2.effectiveStorage(a, slot, UInt256.ZERO)).isEqualTo(UInt256.valueOf(10));

    final PriorTransactionStateOverlay forTx3 =
        PriorTransactionStateOverlay.fromBlockAccessList(bal, 3L);
    assertThat(forTx3.effectiveStorage(a, slot, UInt256.ZERO)).isEqualTo(UInt256.valueOf(20));
  }
}
