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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

class BlockBodyTest {

  @Test
  void transactionsRootIsTheRootOfItsTransactionsComputedOnce() {
    final List<Transaction> transactions =
        new ArrayList<>(new BlockDataGenerator().transactionsWithAllTypes());
    final BlockBody body = new BlockBody(transactions, Collections.emptyList());

    assertThat(body.getTransactionsRoot()).isEqualTo(BodyValidation.transactionsRoot(transactions));
    // the header built from the body and the block validation read the same root
    assertThat(body.getTransactionsRoot()).isSameAs(body.getTransactionsRoot());
  }

  @Test
  void transactionsRootOfAnEmptyBodyIsTheEmptyTrieRoot() {
    assertThat(BlockBody.empty().getTransactionsRoot()).isEqualTo(Hash.EMPTY_TRIE_HASH);
  }
}
