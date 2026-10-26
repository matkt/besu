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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogLayer;
import org.hyperledger.besu.plugin.services.trielogs.TrieLog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Blocks 0 to {@code last}, with their trie logs and BALs, as the conversion reads them. */
final class TestChain implements SelfConversion.Chain {
  private final List<BlockHeader> headers = new ArrayList<>();
  private final Map<Long, TrieLogLayer> trieLogs = new HashMap<>();
  private final Map<Long, List<AccountChanges>> bals = new HashMap<>();

  TestChain(final int last) {
    for (int number = 0; number <= last; number++) {
      final BlockHeaderTestFixture header = new BlockHeaderTestFixture().number(number);
      if (number > 0) {
        header.parentHash(headers.getLast().getHash());
      }
      headers.add(header.buildHeader());
    }
  }

  BlockHeader get(final int number) {
    return headers.get(number);
  }

  TrieLogLayer trieLogOf(final int number) {
    return trieLogs.computeIfAbsent((long) number, n -> new TrieLogLayer());
  }

  void bal(final int number, final AccountChanges... accounts) {
    bals.put((long) number, List.of(accounts));
  }

  @Override
  public Optional<BlockHeader> header(final Hash blockHash) {
    return headers.stream().filter(header -> header.getHash().equals(blockHash)).findFirst();
  }

  @Override
  public BlockHeader canonicalHeader(final long number) {
    return headers.get((int) number);
  }

  @Override
  public Optional<TrieLog> trieLog(final Hash blockHash) {
    return header(blockHash).map(header -> trieLogOf((int) header.getNumber()));
  }

  @Override
  public List<BlockAccessList> bals(final List<BlockHeader> blocks) {
    return blocks.stream()
        .map(header -> new BlockAccessList(bals.getOrDefault(header.getNumber(), List.of())))
        .toList();
  }
}
