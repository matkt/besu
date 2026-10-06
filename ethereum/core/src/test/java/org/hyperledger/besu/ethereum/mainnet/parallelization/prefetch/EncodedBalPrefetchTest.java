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
package org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig.createStatefulConfigWithTrie;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** A prefetch read from the encoding of a block access list, lot by lot as the list is read. */
class EncodedBalPrefetchTest {

  private static final Executor SYNC_EXECUTOR = Runnable::run;
  private static final int ACCOUNTS = 4;
  private static final StorageSlotKey CHANGED_SLOT = new StorageSlotKey(UInt256.valueOf(7));
  private static final StorageSlotKey READ_SLOT = new StorageSlotKey(UInt256.valueOf(9));

  private BonsaiWorldStateKeyValueStorage storage;
  private BonsaiWorldState worldState;

  @BeforeEach
  void setUp() {
    storage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            ImmutableDataStorageConfiguration.builder()
                .dataStorageFormat(DataStorageFormat.BONSAI)
                .extraStorageConfiguration(
                    ImmutableExtraStorageConfiguration.builder()
                        .unstable(
                            ImmutableExtraStorageConfiguration.Unstable.builder()
                                .bonsaiCrossBlockCacheEnabled(true)
                                .build())
                        .build())
                .build());
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    for (int i = 0; i < ACCOUNTS; i++) {
      updater.putAccountInfoState(address(i).addressHash(), Bytes.of(i + 1));
      updater.putStorageValueBySlotHash(
          address(i).addressHash(), CHANGED_SLOT.getSlotHash(), Bytes.of(10 + i));
      updater.putStorageValueBySlotHash(
          address(i).addressHash(), READ_SLOT.getSlotHash(), Bytes.of(20 + i));
    }
    updater.commit();
    // committing caches what it writes: start from a cold cache
    storage.getCacheManager().clear(ACCOUNT_INFO_STATE);
    storage.getCacheManager().clear(ACCOUNT_STORAGE_STORAGE);
    worldState =
        new BonsaiWorldState(
            storage,
            new NoOpBonsaiCachedMerkleTrieLoader(),
            new NoOpBonsaiWorldStateCacheManager(
                storage, EvmConfiguration.DEFAULT, new BonsaiCodeCache()),
            new NoOpTrieLogManager(),
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new BonsaiCodeCache());
  }

  @AfterEach
  void tearDown() throws Exception {
    if (storage.getCacheManager() instanceof final Closeable closeable) {
      closeable.close();
    }
    storage.close();
  }

  @Test
  void readsEveryAccountAndStorageSlotOfTheEncodedList() {
    final BalPrefetch prefetch = new BalPrefetch();

    prefetchEncoded(blockAccessList().encode(), Long.MAX_VALUE, prefetch);

    for (int i = 0; i < ACCOUNTS; i++) {
      assertThat(isAccountCached(i)).isTrue();
      assertThat(isSlotCached(i, CHANGED_SLOT)).isTrue();
      assertThat(isSlotCached(i, READ_SLOT)).isTrue();
    }
    assertThat(prefetch.isCancelled()).isFalse();
  }

  @Test
  void isForTheListDecodedFromTheSameEncoding() {
    final BalPrefetch prefetch = new BalPrefetch();
    final Bytes encoded = blockAccessList().encode();
    // it did not get the encoding yet
    assertThat(prefetch.isFor(BlockAccessList.fromBytes(encoded))).isFalse();

    prefetchEncoded(encoded, Long.MAX_VALUE, prefetch);

    assertThat(prefetch.isFor(BlockAccessList.fromBytes(encoded))).isTrue();
    assertThat(prefetch.isFor(BlockAccessList.fromBytes(blockAccessList(1).encode()))).isFalse();
  }

  @Test
  void stopsAtTheItemBudget() {
    final BalPrefetch prefetch = new BalPrefetch();

    // an account and its two slots are 3 items: the second account is over a budget of 4
    prefetchEncoded(blockAccessList().encode(), 4, prefetch);

    assertThat(isAccountCached(0)).isTrue();
    for (int i = 1; i < ACCOUNTS; i++) {
      assertThat(isAccountCached(i)).isFalse();
    }
    assertThat(prefetch.isCancelled()).isTrue();
  }

  @Test
  void stopsAtAMalformedEncoding() {
    final BalPrefetch prefetch = new BalPrefetch();

    prefetchEncoded(Bytes.fromHexString("0xdeadbeef"), Long.MAX_VALUE, prefetch);

    for (int i = 0; i < ACCOUNTS; i++) {
      assertThat(isAccountCached(i)).isFalse();
    }
    assertThat(prefetch.isCancelled()).isTrue();
  }

  @Test
  void stopsAtAMalformedAccount() {
    final BalPrefetch prefetch = new BalPrefetch();
    final List<Bytes> accounts = new ArrayList<>();
    for (int i = 0; i < ACCOUNTS; i++) {
      accounts.add(accountEncoding(i));
    }
    accounts.set(1, accountWithA19ByteAddress());

    prefetchEncoded(listOf(accounts), Long.MAX_VALUE, prefetch);

    assertThat(isAccountCached(0)).isTrue();
    assertThat(isAccountCached(2)).isFalse();
    assertThat(isAccountCached(3)).isFalse();
    assertThat(prefetch.isCancelled()).isTrue();
  }

  @Test
  void aCancelledPrefetchReadsNothing() {
    final BalPrefetch prefetch = new BalPrefetch();
    prefetch.cancel();

    prefetchEncoded(blockAccessList().encode(), Long.MAX_VALUE, prefetch);

    for (int i = 0; i < ACCOUNTS; i++) {
      assertThat(isAccountCached(i)).isFalse();
    }
  }

  /** In lots of one account, read in order. */
  private void prefetchEncoded(
      final Bytes encoded, final long maxItems, final BalPrefetch prefetch) {
    new BalPrefetcher(true, 1)
        .prefetchEncoded(
            worldState, () -> encoded, maxItems, SYNC_EXECUTOR, SYNC_EXECUTOR, prefetch)
        .join();
  }

  private static BlockAccessList blockAccessList() {
    return blockAccessList(0);
  }

  private static BlockAccessList blockAccessList(final long txIndex) {
    final List<BlockAccessList.AccountChanges> accounts = new ArrayList<>();
    for (int i = 0; i < ACCOUNTS; i++) {
      accounts.add(accountChanges(i, txIndex));
    }
    return new BlockAccessList(accounts);
  }

  private static BlockAccessList.AccountChanges accountChanges(final int i, final long txIndex) {
    return new BlockAccessList.AccountChanges(
        address(i),
        List.of(
            new BlockAccessList.SlotChanges(
                CHANGED_SLOT,
                List.of(new BlockAccessList.StorageChange(txIndex, UInt256.valueOf(1))))),
        List.of(new BlockAccessList.SlotRead(READ_SLOT)),
        List.of(new BlockAccessList.BalanceChange(txIndex, Wei.ONE)),
        List.of(),
        List.of());
  }

  /** The encoding of the account at index {@code i}, as an entry of a block access list. */
  private static Bytes accountEncoding(final int i) {
    final RLPInput list = RLP.input(new BlockAccessList(List.of(accountChanges(i, 0))).encode());
    list.enterList();
    return list.readAsRlp().raw();
  }

  private static Bytes accountWithA19ByteAddress() {
    return RLP.encode(
        out -> {
          out.startList();
          out.writeBytes(Bytes.repeat((byte) 1, Address.SIZE - 1));
          for (int changes = 0; changes < 5; changes++) {
            out.startList();
            out.endList();
          }
          out.endList();
        });
  }

  private static Bytes listOf(final List<Bytes> accounts) {
    return RLP.encode(
        out -> {
          out.startList();
          accounts.forEach(out::writeRaw);
          out.endList();
        });
  }

  private boolean isAccountCached(final int i) {
    return storage.isCached(ACCOUNT_INFO_STATE, address(i).addressHash().getBytes());
  }

  private boolean isSlotCached(final int i, final StorageSlotKey slot) {
    return storage.isCached(
        ACCOUNT_STORAGE_STORAGE,
        Bytes.concatenate(address(i).addressHash().getBytes(), slot.getSlotHash().getBytes()));
  }

  private static Address address(final int i) {
    return Address.fromHexString(String.format("0x%040x", i + 1));
  }
}
