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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile.AccountPreimages;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

/**
 * The self-conversion on RocksDB while the node imports blocks: the flat database is on a branch
 * that won a reorg, then a second reorg follows.
 */
class PbtSelfConversionReorgTest {

  /** The PBT fork is never reached here. */
  private static final long PBT_TIME = Long.MAX_VALUE;

  private static final DataStorageConfiguration CONFIG =
      DataStorageConfiguration.DEFAULT_BONSAI_CONFIG;
  private static final Bytes CODE = Bytes.fromHexString("0x" + "60016001".repeat(40));
  private static final Bytes OTHER_CODE = Bytes.fromHexString("0x" + "60026002".repeat(20));

  @TempDir Path tmp;

  private final Map<Hash, BlockHeader> headers = new HashMap<>();
  private final Map<Hash, BlockAccessList> bals = new HashMap<>();
  private final Map<Hash, Map<Address, Account>> states = new HashMap<>();
  private final List<BlockHeader> canonical = new ArrayList<>();
  private final Blockchain blockchain = mock(Blockchain.class);
  private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
  private final AtomicLong now = new AtomicLong();
  private RocksDbWorldStates databases;
  private BonsaiWorldStateKeyValueStorage storage;
  private BonsaiWorldStateProvider provider;
  private BlockHeader finalized;
  private PbtMigrator migrator;

  /** An account as the test keeps it, to write preimages and to rebuild a state from scratch. */
  private record Account(long nonce, Wei balance, Bytes code, TreeMap<UInt256, UInt256> slots) {
    Account copy() {
      return new Account(nonce, balance, code, new TreeMap<>(slots));
    }
  }

  @BeforeEach
  void startChain() throws Exception {
    databases = new RocksDbWorldStates(tmp);
    storage = RocksDbWorldStates.worldStateStorage(databases.database("node"));
    storage.upgradeToFullFlatDbMode();
    when(blockchain.getChainHeadHeader())
        .thenAnswer(
            i ->
                canonical.isEmpty()
                    ? new BlockHeaderTestFixture().number(0).timestamp(0).buildHeader()
                    : canonical.getLast());
    when(blockchain.getBlockHeader(any(Hash.class)))
        .thenAnswer(i -> Optional.ofNullable(headers.get(i.<Hash>getArgument(0))));
    when(blockchain.getBlockHeader(anyLong())).thenAnswer(i -> canonical(i.getArgument(0)));
    when(blockchain.getBlockHashByNumber(anyLong()))
        .thenAnswer(i -> canonical(i.getArgument(0)).map(BlockHeader::getBlockHash));
    when(blockchain.getBlockAccessList(any(Hash.class)))
        .thenAnswer(i -> Optional.ofNullable(bals.get(i.<Hash>getArgument(0))));
    when(blockchain.getFinalized())
        .thenAnswer(i -> Optional.ofNullable(finalized).map(BlockHeader::getBlockHash));
    when(scheduler.awaitTermination(anyLong(), any())).thenReturn(true);
    provider = newProvider(storage, blockchain, PBT_TIME);
  }

  @AfterEach
  void close() throws Exception {
    if (migrator != null) {
      migrator.close();
    }
    databases.close();
  }

  @Test
  void convertsTheFinalizedStateThenFollowsBothReorgs() throws Exception {
    // Block 1 is the preimage file's anchor; block 3 is finalized.
    final BlockHeader genesis =
        block(
            null,
            0,
            balance(1, 100),
            balance(2, 200),
            balance(3, 300),
            code(10, CODE),
            slot(10, 1, 11),
            slot(10, 2, 12),
            code(11, CODE),
            slot(11, 1, 21),
            code(12, Bytes.concatenate(Bytes.fromHexString("0xef0100"), address(1).getBytes())));
    final BlockHeader anchor = block(genesis, 0, balance(1, 101), slot(10, 3, 13));
    final Path preimages = preimagesOf(anchor);
    // created after the anchor: an account, a contract with a new code, slots
    final BlockHeader b2 =
        block(anchor, 0, balance(4, 400), code(13, OTHER_CODE), slot(13, 7, 77), slot(11, 9, 99));
    finalized = block(b2, 0, balance(2, 201), slot(10, 1, 0), slot(10, 4, 14));
    final BlockHeader b4 = block(finalized, 0, balance(3, 301), slot(10, 2, 22), delete(4));
    // the sibling of block 4 wins: the flat database rolls back to block 3, then onto it
    final BlockHeader sibling4 =
        block(finalized, 100, balance(5, 500), slot(10, 2, 32), slot(13, 7, 0), delete(2));
    final BlockHeader sibling5 = block(sibling4, 100, slot(11, 9, 98), balance(1, 102));

    final Runnable tick = startMigrator(anchor, preimages);
    tick();
    tick.run();

    assertThat(storage.getWorldStateBlockHash(TrieBranchType.BINARY))
        .contains(sibling5.getBlockHash());
    assertThat(binaryRoot()).isEqualTo(binaryRootFromScratch(states.get(sibling5.getHash())));

    // block 4's branch wins back: the migrator rolls back to block 3, then forward
    final BlockHeader b5 = block(b4, 0, balance(6, 600), slot(13, 8, 88));
    final BlockHeader b6 = block(b5, 0, code(14, CODE), balance(14, 1));
    tick();
    tick.run();

    assertThat(storage.getWorldStateBlockHash(TrieBranchType.BINARY)).contains(b6.getBlockHash());
    assertThat(binaryRoot()).isEqualTo(binaryRootFromScratch(states.get(b6.getHash())));
  }

  /** A self-converting migrator; returns its scheduled tick. */
  private Runnable startMigrator(final BlockHeader anchor, final Path preimages) {
    migrator =
        new PbtMigrator(
            provider,
            blockchain,
            Optional.of(PBT_TIME),
            scheduler,
            Stream::empty,
            Optional.of(
                PbtMigrator.SnapshotBootstrap.selfConversion(
                    preimages, anchor.getBlockHash(), PbtMigrator.SnapshotBootstrap.workDir(tmp))),
            false,
            blocks -> Map.of(),
            1,
            new PbtMigrator.Tuning(2, 128, Optional.of(Duration.ZERO), 1 << 10),
            Duration.ZERO,
            now::get);
    migrator.start();
    final ArgumentCaptor<Runnable> tick = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler)
        .scheduleWithFixedDelay(tick.capture(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
    return tick.getValue();
  }

  /** The clock at the head's time: the node follows the chain. */
  private void tick() {
    now.set(canonical.getLast().getTimestamp() * 1000);
  }

  // ---- Blocks ----

  private record Change(Address address, Wei balance, Bytes code, UInt256 slot, UInt256 value) {}

  private static Change balance(final int account, final long wei) {
    return new Change(address(account), Wei.of(wei), null, null, null);
  }

  private static Change code(final int account, final Bytes code) {
    return new Change(address(account), null, code, null, null);
  }

  private static Change slot(final int account, final long slot, final long value) {
    return new Change(address(account), null, null, UInt256.valueOf(slot), UInt256.valueOf(value));
  }

  private static Change delete(final int account) {
    return new Change(address(account), null, null, null, null);
  }

  /**
   * The child of {@code parent} (genesis when null) with {@code changes}, persisted as the head.
   * {@code branch} gives siblings different timestamps.
   */
  private BlockHeader block(final BlockHeader parent, final long branch, final Change... changes) {
    final BonsaiWorldState worldState =
        parent == null ? (BonsaiWorldState) provider.getWorldState() : stateOf(parent);
    final Map<Address, Account> state = new HashMap<>();
    if (parent != null) {
      states
          .get(parent.getHash())
          .forEach((address, account) -> state.put(address, account.copy()));
    }
    final WorldUpdater updater = worldState.updater();
    for (final Change change : changes) {
      apply(change, updater, state);
    }
    updater.commit();
    final long number = parent == null ? 0 : parent.getNumber() + 1;
    final BlockHeader header =
        new BlockHeaderTestFixture()
            .number(number)
            .parentHash(parent == null ? Hash.ZERO : parent.getHash())
            .timestamp(number + branch)
            .stateRoot(worldState.frontierRootHash())
            .buildHeader();
    worldState.persist(header);
    headers.put(header.getHash(), header);
    states.put(header.getHash(), state);
    bals.put(header.getHash(), balOf(changes));
    while (canonical.size() > number) {
      canonical.removeLast();
    }
    canonical.add(header);
    return header;
  }

  private static void apply(
      final Change change, final WorldUpdater updater, final Map<Address, Account> state) {
    if (change.balance() == null && change.code() == null && change.slot() == null) {
      updater.deleteAccount(change.address());
      state.remove(change.address());
      return;
    }
    final Account account =
        state.computeIfAbsent(
            change.address(), a -> new Account(0, Wei.ZERO, Bytes.EMPTY, new TreeMap<>()));
    if (change.balance() != null) {
      updater.getOrCreate(change.address()).setBalance(change.balance());
      state.put(
          change.address(),
          new Account(account.nonce(), change.balance(), account.code(), account.slots()));
    } else if (change.code() != null) {
      updater.getOrCreate(change.address()).setCode(change.code());
      state.put(
          change.address(),
          new Account(account.nonce(), account.balance(), change.code(), account.slots()));
    } else {
      updater.getOrCreate(change.address()).setStorageValue(change.slot(), change.value());
      if (change.value().isZero()) {
        account.slots().remove(change.slot());
      } else {
        account.slots().put(change.slot(), change.value());
      }
    }
  }

  /** The BAL of a block: what it writes, as EIP-7928 records it. */
  private static BlockAccessList balOf(final Change... changes) {
    final Map<Address, AccountChanges> accounts = new LinkedHashMap<>();
    for (final Change change : changes) {
      final AccountChanges written =
          new AccountChanges(
              change.address(),
              change.slot() == null
                  ? List.of()
                  : List.of(
                      new SlotChanges(
                          new StorageSlotKey(change.slot()),
                          List.of(new StorageChange(0, change.value())))),
              List.of(),
              change.balance() == null && change.slot() == null && change.code() == null
                  ? List.of(new BalanceChange(0, Wei.ZERO))
                  : change.balance() == null
                      ? List.of()
                      : List.of(new BalanceChange(0, change.balance())),
              List.of(),
              change.code() == null ? List.of() : List.of(new CodeChange(0, change.code())));
      accounts.merge(
          change.address(),
          written,
          (first, second) ->
              new AccountChanges(
                  first.address(),
                  concat(first.storageChanges(), second.storageChanges()),
                  List.of(),
                  concat(first.balanceChanges(), second.balanceChanges()),
                  List.of(),
                  concat(first.codeChanges(), second.codeChanges())));
    }
    return new BlockAccessList(List.copyOf(accounts.values()));
  }

  private static <T> List<T> concat(final List<T> first, final List<T> second) {
    final List<T> all = new ArrayList<>(first);
    all.addAll(second);
    return all;
  }

  /** The state after {@code parent}, the head moved there as a forkchoice update moves it. */
  private BonsaiWorldState stateOf(final BlockHeader parent) {
    return (BonsaiWorldState)
        provider
            .getWorldState(
                WorldStateQueryParams.newBuilder()
                    .withBlockHeader(parent)
                    .withShouldWorldStateUpdateHead(true)
                    .withTimeStamp(parent.getTimestamp() + 1)
                    .build())
            .orElseThrow();
  }

  private Optional<BlockHeader> canonical(final long number) {
    return number < canonical.size() ? Optional.of(canonical.get((int) number)) : Optional.empty();
  }

  // ---- Preimages and roots ----

  private Path preimagesOf(final BlockHeader block) throws Exception {
    final List<AccountPreimages> accounts = new ArrayList<>();
    states
        .get(block.getHash())
        .forEach(
            (address, account) ->
                accounts.add(
                    new AccountPreimages(
                        address,
                        account.slots().keySet().stream().map(Bytes32::leftPad).toList())));
    final Path file = tmp.resolve("preimages");
    Eip8347PreimageFile.write(file, accounts);
    return file;
  }

  private Hash binaryRoot() {
    return storage
        .getWorldStateRootHash(TrieBranchType.BINARY)
        .map(root -> Hash.wrap(Bytes32.wrap(root)))
        .orElseThrow();
  }

  /** The binary root of {@code state}, on a trie binary from genesis. */
  private static Hash binaryRootFromScratch(final Map<Address, Account> state) {
    final Blockchain other = mock(Blockchain.class);
    when(other.getChainHeadHeader())
        .thenReturn(new BlockHeaderTestFixture().number(0).timestamp(0).buildHeader());
    final BonsaiWorldState worldState =
        (BonsaiWorldState)
            newProvider(
                    (BonsaiWorldStateKeyValueStorage)
                        new KeyValueStorageProvider(
                                segments -> new SegmentedInMemoryKeyValueStorage(),
                                new InMemoryKeyValueStorage(),
                                new NoOpMetricsSystem())
                            .createWorldStateStorage(CONFIG),
                    other,
                    0L)
                .getWorldState();
    final WorldUpdater updater = worldState.updater();
    state.forEach(
        (address, account) -> {
          final var created = updater.createAccount(address, account.nonce(), account.balance());
          if (!account.code().isEmpty()) {
            created.setCode(account.code());
          }
          account.slots().forEach(created::setStorageValue);
        });
    updater.commit();
    return worldState.frontierRootHash();
  }

  private static BonsaiWorldStateProvider newProvider(
      final BonsaiWorldStateKeyValueStorage storage,
      final Blockchain blockchain,
      final long pbtTime) {
    return new BonsaiWorldStateProvider(
        storage,
        blockchain,
        CONFIG.getExtraStorageConfiguration(),
        new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
        null,
        EvmConfiguration.DEFAULT,
        new BonsaiCodeCache(),
        Optional.empty(),
        Optional.of(pbtTime));
  }

  private static Address address(final int index) {
    return Address.fromHexString(String.format("0x%040x", 0x1000 + index));
  }
}
