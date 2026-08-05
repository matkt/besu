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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.snap.SnapTestServing;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedAccountRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DownloadedStorageRangeTracker;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncMetricsManager;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.context.SnapSyncStatePersistenceManager;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;
import org.hyperledger.besu.testutil.TestClock;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the snap/2 reorg path in {@link SnapV2WorldDownloadState#startPivotCatchup},
 * which drives {@code finishPivotCatchup} → {@link SnapV2ReorgHealer#recoverFromReorg}.
 *
 * <p>These tests exercise the production wiring deterministically: no Engine API, no throttled snap
 * pipelines, no log-string polling. State setup mirrors {@link SnapV2ReorgHealerRecoveryTest}; peer
 * fetches are served from a canonical Bonsai world state via {@link SnapTestServing}.
 *
 * <p>Queue purging for deleted accounts is covered separately by {@link SnapV2ReorgQueuePurgeTest}.
 *
 * <p>See {@code ethereum/eth/docs/snap-v2-reorg-testing-proposal.md}.
 */
class SnapV2WorldDownloadStateReorgIntegrationTest {

  private static final Address ALICE =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address DAVE =
      Address.fromHexString("0x4444444444444444444444444444444444444444");
  private static final Address FRANK =
      Address.fromHexString("0x6666666666666666666666666666666666666666");
  private static final Address GRACE =
      Address.fromHexString("0x7777777777777777777777777777777777777777");
  private static final Address NEW_CONTRACT =
      Address.fromHexString("0x9999999999999999999999999999999999999999");

  private static final UInt256 S1 = UInt256.valueOf(1);
  private static final UInt256 S3 = UInt256.valueOf(3);
  private static final UInt256 SN = UInt256.valueOf(42);
  private static final Bytes NC_CODE = Bytes.fromHexString("0x60806040523480156010");

  private static final Bytes32 MAX_KEY =
      Bytes32.fromHexString("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

  private final BonsaiWorldStateKeyValueStorage localStorage = newBonsaiStorage();
  private final WorldStateStorageCoordinator localCoordinator =
      new WorldStateStorageCoordinator(localStorage);
  private final BonsaiWorldStateKeyValueStorage canonicalStorage = newBonsaiStorage();
  private final WorldStateStorageCoordinator canonicalCoordinator =
      new WorldStateStorageCoordinator(canonicalStorage);

  private final ReorgBlockchainBuilder b = new ReorgBlockchainBuilder();
  private final SnapSyncMetricsManager metricsManager = mock(SnapSyncMetricsManager.class);

  @BeforeEach
  void setUpMetrics() {
    when(metricsManager.getMetricsSystem()).thenReturn(new NoOpMetricsSystem());
  }

  /**
   * When the old pivot is orphaned, {@code finishPivotCatchup} must run full reorg recovery and
   * leave the local world state identical to the canonical pivot root.
   *
   * <pre>
   * gen -- 1 (A=100,D=75,F=200+s1:7) -- 2s (A=50,D=60,NC deployed)   orphaned pivot
   *                                  \-- 2c (A=80,F=140+s3:555,G=50) -- 3c (empty)   new pivot
   * </pre>
   */
  @Test
  void finishPivotCatchup_recoversWhenOldPivotIsOrphaned() {
    final Block block1 =
        b.appendBlockWithBal(
            b.header(0),
            b.merge(
                b.balWithBalances(Map.of(ALICE, Wei.of(100), DAVE, Wei.of(75), FRANK, Wei.of(200))),
                b.balWithStorageChanges(FRANK, Map.of(S1, UInt256.valueOf(7)))),
            1L);
    final Block block2s =
        b.appendStale(
            block1.getHeader(),
            b.merge(
                b.balWithBalances(Map.of(ALICE, Wei.of(50), DAVE, Wei.of(60))),
                b.balWithBalances(Map.of(NEW_CONTRACT, Wei.ONE)),
                b.balWithCodeChange(NEW_CONTRACT, NC_CODE),
                b.balWithStorageChanges(NEW_CONTRACT, Map.of(SN, UInt256.valueOf(5)))),
            2L);
    final Block block2c =
        b.appendCanonical(
            block1.getHeader(),
            b.merge(
                b.balWithBalances(Map.of(ALICE, Wei.of(80), FRANK, Wei.of(140), GRACE, Wei.of(50))),
                b.balWithStorageChanges(FRANK, Map.of(S3, UInt256.valueOf(555)))),
            2L);

    final DownloadedAccountRangeTracker canonicalAccountTracker = fullAccountRange();
    applyTo(canonicalCoordinator, 1, 2, canonicalAccountTracker, new DownloadedStorageRangeTracker());
    final Hash canonicalRoot = worldStateRoot(canonicalCoordinator);
    final Block newPivotBlock =
        b.appendCanonical(block2c.getHeader(), b.emptyBal(), 3L, canonicalRoot);

    final SnapSyncProcessState snapSyncState = new SnapSyncProcessState(block2s.getHeader());
    final SnapV2WorldDownloadState downloadState =
        createDownloadState(
            snapSyncState,
            healerServing(canonicalRoot, new AtomicInteger(), new AtomicInteger(), new AtomicInteger()));
    seedFullAccountRange(downloadState);
    applyTo(
        localCoordinator,
        1,
        2,
        downloadState.getAccountRangeTracker(),
        downloadState.getStorageRangeTracker());

    downloadState.startPivotCatchup(newPivotBlock.getHeader());
    awaitPivotCatchup(snapSyncState, newPivotBlock.getHeader());

    assertThat(downloadState.getDownloadFuture()).isNotCompletedExceptionally();
    assertThat(readAccount(ALICE).getBalance()).isEqualTo(Wei.of(80));
    assertThat(readAccount(DAVE).getBalance()).isEqualTo(Wei.of(75));
    assertThat(readAccount(GRACE).getBalance()).isEqualTo(Wei.of(50));
    assertThat(readStorageSlot(FRANK, S3)).hasValue(UInt256.valueOf(555));
    assertThat(accountExists(NEW_CONTRACT)).isFalse();
    assertThat(worldStateRoot(localCoordinator)).isEqualTo(canonicalRoot);
  }

  private static void seedFullAccountRange(final SnapV2WorldDownloadState downloadState) {
    downloadState.getAccountRangeTracker().registerPending(Bytes32.ZERO, MAX_KEY, 0);
  }

  private SnapV2WorldDownloadState createDownloadState(
      final SnapSyncProcessState snapSyncState, final SnapV2ReorgHealer reorgHealer) {
    final SnapV2BlockAccessListApplier applier =
        new SnapV2BlockAccessListApplier(
            localCoordinator, b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule());
    return new SnapV2WorldDownloadState(
        localCoordinator,
        mock(SnapSyncStatePersistenceManager.class),
        snapSyncState,
        new InMemoryTasksPriorityQueues<>(),
        10,
        50_000L,
        metricsManager,
        new TestClock(),
        SyncDurationMetrics.NO_OP_SYNC_DURATION_METRICS,
        null,
        (current, next) -> CompletableFuture.completedFuture(null),
        applier,
        reorgHealer,
        b.blockchain(),
        mock(EthContext.class));
  }

  private static void awaitPivotCatchup(
      final SnapSyncProcessState snapSyncState, final BlockHeader newPivotHeader) {
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () ->
                snapSyncState
                    .getPivotBlockHeader()
                    .filter(header -> header.getHash().equals(newPivotHeader.getHash()))
                    .isPresent());
  }

  private SnapV2ReorgHealer healerServing(
      final Hash canonicalRoot,
      final AtomicInteger accountFetches,
      final AtomicInteger codeFetches,
      final AtomicInteger storageFetches) {
    final SnapTestServing serving = new SnapTestServing(canonicalStorage, canonicalRoot);
    final SnapV2ReorgStateFetcher fetcher =
        new SnapV2ReorgStateFetcher(
            (start, end, pivot) -> {
              accountFetches.incrementAndGet();
              return serving.accountRange(start, end, pivot);
            },
            (accounts, start, end, pivot) -> {
              storageFetches.incrementAndGet();
              return serving.storageRange(accounts, start, end, pivot);
            },
            (codeHashes, pivot) -> {
              codeFetches.incrementAndGet();
              return serving.byteCodes(codeHashes, pivot);
            },
            localCoordinator);
    return new SnapV2ReorgHealer(
        b.blockchain(), localCoordinator, ReorgBlockchainBuilder.balEnabledSchedule(), fetcher);
  }

  private void applyTo(
      final WorldStateStorageCoordinator coordinator,
      final long fromBlock,
      final long toBlock,
      final DownloadedAccountRangeTracker accountTracker,
      final DownloadedStorageRangeTracker storageTracker) {
    new SnapV2BlockAccessListApplier(
            coordinator, b.blockchain(), ReorgBlockchainBuilder.balEnabledSchedule())
        .applyBlockAccessLists(fromBlock, toBlock, accountTracker, storageTracker)
        .commit();
  }

  private static BonsaiWorldStateKeyValueStorage newBonsaiStorage() {
    return new BonsaiWorldStateKeyValueStorage(
        new InMemoryKeyValueStorageProvider(),
        new NoOpMetricsSystem(),
        DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  }

  private static Hash worldStateRoot(final WorldStateStorageCoordinator coordinator) {
    return coordinator.getTrieNodeUnsafe(Bytes.EMPTY).map(Hash::hash).orElse(Hash.EMPTY_TRIE_HASH);
  }

  private static DownloadedAccountRangeTracker fullAccountRange() {
    final DownloadedAccountRangeTracker tracker = new DownloadedAccountRangeTracker();
    tracker.registerPending(Bytes32.ZERO, MAX_KEY, 0);
    return tracker;
  }

  private PmtStateTrieAccountValue readAccount(final Address address) {
    return PmtStateTrieAccountValue.readFrom(
        RLP.input(readAccountBytes(localCoordinator, address).orElseThrow()));
  }

  private boolean accountExists(final Address address) {
    return readAccountBytes(localCoordinator, address).isPresent();
  }

  private static Optional<Bytes> readAccountBytes(
      final WorldStateStorageCoordinator coordinator, final Address address) {
    return coordinator.applyForStrategy(
        bonsai -> bonsai.getAccount(address.addressHash()), forest -> Optional.<Bytes>empty());
  }

  private Optional<UInt256> readStorageSlot(final Address address, final UInt256 slotKey) {
    return localCoordinator
        .applyForStrategy(
            bonsai ->
                bonsai.getStorageValueByStorageSlotKey(
                    address.addressHash(), new StorageSlotKey(slotKey)),
            forest -> Optional.<Bytes>empty())
        .map(UInt256::fromBytes);
  }
}
