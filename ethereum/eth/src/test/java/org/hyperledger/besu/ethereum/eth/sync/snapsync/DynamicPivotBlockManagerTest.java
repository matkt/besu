/*
 * Copyright contributors to Hyperledger Besu
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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.fastsync.FastSyncActions;
import org.hyperledger.besu.ethereum.eth.sync.fastsync.FastSyncState;
import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;
import org.hyperledger.besu.testutil.DeterministicEthScheduler;

import java.util.OptionalLong;
import java.util.function.BiConsumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
public class DynamicPivotBlockManagerTest {

  private final SnapSyncProcessState snapSyncState = mock(SnapSyncProcessState.class);
  private final FastSyncActions fastSyncActions = mock(FastSyncActions.class);
  private final SyncState syncState = mock(SyncState.class);
  private final EthContext ethContext = mock(EthContext.class);
  private DynamicPivotBlockSelector dynamicPivotBlockManager;

  @BeforeEach
  public void setup() {
    when(fastSyncActions.getSyncState()).thenReturn(syncState);
    when(ethContext.getScheduler()).thenReturn(new DeterministicEthScheduler());
    dynamicPivotBlockManager =
        new DynamicPivotBlockSelector(
            ethContext,
            fastSyncActions,
            snapSyncState,
            SnapSyncConfiguration.DEFAULT_PIVOT_BLOCK_WINDOW_VALIDITY,
            SnapSyncConfiguration.DEFAULT_PIVOT_BLOCK_DISTANCE_BEFORE_CACHING);
  }

  @Test
  public void shouldNotSearchNewPivotBlockWhenCloseToTheHead() {

    when(fastSyncActions.getBestChainHeight()).thenReturn(1000L);

    when(snapSyncState.getPivotBlockNumber()).thenReturn(OptionalLong.of(999));
    final BiConsumer<BlockHeader, Boolean> onNewPivotBlock = spy(BiConsumer.class);
    dynamicPivotBlockManager.checkPivotBlock(onNewPivotBlock);
    verify(onNewPivotBlock, never()).accept(any(BlockHeader.class), anyBoolean());
  }

  @Test
  public void shouldSearchNewPivotBlockWhenNotCloseToTheHead() {
    final FastSyncState selectPivotBlockState = new FastSyncState(1090);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(1090).buildHeader();
    final FastSyncState downloadPivotBlockHeaderState = new FastSyncState(pivotBlockHeader);
    when(fastSyncActions.selectPivotBlock(FastSyncState.EMPTY_SYNC_STATE))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.downloadPivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(downloadPivotBlockHeaderState));

    when(fastSyncActions.getBestChainHeight()).thenReturn(1000L);

    when(snapSyncState.getPivotBlockNumber()).thenReturn(OptionalLong.of(939));

    final BiConsumer<BlockHeader, Boolean> onNewPivotBlock = spy(BiConsumer.class);
    dynamicPivotBlockManager.checkPivotBlock(onNewPivotBlock);
    verify(onNewPivotBlock, never()).accept(any(BlockHeader.class), anyBoolean());
    verify(fastSyncActions, times(1)).downloadPivotBlockHeader(selectPivotBlockState);
  }

  @Test
  public void shouldSwitchToNewPivotBlockWhenNeeded() {
    final FastSyncState selectPivotBlockState = new FastSyncState(1060);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(1060).buildHeader();
    final FastSyncState downloadPivotBlockHeaderState = new FastSyncState(pivotBlockHeader);
    when(fastSyncActions.selectPivotBlock(FastSyncState.EMPTY_SYNC_STATE))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.downloadPivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(downloadPivotBlockHeaderState));
    when(fastSyncActions.getBestChainHeight()).thenReturn(1000L);

    when(snapSyncState.getPivotBlockNumber()).thenReturn(OptionalLong.of(939));

    final BiConsumer<BlockHeader, Boolean> onNewPivotBlock = spy(BiConsumer.class);
    dynamicPivotBlockManager.checkPivotBlock(onNewPivotBlock);
    verify(onNewPivotBlock, never()).accept(any(BlockHeader.class), anyBoolean());

    when(fastSyncActions.getBestChainHeight()).thenReturn(1066L);

    dynamicPivotBlockManager.checkPivotBlock(onNewPivotBlock);
    verify(onNewPivotBlock, times(1)).accept(eq(pivotBlockHeader), eq(true));

    verify(snapSyncState).setCurrentHeader(pivotBlockHeader);
  }

  @Test
  public void shouldSwitchToNewPivotOnlyOnce() {
    final FastSyncState selectPivotBlockState = new FastSyncState(1060);
    final BlockHeader pivotBlockHeader = new BlockHeaderTestFixture().number(1060).buildHeader();
    final FastSyncState downloadPivotBlockHeaderState = new FastSyncState(pivotBlockHeader);
    when(fastSyncActions.selectPivotBlock(FastSyncState.EMPTY_SYNC_STATE))
        .thenReturn(completedFuture(selectPivotBlockState));
    when(fastSyncActions.downloadPivotBlockHeader(selectPivotBlockState))
        .thenReturn(completedFuture(downloadPivotBlockHeaderState));

    when(fastSyncActions.getBestChainHeight()).thenReturn(1066L);

    when(snapSyncState.getPivotBlockNumber()).thenReturn(OptionalLong.of(939));
    BiConsumer<BlockHeader, Boolean> onNewPivotBlock = spy(BiConsumer.class);
    dynamicPivotBlockManager.checkPivotBlock(onNewPivotBlock);
    verify(onNewPivotBlock, times(1)).accept(eq(pivotBlockHeader), eq(true));

    when(snapSyncState.getPivotBlockNumber())
        .thenReturn(OptionalLong.of(pivotBlockHeader.getNumber()));
    onNewPivotBlock = spy(BiConsumer.class);
    dynamicPivotBlockManager.checkPivotBlock(onNewPivotBlock);
    verify(onNewPivotBlock, never()).accept(any(BlockHeader.class), anyBoolean());
  }
}
