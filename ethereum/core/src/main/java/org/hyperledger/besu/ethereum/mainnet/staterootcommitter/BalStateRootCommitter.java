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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockProcessingExecutors;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary.BinaryBalEngine;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.patricia.PatriciaBalEngine;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;
import org.hyperledger.besu.plugin.services.worldstate.StateRootComputation;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background BAL state-root committer. Trie materialization is delegated to an {@link Engine}:
 * {@link PatriciaBalEngine} (account + storage Patricia tries) or {@link BinaryBalEngine}
 * (partitioned binary trie).
 */
public final class BalStateRootCommitter implements StateRootCommitter {

  /**
   * Format-specific BAL root computation. The orchestrator owns parent snapshot, background
   * execution, header check, and persist-time accumulator patching.
   */
  public interface Engine {
    /**
     * When true, the parent world state is opened with a BAL overlay. Patricia uses this so account
     * loads see BAL-merged state; Binary reads prior state from the parent flat DB and applies the
     * BAL onto the binary trie directly.
     */
    boolean useBalOverlay();

    Result compute(
        BonsaiWorldState parent, BlockAccessListAccountLookup accountLookup, boolean storageFrozen);
  }

  /**
   * Outcome of a BAL background root computation.
   *
   * @param computation root and deferred KV writes
   * @param storageRoots Patricia per-account storage roots to patch into the EVM accumulator (empty
   *     for Binary)
   * @param introducedCodeHashes Binary code hashes newly introduced by this block (empty for
   *     Patricia)
   */
  public record Result(
      StateRootComputation computation,
      Map<Address, Hash> storageRoots,
      Set<Hash> introducedCodeHashes) {

    static Result empty(final Hash parentRoot) {
      return new Result(StateRootComputations.pathBased(parentRoot, List.of()), Map.of(), Set.of());
    }
  }

  private final CompletableFuture<Result> backgroundComputation;
  private final AtomicBoolean cancelled = new AtomicBoolean(false);
  private final Engine engine;

  /** Patricia BAL background committer (existing public constructor). */
  public BalStateRootCommitter(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final BlockAccessListAccountLookup accountLookup,
      final boolean storageFrozen) {
    this(protocolContext, blockHeader, accountLookup, storageFrozen, PatriciaBalEngine.INSTANCE);
  }

  public BalStateRootCommitter(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final BlockAccessListAccountLookup accountLookup,
      final boolean storageFrozen,
      final Engine engine) {
    this.engine = engine;
    this.backgroundComputation =
        CompletableFuture.supplyAsync(
            () -> {
              try (BonsaiWorldState parent =
                  openParentWorldState(protocolContext, blockHeader, accountLookup, engine)) {
                return runComputation(parent, accountLookup, storageFrozen, engine);
              }
            },
            BlockProcessingExecutors.stateRootExecutor());
  }

  @Override
  public void cancel() {
    cancelled.set(true);
    backgroundComputation.cancel(true);
  }

  @Override
  public TrieBranchType getTrieBranchType() {
    return engine == BinaryBalEngine.INSTANCE ? TrieBranchType.BINARY : TrieBranchType.PATRICIA;
  }

  /**
   * Waits for the background computation, patches format-specific accumulator state (Patricia
   * storage roots / Binary introduced code hashes), and returns the BAL-computed root.
   *
   * <p>The BAL-computed root is the authoritative source. If it does not match the block header
   * state root, an {@link IllegalStateException} is thrown.
   */
  @Override
  public StateRootComputation compute(
      final MutableWorldState worldState,
      final BlockHeader blockHeader,
      final WorldUpdater worldUpdater) {
    final Result result = awaitBackgroundComputation(backgroundComputation);
    final BonsaiWorldStateUpdateAccumulator accumulator =
        (BonsaiWorldStateUpdateAccumulator)
            Objects.requireNonNull(
                worldUpdater, "BAL state root committer requires a non-null WorldUpdater");
    result
        .storageRoots()
        .forEach(
            (address, newStorageRoot) -> {
              final var entry = accumulator.getAccountsToUpdate().get(address);
              if (entry != null && entry.getUpdated() != null) {
                entry.getUpdated().setStorageRoot(newStorageRoot);
              }
            });
    accumulator.getIntroducedCodeHashes().addAll(result.introducedCodeHashes());

    if (blockHeader != null && !result.computation().root().equals(blockHeader.getStateRoot())) {
      throw new IllegalStateException(
          "BAL-computed root does not match block header state root: expected "
              + blockHeader.getStateRoot()
              + " but BAL computed "
              + result.computation().root());
    }
    return result.computation();
  }

  private static Result runComputation(
      final BonsaiWorldState worldState,
      final BlockAccessListAccountLookup accountLookup,
      final boolean storageFrozen,
      final Engine engine) {
    if (accountLookup.isEmpty()) {
      return Result.empty(worldState.getWorldStateRootHash());
    }
    return engine.compute(worldState, accountLookup, storageFrozen);
  }

  private Result awaitBackgroundComputation(final CompletableFuture<Result> future) {
    try {
      final Result result = future.get();
      if (cancelled.get()) {
        throw new IllegalStateException("Background BAL state root computation was cancelled");
      }
      return result;
    } catch (final CancellationException e) {
      throw new IllegalStateException("Background BAL state root computation was cancelled", e);
    } catch (final ExecutionException e) {
      final Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new IllegalStateException("Background BAL state root computation failed", cause);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while waiting for background BAL state root computation", e);
    }
  }

  private static BonsaiWorldState openParentWorldState(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final BlockAccessListAccountLookup accountLookup,
      final Engine engine) {
    final Hash parentHash = blockHeader.getParentHash();
    final BlockHeader parentHeader =
        protocolContext
            .getBlockchain()
            .getBlockHeader(parentHash)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Parent %s of block %s not found",
                            parentHash, blockHeader.getBlockHash())));
    final WorldStateQueryParams.Builder queryParams =
        WorldStateQueryParams.newBuilder()
            .withBlockHeader(parentHeader)
            .withShouldWorldStateUpdateHead(false);
    if (engine.useBalOverlay()) {
      queryParams.withBalOverlay(new BlockAccessListOverlay(accountLookup, Long.MAX_VALUE));
    }
    final BonsaiWorldState worldState =
        (BonsaiWorldState)
            protocolContext.getWorldStateArchive().getWorldState(queryParams.build()).orElseThrow();
    worldState.disableCacheMerkleTrieLoader();
    return worldState;
  }
}
