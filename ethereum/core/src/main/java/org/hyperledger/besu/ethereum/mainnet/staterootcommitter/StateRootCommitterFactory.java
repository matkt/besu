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

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.trie.forest.ForestWorldStateArchive;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;

/**
 * Picks one committer per block:
 *
 * <ul>
 *   <li>{@link ForestStateRootCommitter} — Forest archive
 *   <li>{@link BalStateRootCommitter} — Bonsai + BAL background root
 *   <li>{@link TrieDisabledStateRootCommitter} — Bonsai flat (trie-disabled) mode
 *   <li>{@link DefaultStateRootCommitter} — Bonsai accumulator at persist
 * </ul>
 */
public final class StateRootCommitterFactory {

  private enum Mode {
    BAL,
    DEFAULT,
    FOREST,
    TRIE_DISABLED
  }

  /** A BAL state root computation started before its block is processed. */
  private record StartedAhead(BlockAccessList blockAccessList, BalStateRootCommitter committer) {}

  private final BalConfiguration balConfiguration;
  private final AtomicReference<StartedAhead> startedAhead = new AtomicReference<>();

  public StateRootCommitterFactory(final BalConfiguration balConfiguration) {
    this.balConfiguration = balConfiguration;
  }

  /**
   * Starts the BAL state root computation of an upcoming block as soon as its access list is known:
   * it only needs the parent state and the access list. Processing that block (the same access list
   * instance) then picks it up in {@link #forBlock}.
   *
   * @param protocolContext the protocol context
   * @param parentHeader the header of the parent of the upcoming block
   * @param blockAccessList the block access list of the upcoming block
   */
  public void startAhead(
      final ProtocolContext protocolContext,
      final BlockHeader parentHeader,
      final BlockAccessList blockAccessList) {
    if (resolveMode(protocolContext, Optional.of(blockAccessList)) != Mode.BAL) {
      return;
    }
    // a payload is imported on top of the head, which is not frozen
    final BalStateRootCommitter committer =
        BalStateRootCommitter.forParent(protocolContext, parentHeader, blockAccessList, false)
            .start();
    final StartedAhead previous =
        startedAhead.getAndSet(new StartedAhead(blockAccessList, committer));
    if (previous != null) {
      previous.committer().cancel();
    }
  }

  /**
   * Cancels the computation started ahead for {@code blockAccessList}, unless its block took it.
   *
   * @param blockAccessList the block access list it was started for
   */
  public void cancelAhead(final BlockAccessList blockAccessList) {
    takeStartedAhead(blockAccessList).ifPresent(BalStateRootCommitter::cancel);
  }

  public StateRootCommitter forBlock(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final Optional<BlockAccessList> maybeBal,
      final boolean storageFrozen) {
    return switch (resolveMode(protocolContext, maybeBal)) {
      case BAL -> {
        final Optional<BalStateRootCommitter> ahead = takeStartedAhead(maybeBal.get());
        if (ahead.isPresent() && !storageFrozen) {
          yield ahead.get();
        }
        // started for a world state that is not frozen: its writes are not the ones wanted here
        ahead.ifPresent(BalStateRootCommitter::cancel);
        yield BalStateRootCommitter.forBlock(
                protocolContext, blockHeader, maybeBal.get(), storageFrozen)
            .start();
      }
      case DEFAULT -> new DefaultStateRootCommitter();
      case FOREST -> ForestStateRootCommitter.INSTANCE;
      case TRIE_DISABLED -> TrieDisabledStateRootCommitter.INSTANCE;
    };
  }

  @VisibleForTesting
  boolean hasStartedAhead() {
    return startedAhead.get() != null;
  }

  // the very instance decoded from the payload reaches the block processing: compare identities,
  // not the content of two possibly large lists
  @SuppressWarnings("ReferenceEquality")
  private Optional<BalStateRootCommitter> takeStartedAhead(final BlockAccessList blockAccessList) {
    final StartedAhead started = startedAhead.get();
    return started != null
            && started.blockAccessList() == blockAccessList
            && startedAhead.compareAndSet(started, null)
        ? Optional.of(started.committer())
        : Optional.empty();
  }

  private Mode resolveMode(
      final ProtocolContext protocolContext, final Optional<BlockAccessList> maybeBal) {
    if (protocolContext.getWorldStateArchive() instanceof ForestWorldStateArchive) {
      return Mode.FOREST;
    }
    if (maybeBal.isPresent()
        && balConfiguration.isBalStateRootEnabled()
        && !isTrieDisabled(protocolContext)) {
      return Mode.BAL;
    }
    if (isTrieDisabled(protocolContext)) {
      return Mode.TRIE_DISABLED;
    }
    return Mode.DEFAULT;
  }

  private static boolean isTrieDisabled(final ProtocolContext protocolContext) {
    return protocolContext.getWorldStateArchive() instanceof PathBasedWorldStateProvider provider
        && provider.getWorldStateSharedSpec().isTrieDisabled();
  }
}
