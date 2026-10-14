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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeerImmutableAttributes;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.WorldStateTrieBuilder;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

/** Metrics of a SNAP2 sync, read from its current attempt, and a summary of its peers. */
final class Snap2Metrics {

  private final EthPeers ethPeers;
  private final Supplier<SyncAttempt> current;
  private final AtomicLong moves = new AtomicLong();
  private final AtomicInteger restarts = new AtomicInteger();

  Snap2Metrics(final EthPeers ethPeers, final Supplier<SyncAttempt> current) {
    this.ethPeers = ethPeers;
    this.current = current;
  }

  void pivotMoved() {
    moves.incrementAndGet();
  }

  void restarted() {
    restarts.incrementAndGet();
  }

  int restarts() {
    return restarts.get();
  }

  long pivotMoves() {
    return moves.get();
  }

  /** Registers the gauges and counters. */
  void register(final MetricsSystem metricsSystem) {
    final BesuMetricCategory category = BesuMetricCategory.SYNCHRONIZER;
    metricsSystem.createLongGauge(
        category,
        "snap2_pivot_block",
        "Block of the SNAP2 pivot",
        () -> attemptValue(SyncAttempt::pivotNumber));
    metricsSystem.createLongGauge(
        category,
        "snap2_headers_lowest_stored",
        "Lowest block header stored by the SNAP2 header download",
        () -> attemptValue(SyncAttempt::lowestStoredHeader));
    metricsSystem.createGauge(
        category,
        "snap2_account_coverage",
        "Share of the account key space SNAP2 downloaded, from 0 to 1",
        () -> {
          final SyncAttempt attempt = current.get();
          return attempt == null ? 0 : attempt.stateSync().accountCoverage();
        });
    metricsSystem.createLongGauge(
        category,
        "snap2_pending_storages",
        "Accounts whose storage SNAP2 still has to download",
        () -> attemptValue(attempt -> attempt.stateSync().pendingStorageCount()));
    metricsSystem.createLongGauge(
        category,
        "snap2_peers_without_state",
        "Peers left out of state requests because they answer nothing for the pivot",
        () -> attemptValue(attempt -> attempt.stateSync().peersWithoutState()));
    metricsSystem.createLongGauge(
        category,
        "snap2_storage_tries_built",
        "Storage tries SNAP2 wrote while downloading",
        () -> attemptValue(attempt -> attempt.stateSync().storageTriesBuilt()));
    metricsSystem.createLongGauge(
        category,
        "snap2_storage_tries_updated",
        "Storage tries SNAP2 wrote while downloading then changed for pivot moves",
        () -> attemptValue(attempt -> attempt.stateSync().storageTriesUpdated()));
    metricsSystem.createLongGauge(
        category,
        "snap2_storage_trie_slots",
        "Slots in the storage tries SNAP2 wrote while downloading",
        () -> attemptValue(attempt -> attempt.stateSync().storageTrieSlots()));
    metricsSystem.createLongGauge(
        category,
        "snap2_trie_accounts_processed",
        "Accounts added to the tries SNAP2 is building",
        () -> trieValue(WorldStateTrieBuilder::accountsProcessed));
    metricsSystem.createLongGauge(
        category,
        "snap2_trie_slots_processed",
        "Storage slots added to the tries SNAP2 is building",
        () -> trieValue(WorldStateTrieBuilder::slotsProcessed));
    metricsSystem.createLongGauge(
        category,
        "snap2_trie_storage_tries_kept",
        "Storage tries written while downloading that the final SNAP2 build kept",
        () -> trieValue(WorldStateTrieBuilder::keptStorageTries));
    metricsSystem.createLongGauge(
        category,
        "snap2_storage_tries_left",
        "Downloaded storages whose trie SNAP2 left to the final build",
        () -> attemptValue(attempt -> attempt.stateSync().storageTriesLeft()));
    metricsSystem.createCounter(
        category,
        "snap2_accounts_downloaded_total",
        "Accounts downloaded by SNAP2",
        () -> attemptValue(attempt -> attempt.stateSync().downloadedAccounts()));
    metricsSystem.createCounter(
        category,
        "snap2_slots_downloaded_total",
        "Storage slots downloaded by SNAP2",
        () -> attemptValue(attempt -> attempt.stateSync().downloadedSlots()));
    metricsSystem.createCounter(
        category,
        "snap2_bytecodes_downloaded_total",
        "Bytecodes downloaded by SNAP2",
        () -> attemptValue(attempt -> attempt.stateSync().downloadedCodes()));
    final var progress =
        metricsSystem.createLabelledSuppliedGauge(
            category, "snap2_progress", "Share done of each SNAP2 phase, from 0 to 1", "phase");
    progress.labels(() -> progressValue(SyncAttempt.Progress::state), "state");
    progress.labels(() -> progressValue(SyncAttempt.Progress::tries), "tries");
    progress.labels(() -> progressValue(SyncAttempt.Progress::headers), "headers");
    progress.labels(() -> progressValue(SyncAttempt.Progress::bodies), "bodies");
    metricsSystem.createGauge(
        category,
        "snap2_eta_seconds",
        "Time left to the end of the slowest SNAP2 phase running once the state is downloaded, NaN before",
        () -> progressValue(figures -> figures.secondsLeft().orElse(Double.NaN)));
    metricsSystem.createCounter(
        category, "snap2_pivot_moves_total", "Pivot moves of SNAP2", moves::get);
    metricsSystem.createCounter(
        category,
        "snap2_pivot_move_milliseconds_total",
        "Time SNAP2 pivot moves held the state download",
        () -> attemptValue(attempt -> attempt.stateSync().moveMillis()));
    metricsSystem.createCounter(
        category,
        "snap2_persisted_responses_total",
        "State responses SNAP2 persisted",
        () -> attemptValue(attempt -> attempt.stateSync().persistedResponses()));
    metricsSystem.createCounter(
        category,
        "snap2_persist_milliseconds_total",
        "Time SNAP2 spent persisting state responses",
        () -> attemptValue(attempt -> attempt.stateSync().persistMillis()));
    metricsSystem.createLongGauge(
        category,
        "snap2_persist_queue",
        "State responses waiting for a thread to persist them",
        () -> attemptValue(attempt -> attempt.stateSync().responsesWaitingToPersist()));
    metricsSystem.createLongGauge(
        category,
        "snap2_storage_trie_updates_pending",
        "Storage tries whose pivot move changes wait to be applied",
        () -> attemptValue(attempt -> attempt.stateSync().storageTrieUpdatesPending()));
    metricsSystem.createLongGauge(
        category,
        "snap2_storage_trie_update_slots_pending",
        "Changed slots waiting to be applied to storage tries",
        () -> attemptValue(attempt -> attempt.stateSync().storageTrieUpdateSlotsPending()));
    metricsSystem.createCounter(
        category, "snap2_restarts_total", "Times SNAP2 started over", restarts::get);
    final var stage =
        metricsSystem.createLabelledSuppliedGauge(
            category, "snap2_stage", "Current SNAP2 stage, 1 for the active one", "stage");
    for (final String name : List.of("waiting", "state", "tries", "chain", "done")) {
      stage.labels(() -> name.equals(stage()) ? 1 : 0, name);
    }
    final var peers =
        metricsSystem.createLabelledSuppliedGauge(
            category, "snap2_peers", "Connected peers by what they serve to SNAP2", "capability");
    peers.labels(() -> peerCounts().all(), "all");
    peers.labels(() -> peerCounts().snap1(), "snap1");
    peers.labels(() -> peerCounts().snap2(), "snap2");
    peers.labels(() -> peerCounts().eth71(), "eth71");
  }

  /** Returns the progress of the current attempt, none before the first one. */
  Optional<SyncAttempt.Progress> progress() {
    return Optional.ofNullable(current.get()).map(SyncAttempt::progressFigures);
  }

  private double progressValue(final ToDoubleFunction<SyncAttempt.Progress> figure) {
    return progress().map(figure::applyAsDouble).orElse(0.0);
  }

  // a value of the final trie build, 0 before it starts
  private long trieValue(final ToLongFunction<WorldStateTrieBuilder> value) {
    return attemptValue(attempt -> attempt.trieBuilder().map(value::applyAsLong).orElse(0L));
  }

  // a value of the current attempt, 0 before the first one
  private long attemptValue(final ToLongFunction<SyncAttempt> value) {
    final SyncAttempt attempt = current.get();
    return attempt == null ? 0 : value.applyAsLong(attempt);
  }

  String stage() {
    final SyncAttempt attempt = current.get();
    return attempt == null ? "waiting" : attempt.stage();
  }

  /** The connected peers by what they can serve: snap ranges, and block access lists. */
  record PeerCounts(int all, int snap1, int snap2, int eth71) {

    /** Returns whether the peers are worth a mention: few of them, or none serving snap/2. */
    boolean few() {
      return all < 5 || snap2 == 0;
    }

    @Override
    public String toString() {
      return String.format("peers %d (snap/2 %d, snap/1 %d, eth/71 %d)", all, snap2, snap1, eth71);
    }
  }

  PeerCounts peerCounts() {
    int peers = 0;
    int snap1 = 0;
    int snap2 = 0;
    int eth71 = 0;
    for (final EthPeer peer :
        ethPeers.streamAvailablePeers().map(EthPeerImmutableAttributes::ethPeer).toList()) {
      final Set<Capability> capabilities = peer.getAgreedCapabilities();
      peers++;
      if (capabilities.contains(SnapProtocol.SNAP2)) {
        snap2++;
      } else if (capabilities.contains(SnapProtocol.SNAP1)) {
        snap1++;
      }
      if (capabilities.stream().anyMatch(EthProtocol::isEth71Compatible)) {
        eth71++;
      }
    }
    return new PeerCounts(peers, snap1, snap2, eth71);
  }
}
