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
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import com.google.common.annotations.VisibleForTesting;

/** Metrics of a SNAP2 sync, read from its current attempt, and a summary of its peers. */
final class Snap2Metrics {

  private final EthPeers ethPeers;
  private final Supplier<SyncAttempt> current;
  private final AtomicLong moves = new AtomicLong();
  private final AtomicLong reorgs = new AtomicLong();
  private final AtomicInteger restarts = new AtomicInteger();

  Snap2Metrics(final EthPeers ethPeers, final Supplier<SyncAttempt> current) {
    this.ethPeers = ethPeers;
    this.current = current;
  }

  void pivotMoved() {
    moves.incrementAndGet();
  }

  void reorged() {
    reorgs.incrementAndGet();
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

  @VisibleForTesting
  long reorgs() {
    return reorgs.get();
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
    metricsSystem.createLongGauge(
        category,
        "snap2_bodies_stored",
        "Blocks whose body and receipts SNAP2 stored",
        () -> attemptValue(SyncAttempt::bodiesStored));
    metricsSystem.createLongGauge(
        category,
        "snap2_bodies_needed",
        "Blocks whose body and receipts SNAP2 needs below the pivot before setting the chain head",
        () -> attemptValue(SyncAttempt::bodiesNeeded));
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
        "Storage tries SNAP2 built while downloading",
        () -> attemptValue(attempt -> attempt.stateSync().storageTriesBuilt()));
    metricsSystem.createLongGauge(
        category,
        "snap2_storage_tries_queued",
        "Downloaded storages waiting for their trie",
        () -> attemptValue(attempt -> attempt.stateSync().storageTriesQueued()));
    metricsSystem.createLongGauge(
        category,
        "snap2_trie_accounts_processed",
        "Accounts added to the tries SNAP2 is building",
        () -> attemptValue(attempt -> attempt.trieProgress(true)));
    metricsSystem.createLongGauge(
        category,
        "snap2_trie_slots_processed",
        "Storage slots added to the tries SNAP2 is building",
        () -> attemptValue(attempt -> attempt.trieProgress(false)));
    metricsSystem.createLongGauge(
        category,
        "snap2_trie_storage_tries_kept",
        "Storage tries built while downloading that the final SNAP2 build kept",
        () -> attemptValue(SyncAttempt::trieKeptStorageTries));
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
    metricsSystem.createCounter(
        category, "snap2_pivot_moves_total", "Pivot moves of SNAP2", moves::get);
    metricsSystem.createCounter(
        category, "snap2_reorgs_total", "Reorgs below the SNAP2 pivot", reorgs::get);
    metricsSystem.createCounter(
        category, "snap2_restarts_total", "Times SNAP2 started over", restarts::get);
    final var stage =
        metricsSystem.createLabelledSuppliedGauge(
            category, "snap2_stage", "Current SNAP2 stage, 1 for the active one", "stage");
    for (final String name : List.of("waiting", "state", "tries", "chain")) {
      stage.labels(() -> name.equals(stage()) ? 1 : 0, name);
    }
    final var peers =
        metricsSystem.createLabelledSuppliedGauge(
            category, "snap2_peers", "Connected peers by what they serve to SNAP2", "capability");
    peers.labels(() -> countPeers(capabilities -> true), "all");
    peers.labels(
        () -> countPeers(capabilities -> capabilities.contains(SnapProtocol.SNAP1)), "snap1");
    peers.labels(
        () -> countPeers(capabilities -> capabilities.contains(SnapProtocol.SNAP2)), "snap2");
    peers.labels(
        () ->
            countPeers(
                capabilities -> capabilities.stream().anyMatch(EthProtocol::isEth71Compatible)),
        "eth71");
  }

  // a value of the current attempt, 0 before the first one
  private long attemptValue(final ToLongFunction<SyncAttempt> value) {
    final SyncAttempt attempt = current.get();
    return attempt == null ? 0 : value.applyAsLong(attempt);
  }

  private String stage() {
    final SyncAttempt attempt = current.get();
    return attempt == null ? "waiting" : attempt.stage();
  }

  private int countPeers(final Predicate<Set<Capability>> filter) {
    return (int)
        ethPeers
            .streamAvailablePeers()
            .map(EthPeerImmutableAttributes::ethPeer)
            .filter(peer -> filter.test(peer.getAgreedCapabilities()))
            .count();
  }

  /** Returns what the connected peers can serve: snap ranges, and block access lists. */
  String peerSummary() {
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
    return String.format("%d peers (snap/1 %d, snap/2 %d, eth/71 %d)", peers, snap1, snap2, eth71);
  }
}
