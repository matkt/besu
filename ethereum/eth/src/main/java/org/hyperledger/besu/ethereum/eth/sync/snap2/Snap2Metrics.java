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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import org.hyperledger.besu.ethereum.eth.sync.snap2.state.StateSync;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.WorldStateTrieBuilder;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.LabelledSuppliedMetric;

import java.util.Optional;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

/** The metrics of the SNAP2 sync, read from its current attempt, 0 before the first one. */
final class Snap2Metrics {

  private static final BesuMetricCategory CATEGORY = BesuMetricCategory.SYNCHRONIZER;

  private final Supplier<Optional<SyncAttempt>> current;

  private Snap2Metrics(final Supplier<Optional<SyncAttempt>> current) {
    this.current = current;
  }

  /** Registers the metrics of the sync. */
  static void register(final MetricsSystem metricsSystem, final Snap2SyncController sync) {
    final Snap2Metrics metrics = new Snap2Metrics(sync::current);
    metricsSystem.createLongGauge(
        CATEGORY,
        "snap2_pivot_block",
        "Block of the SNAP2 pivot",
        () -> metrics.attemptValue(SyncAttempt::pivotNumber));
    metricsSystem.createCounter(
        CATEGORY, "snap2_pivot_moves_total", "Pivot moves of SNAP2", sync::pivotMoves);
    metricsSystem.createCounter(
        CATEGORY, "snap2_restarts_total", "Times SNAP2 started over", sync::restarts);
    metrics.registerState(metricsSystem);
    metrics.registerTries(metricsSystem);
    final LabelledSuppliedMetric progress =
        metricsSystem.createLabelledSuppliedGauge(
            CATEGORY, "snap2_progress", "Share done of each SNAP2 phase, from 0 to 1", "phase");
    progress.labels(() -> metrics.progressValue(SyncAttempt.Progress::state), "state");
    progress.labels(() -> metrics.progressValue(SyncAttempt.Progress::tries), "tries");
    progress.labels(() -> metrics.progressValue(SyncAttempt.Progress::headers), "headers");
    progress.labels(() -> metrics.progressValue(SyncAttempt.Progress::bodies), "bodies");
    final LabelledSuppliedMetric stage =
        metricsSystem.createLabelledSuppliedGauge(
            CATEGORY, "snap2_stage", "Current SNAP2 stage, 1 for the active one", "stage");
    // waiting before the first attempt, and between two
    stage.labels(() -> metrics.stage().isEmpty() ? 1 : 0, "waiting");
    for (final SyncAttempt.Stage name : SyncAttempt.Stage.values()) {
      stage.labels(() -> metrics.stage().filter(name::equals).isPresent() ? 1 : 0, name.label());
    }
  }

  private void registerState(final MetricsSystem metricsSystem) {
    metricsSystem.createCounter(
        CATEGORY,
        "snap2_accounts_downloaded_total",
        "Accounts downloaded by SNAP2",
        () -> stateValue(StateSync::downloadedAccounts));
    metricsSystem.createCounter(
        CATEGORY,
        "snap2_slots_downloaded_total",
        "Storage slots downloaded by SNAP2",
        () -> stateValue(StateSync::downloadedSlots));
    metricsSystem.createCounter(
        CATEGORY,
        "snap2_bytecodes_downloaded_total",
        "Bytecodes downloaded by SNAP2",
        () -> stateValue(StateSync::downloadedCodes));
    metricsSystem.createLongGauge(
        CATEGORY,
        "snap2_pending_storages",
        "Accounts whose storage SNAP2 still has to download",
        () -> stateValue(StateSync::pendingStorageCount));
    metricsSystem.createLongGauge(
        CATEGORY,
        "snap2_peers_without_state",
        "Peers left out of the state requests, as they serve nothing for the pivot",
        () -> stateValue(StateSync::peersWithoutState));
    metricsSystem.createLongGauge(
        CATEGORY,
        "snap2_storage_tries_built",
        "Storage tries SNAP2 wrote while downloading",
        () -> stateValue(StateSync::storageTriesBuilt));
    metricsSystem.createLongGauge(
        CATEGORY,
        "snap2_storage_tries_updated",
        "Storage tries SNAP2 wrote while downloading, then changed for pivot moves",
        () -> stateValue(StateSync::storageTriesUpdated));
    metricsSystem.createLongGauge(
        CATEGORY,
        "snap2_storage_tries_left",
        "Downloaded storages whose trie SNAP2 left to the final build",
        () -> stateValue(StateSync::storageTriesLeft));
    metricsSystem.createLongGauge(
        CATEGORY,
        "snap2_storage_trie_updates_pending",
        "Storage tries whose changes from pivot moves wait to be applied",
        () -> stateValue(StateSync::storageTrieUpdatesPending));
  }

  private void registerTries(final MetricsSystem metricsSystem) {
    metricsSystem.createLongGauge(
        CATEGORY,
        "snap2_trie_accounts_processed",
        "Accounts added to the tries SNAP2 builds",
        () -> trieValue(WorldStateTrieBuilder::accountsProcessed));
    metricsSystem.createLongGauge(
        CATEGORY,
        "snap2_trie_slots_processed",
        "Storage slots added to the tries SNAP2 builds",
        () -> trieValue(WorldStateTrieBuilder::slotsProcessed));
    metricsSystem.createLongGauge(
        CATEGORY,
        "snap2_trie_storage_tries_kept",
        "Storage tries written while downloading that the final SNAP2 build kept",
        () -> trieValue(WorldStateTrieBuilder::keptStorageTries));
  }

  private Optional<SyncAttempt.Stage> stage() {
    return current.get().map(SyncAttempt::stage);
  }

  private double progressValue(final ToDoubleFunction<SyncAttempt.Progress> phase) {
    return current.get().map(attempt -> phase.applyAsDouble(attempt.progress())).orElse(0.0);
  }

  private long trieValue(final ToLongFunction<WorldStateTrieBuilder> value) {
    return current.get().flatMap(SyncAttempt::trieBuilder).map(value::applyAsLong).orElse(0L);
  }

  private long stateValue(final ToLongFunction<StateSync> value) {
    return attemptValue(attempt -> value.applyAsLong(attempt.stateSync()));
  }

  private long attemptValue(final ToLongFunction<SyncAttempt> value) {
    return current.get().map(value::applyAsLong).orElse(0L);
  }
}
