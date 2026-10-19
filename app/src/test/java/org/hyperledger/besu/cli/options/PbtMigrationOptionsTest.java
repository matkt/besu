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
package org.hyperledger.besu.cli.options;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.cli.CommandTestAbstract;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.PbtMigrator;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PbtMigrationOptionsTest extends CommandTestAbstract {

  @Test
  public void defaultTuningTakesHalfTheCores() {
    parseCommand();

    assertThat(tuning()).isEqualTo(PbtMigrator.Tuning.DEFAULT);
    assertThat(PbtMigrator.Tuning.DEFAULT)
        .isEqualTo(
            new PbtMigrator.Tuning(
                Math.max(1, Runtime.getRuntime().availableProcessors() / 2),
                128,
                Optional.empty(),
                64L << 20));
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void tuningOptionsReachTheMigrator() {
    parseCommand(
        "--Xpbt-migration-threads",
        "1",
        "--Xpbt-migration-blocks-per-roll",
        "16",
        "--Xpbt-migration-roll-pause",
        "250",
        "--Xpbt-migration-sort-buffer",
        "8");

    assertThat(tuning())
        .isEqualTo(new PbtMigrator.Tuning(1, 16, Optional.of(Duration.ofMillis(250)), 8L << 20));
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void noThreadIsRejected() {
    parseCommand("--Xpbt-migration-threads", "0");

    Mockito.verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("PBT migration threads must be between 1 and");
  }

  @Test
  public void rollsLongerThanTheKeptTrieLogsAreRejected() {
    parseCommand("--Xpbt-migration-blocks-per-roll", "513");

    Mockito.verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("PBT migration blocks per roll must be between 1 and 512, got 513");
  }

  @Test
  public void negativeRollPauseIsRejected() {
    parseCommand("--Xpbt-migration-roll-pause", "-1");

    Mockito.verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("PBT migration roll pause must not be negative");
  }

  @Test
  public void genesisStartIsOffByDefault() {
    parseCommand();

    verify(mockControllerBuilder).pbtMigrationFromGenesis(false);
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void genesisStartOptionReachesTheMigrator() {
    parseCommand("--Xpbt-migration-from-genesis");

    verify(mockControllerBuilder).pbtMigrationFromGenesis(true);
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void genesisStartAndSnapshotCannotBeSetTogether() {
    parseCommand(
        "--Xpbt-migration-from-genesis",
        "--Xpbt-snapshot-file",
        "snapshot.bin",
        "--Xpbt-preimages-file",
        "preimages.bin",
        "--Xpbt-snapshot-anchor-block-hash",
        "0x0000000000000000000000000000000000000000000000000000000000000001");

    Mockito.verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("--Xpbt-migration-from-genesis cannot be used with --Xpbt-snapshot-file");
  }

  private PbtMigrator.Tuning tuning() {
    final ArgumentCaptor<PbtMigrator.Tuning> tuning =
        ArgumentCaptor.forClass(PbtMigrator.Tuning.class);
    verify(mockControllerBuilder).pbtMigrationTuning(tuning.capture());
    return tuning.getValue();
  }
}
