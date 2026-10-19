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
package org.hyperledger.besu.cli.options;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import org.hyperledger.besu.cli.CommandTestAbstract;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.PbtMigrator;

import java.time.Duration;

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
                Duration.ZERO,
                64L << 20));
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void tuningOptionsReachTheMigrator() {
    parseCommand(
        "--Xpbt-migration-threads",
        "3",
        "--Xpbt-migration-blocks-per-roll",
        "16",
        "--Xpbt-migration-roll-pause",
        "250",
        "--Xpbt-migration-sort-buffer",
        "8");

    assertThat(tuning()).isEqualTo(new PbtMigrator.Tuning(3, 16, Duration.ofMillis(250), 8L << 20));
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void noThreadIsRejected() {
    parseCommand("--Xpbt-migration-threads", "0");

    Mockito.verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("PBT migration threads must be positive, got 0");
  }

  @Test
  public void negativeRollPauseIsRejected() {
    parseCommand("--Xpbt-migration-roll-pause", "-1");

    Mockito.verifyNoInteractions(mockRunnerBuilder);
    assertThat(commandErrorOutput.toString(UTF_8))
        .contains("PBT migration roll pause must not be negative");
  }

  private PbtMigrator.Tuning tuning() {
    final ArgumentCaptor<PbtMigrator.Tuning> tuning =
        ArgumentCaptor.forClass(PbtMigrator.Tuning.class);
    verify(mockControllerBuilder).pbtMigrationTuning(tuning.capture());
    return tuning.getValue();
  }
}
