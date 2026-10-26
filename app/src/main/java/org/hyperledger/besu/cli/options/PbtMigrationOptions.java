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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.PbtMigrator;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

import picocli.CommandLine;

/** Command-line options for the EIP-8347 PBT migration: snapshot load, self-conversion, tuning. */
public class PbtMigrationOptions {
  /** Default constructor. */
  public PbtMigrationOptions() {}

  @CommandLine.Option(
      names = {"--Xpbt-snapshot-file"},
      hidden = true,
      paramLabel = "<PATH>",
      description =
          "EIP-8347 PBT snapshot the PBT migrator starts from. Requires "
              + "--Xpbt-preimages-file and --Xpbt-snapshot-anchor-block-hash.")
  Path snapshotFile = null;

  @CommandLine.Option(
      names = {"--Xpbt-migration-self-convert"},
      hidden = true,
      description =
          "Build the PBT from the node's own flat database at the latest finalized block, with "
              + "--Xpbt-preimages-file and the BALs since its anchor, instead of a snapshot. "
              + "Requires --Xpbt-preimages-file and --Xpbt-snapshot-anchor-block-hash.")
  boolean selfConvert = false;

  @CommandLine.Option(
      names = {"--Xpbt-preimages-file"},
      hidden = true,
      paramLabel = "<PATH>",
      description =
          "EIP-8347 preimage file matching --Xpbt-snapshot-file, or for "
              + "--Xpbt-migration-self-convert.")
  Path preimagesFile = null;

  @CommandLine.Option(
      names = {"--Xpbt-snapshot-anchor-block-hash"},
      hidden = true,
      paramLabel = "<HASH>",
      description = "Finalized pre-fork block the snapshot or the preimages are anchored to.")
  String anchorBlockHash = null;

  @CommandLine.Option(
      names = {"--Xpbt-migration-threads"},
      hidden = true,
      paramLabel = "<INT>",
      description =
          "Threads of the PBT migrator, for the snapshot load and the binary roots "
              + "(default: half the cores, ${DEFAULT-VALUE}).")
  int threads = PbtMigrator.Tuning.DEFAULT.threads();

  @CommandLine.Option(
      names = {"--Xpbt-migration-blocks-per-roll"},
      hidden = true,
      paramLabel = "<INT>",
      description = "Most blocks the PBT migrator applies in one roll (default: ${DEFAULT-VALUE}).")
  int blocksPerRoll = PbtMigrator.Tuning.DEFAULT.blocksPerRoll();

  @CommandLine.Option(
      names = {"--Xpbt-migration-roll-pause"},
      hidden = true,
      paramLabel = "<MS>",
      description =
          "Milliseconds the PBT migrator waits between two rolls while it catches up "
              + "(default: as long as the last roll took).")
  Long rollPauseMillis = null;

  @CommandLine.Option(
      names = {"--Xpbt-migration-sort-buffer"},
      hidden = true,
      paramLabel = "<MIB>",
      description =
          "Heap in MiB of each external sort of the PBT snapshot load (default: ${DEFAULT-VALUE}).")
  int sortBufferMib = (int) (PbtMigrator.Tuning.DEFAULT.sortBufferBytes() >> 20);

  @CommandLine.Option(
      names = {"--Xpbt-migration-from-genesis"},
      hidden = true,
      description =
          "Let the PBT migrator start from genesis when no snapshot is set. It then replays every "
              + "block, so each needs a trie log or a BAL (e.g. a devnet with BALs from genesis).")
  boolean fromGenesis = false;

  /**
   * Whether the PBT migrator may start from genesis when no snapshot is set.
   *
   * @return {@code true} when starting from genesis is enabled
   */
  public boolean migrationFromGenesis() {
    return fromGenesis;
  }

  /**
   * The snapshot bootstrap or self-conversion, if configured.
   *
   * @param dataDir the node's data directory, under which the migration spills
   * @return the bootstrap, empty when none of the options is set
   * @throws IllegalArgumentException when only some of the options are set
   */
  public Optional<PbtMigrator.SnapshotBootstrap> toDomainObject(final Path dataDir) {
    if (!isBootstrapSet()) {
      return Optional.empty();
    }
    final Hash anchor = Hash.fromHexString(anchorBlockHash);
    final Path workDir = PbtMigrator.SnapshotBootstrap.workDir(dataDir);
    return Optional.of(
        selfConvert
            ? PbtMigrator.SnapshotBootstrap.selfConversion(preimagesFile, anchor, workDir)
            : new PbtMigrator.SnapshotBootstrap(snapshotFile, preimagesFile, anchor, workDir));
  }

  /**
   * How much of the node the PBT migrator takes.
   *
   * @return the migrator's threads, roll size, pause between rolls and sort buffer
   * @throws IllegalArgumentException when a value is out of range
   */
  public PbtMigrator.Tuning toTuning() {
    return new PbtMigrator.Tuning(
        threads,
        blocksPerRoll,
        Optional.ofNullable(rollPauseMillis).map(Duration::ofMillis),
        (long) sortBufferMib << 20);
  }

  /**
   * Checks the options.
   *
   * @param commandLine the command line, to report an invalid option
   */
  public void validate(final CommandLine commandLine) {
    try {
      isBootstrapSet();
      toTuning();
    } catch (final IllegalArgumentException e) {
      throw new CommandLine.ParameterException(commandLine, e.getMessage());
    }
  }

  private boolean isBootstrapSet() {
    if (selfConvert && snapshotFile != null) {
      throw new IllegalArgumentException(
          "--Xpbt-migration-self-convert cannot be used with --Xpbt-snapshot-file");
    }
    final String source = selfConvert ? "--Xpbt-migration-self-convert" : "--Xpbt-snapshot-file";
    final int set =
        (selfConvert || snapshotFile != null ? 1 : 0)
            + (preimagesFile != null ? 1 : 0)
            + (anchorBlockHash != null ? 1 : 0);
    if (set != 0 && set != 3) {
      throw new IllegalArgumentException(
          source
              + ", --Xpbt-preimages-file and --Xpbt-snapshot-anchor-block-hash "
              + "must be set together");
    }
    if (set == 3 && fromGenesis) {
      throw new IllegalArgumentException(
          "--Xpbt-migration-from-genesis cannot be used with " + source);
    }
    return set == 3;
  }
}
