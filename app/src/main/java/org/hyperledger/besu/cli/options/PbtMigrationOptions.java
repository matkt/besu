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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.PbtMigrator;

import java.nio.file.Path;
import java.util.Optional;

import picocli.CommandLine;

/** Command-line options for the EIP-8347 PBT migration (snapshot bootstrap). */
public class PbtMigrationOptions {
  /** Default constructor. */
  public PbtMigrationOptions() {}

  @CommandLine.Option(
      names = {"--Xpbt-snapshot-file"},
      hidden = true,
      paramLabel = "<PATH>",
      description =
          "EIP-8347 PBT snapshot to start the PBT migrator from instead of genesis. Requires "
              + "--Xpbt-preimages-file and --Xpbt-snapshot-anchor-block-hash.")
  Path snapshotFile = null;

  @CommandLine.Option(
      names = {"--Xpbt-preimages-file"},
      hidden = true,
      paramLabel = "<PATH>",
      description = "EIP-8347 preimage file matching --Xpbt-snapshot-file.")
  Path preimagesFile = null;

  @CommandLine.Option(
      names = {"--Xpbt-snapshot-anchor-block-hash"},
      hidden = true,
      paramLabel = "<HASH>",
      description = "Finalized pre-fork block the snapshot and preimages are anchored to.")
  String anchorBlockHash = null;

  /**
   * The snapshot bootstrap, if configured.
   *
   * @param dataDir the node's data directory, under which verification spills
   * @return the bootstrap, empty when none of the options is set
   * @throws IllegalArgumentException when only some of the options are set
   */
  public Optional<PbtMigrator.SnapshotBootstrap> toDomainObject(final Path dataDir) {
    final int set =
        (snapshotFile != null ? 1 : 0)
            + (preimagesFile != null ? 1 : 0)
            + (anchorBlockHash != null ? 1 : 0);
    if (set == 0) {
      return Optional.empty();
    }
    if (set != 3) {
      throw new IllegalArgumentException(
          "--Xpbt-snapshot-file, --Xpbt-preimages-file and --Xpbt-snapshot-anchor-block-hash "
              + "must be set together");
    }
    return Optional.of(
        new PbtMigrator.SnapshotBootstrap(
            snapshotFile,
            preimagesFile,
            Hash.fromHexString(anchorBlockHash),
            PbtMigrator.SnapshotBootstrap.workDir(dataDir)));
  }
}
