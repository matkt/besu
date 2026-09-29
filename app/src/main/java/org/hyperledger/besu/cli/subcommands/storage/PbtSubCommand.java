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
package org.hyperledger.besu.cli.subcommands.storage;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.convert.Eip8347SnapshotGenerator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.convert.Eip8347StateSource;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.verify.Eip8347DualCheckVerifier;
import org.hyperledger.besu.evm.worldstate.WorldState;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

/**
 * Storage subcommands for EIP-8347 partitioned-binary-trie (PBT) migration artifacts.
 *
 * <p>Exposes dual-check verify and offline snapshot generation (converter) from preimages + anchor
 * state.
 */
@Command(
    name = PbtSubCommand.COMMAND_NAME,
    description = "EIP-8347 PBT migration artifact commands.",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class,
    subcommands = {PbtSubCommand.Verify.class, PbtSubCommand.Convert.class})
public class PbtSubCommand implements Runnable {

  /** Command name. */
  public static final String COMMAND_NAME = "pbt";

  private static final Logger LOG = LoggerFactory.getLogger(PbtSubCommand.class);

  @ParentCommand private StorageSubCommand parentCommand;

  @CommandLine.Spec private CommandLine.Model.CommandSpec spec;

  /** Default constructor for picocli. */
  public PbtSubCommand() {}

  @Override
  public void run() {
    final PrintWriter out = spec.commandLine().getOut();
    spec.commandLine().usage(out);
  }

  /** Dual-check verify of an EIP-8347 snapshot + preimage against an anchor block. */
  @Command(
      name = "verify",
      description =
          "Verify an EIP-8347 PBT snapshot and preimage pair (dual-check) against the local chain's anchor block stateRoot. Exit 0 on accept, 1 on reject.",
      mixinStandardHelpOptions = true,
      versionProvider = VersionProvider.class)
  public static class Verify implements Runnable {

    @ParentCommand private PbtSubCommand parentCommand;

    @Option(
        names = {"--snapshot"},
        description = "Path to the EIP-8347 PBT snapshot artifact",
        required = true)
    private Path snapshotPath;

    @Option(
        names = {"--preimages"},
        description = "Path to the EIP-8347 preimage artifact",
        required = true)
    private Path preimagesPath;

    @Option(
        names = {"--anchor", "--anchor-block"},
        description = "Anchor block hash (0x...) or block number",
        required = true)
    private String anchor;

    @Override
    public void run() {
      checkNotNull(parentCommand);
      checkNotNull(parentCommand.parentCommand);
      final int code = verifyAndExitCode();
      if (code != 0) {
        System.exit(code);
      }
    }

    int verifyAndExitCode() {
      try (final BesuController controller =
          parentCommand.parentCommand.besuCommand.buildController()) {
        final Blockchain blockchain = controller.getProtocolContext().getBlockchain();
        final BlockHeader header = resolveAnchor(blockchain, anchor);
        final Bytes32 stateRoot = Bytes32.wrap(header.getStateRoot().getBytes());
        LOG.info(
            "EIP-8347 verify: snapshot={}, preimages={}, anchor={} (#{}, stateRoot={})",
            snapshotPath,
            preimagesPath,
            header.getBlockHash(),
            header.getNumber(),
            stateRoot.toHexString());
        Eip8347DualCheckVerifier.verify(snapshotPath, preimagesPath, stateRoot);
        LOG.info("EIP-8347 dual-check accepted");
        return 0;
      } catch (final Eip8347ArtifactVerificationException e) {
        LOG.error("EIP-8347 dual-check rejected: {}", e.getMessage());
        return 1;
      } catch (final Exception e) {
        LOG.error("EIP-8347 verify failed", e);
        return 2;
      }
    }

    static BlockHeader resolveAnchor(final Blockchain blockchain, final String anchor) {
      if (anchor.startsWith("0x") || anchor.startsWith("0X")) {
        final Hash hash = Hash.fromHexString(anchor);
        return blockchain
            .getBlockHeader(hash)
            .orElseThrow(() -> new IllegalArgumentException("unknown anchor block hash " + anchor));
      }
      final long number;
      try {
        number = Long.parseLong(anchor);
      } catch (final NumberFormatException e) {
        throw new IllegalArgumentException(
            "anchor must be a block hash (0x...) or decimal block number", e);
      }
      final Optional<BlockHeader> header = blockchain.getBlockHeader(number);
      return header.orElseThrow(
          () -> new IllegalArgumentException("unknown anchor block number " + number));
    }
  }

  /**
   * Offline EIP-8347 converter: preimages + local anchor world state → PBT snapshot. Preimages may
   * be supplied or derived from the genesis {@code alloc} (Bonsai has no keccak preimage store).
   */
  @Command(
      name = "convert",
      description =
          "Generate EIP-8347 artifacts: PBT snapshot from preimages + anchor world state. "
              + "Omit --preimages to derive the preimage file from genesis alloc (writes --preimages-out).",
      mixinStandardHelpOptions = true,
      versionProvider = VersionProvider.class)
  public static class Convert implements Runnable {

    @ParentCommand private PbtSubCommand parentCommand;

    @Option(
        names = {"--preimages"},
        description =
            "Path to an existing EIP-8347 preimage artifact (input). "
                + "Omit to derive preimages from genesis alloc (requires --preimages-out).")
    private Path preimagesPath;

    @Option(
        names = {"--preimages-out"},
        description =
            "Output path for the EIP-8347 preimage artifact. Required when --preimages is omitted; "
                + "optional copy when --preimages is supplied.")
    private Path preimagesOutPath;

    @Option(
        names = {"--snapshot"},
        description = "Output path for the generated EIP-8347 PBT snapshot",
        required = true)
    private Path snapshotPath;

    @Option(
        names = {"--anchor", "--anchor-block"},
        description = "Anchor block hash (0x...) or block number",
        required = true)
    private String anchor;

    @Override
    public void run() {
      checkNotNull(parentCommand);
      checkNotNull(parentCommand.parentCommand);
      final int code = convertAndExitCode();
      if (code != 0) {
        System.exit(code);
      }
    }

    int convertAndExitCode() {
      try (final BesuController controller =
          parentCommand.parentCommand.besuCommand.buildController()) {
        final Blockchain blockchain = controller.getProtocolContext().getBlockchain();
        final BlockHeader header = Verify.resolveAnchor(blockchain, anchor);
        final Hash stateRoot = header.getStateRoot();
        final Hash blockHash = header.getBlockHash();
        final Optional<WorldState> worldState =
            controller.getProtocolContext().getWorldStateArchive().get(stateRoot, blockHash);
        if (worldState.isEmpty()) {
          throw new IllegalArgumentException(
              "world state unavailable for anchor " + blockHash + " (stateRoot=" + stateRoot + ")");
        }

        final Path effectivePreimages = resolvePreimagesPath();
        LOG.info(
            "EIP-8347 convert: preimages={}, snapshot={}, anchor={} (#{}, stateRoot={})",
            effectivePreimages,
            snapshotPath,
            blockHash,
            header.getNumber(),
            stateRoot);
        final Eip8347SnapshotGenerator.Result result =
            Eip8347SnapshotGenerator.generate(
                effectivePreimages, Eip8347StateSource.of(worldState.get()), snapshotPath);
        // Dual-check: incomplete / surplus preimages must refuse (converter step 2).
        Eip8347DualCheckVerifier.verify(
            snapshotPath, effectivePreimages, Bytes32.wrap(stateRoot.getBytes()));
        LOG.info(
            "EIP-8347 convert accepted (leaves={}, pbtRoot={})",
            result.leafCount(),
            result.pbtRoot().toHexString());
        return 0;
      } catch (final Eip8347ArtifactVerificationException e) {
        LOG.error("EIP-8347 convert rejected: {}", e.getMessage());
        return 1;
      } catch (final Exception e) {
        LOG.error("EIP-8347 convert failed", e);
        return 2;
      }
    }

    private Path resolvePreimagesPath() throws Exception {
      if (preimagesPath != null) {
        if (preimagesOutPath != null
            && !preimagesPath.toAbsolutePath().equals(preimagesOutPath.toAbsolutePath())) {
          final Path parent = preimagesOutPath.toAbsolutePath().getParent();
          if (parent != null) {
            Files.createDirectories(parent);
          }
          Files.copy(preimagesPath, preimagesOutPath);
        }
        return preimagesPath;
      }
      if (preimagesOutPath == null) {
        throw new IllegalArgumentException(
            "convert requires --preimages (input) or --preimages-out (derive from genesis alloc)");
      }
      final GenesisConfig genesis = parentCommand.parentCommand.besuCommand.getGenesisConfig();
      final int records =
          Eip8347PreimageFile.writeGenesis(preimagesOutPath, genesis.streamAllocations());
      LOG.info(
          "EIP-8347 preimages derived from genesis alloc (records={}, path={})",
          records,
          preimagesOutPath);
      return preimagesOutPath;
    }
  }
}
