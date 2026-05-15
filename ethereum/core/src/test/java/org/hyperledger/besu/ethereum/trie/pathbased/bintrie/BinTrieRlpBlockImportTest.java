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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.util.RawBlockIterator;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test that imports RLP-encoded blocks against a BinTrie genesis and verifies the state
 * root after each block.
 */
class BinTrieRlpBlockImportTest {

  private static final Logger LOG = LoggerFactory.getLogger(BinTrieRlpBlockImportTest.class);

  private static final String GENESIS_RESOURCE =
      "/org/hyperledger/besu/ethereum/trie/pathbased/bintrie/bintrie-dev-genesis-funded.json";
  private static final String BLOCKS_RESOURCE =
      "/org/hyperledger/besu/ethereum/trie/pathbased/bintrie/bintrie-blocks.rlp";

  private ExecutionContextTestFixture fixture;

  @BeforeEach
  void setUp() {
    fixture =
        ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_RESOURCE))
            .dataStorageFormat(DataStorageFormat.BINTRIE)
            .dataStorageConfiguration(DataStorageConfiguration.DEFAULT_BINTRIE_CONFIG)
            .build();
  }

  @Test
  void testImportBlocksFromRlpAndVerifyStateRoot() throws IOException, URISyntaxException {
    final List<Block> blocks = loadBlocksFromRlp();
    assertThat(blocks).isNotEmpty();

    LOG.info("Loaded {} blocks from RLP file", blocks.size());

    int importedCount = 0;
    for (final Block block : blocks) {
      final long blockNumber = block.getHeader().getNumber();
      final Hash expectedStateRoot = block.getHeader().getStateRoot();
      if (blockNumber == 0) {
        LOG.info(
            "Skipping genesis block 0 (hash={}), verifying state root={}",
            block.getHash(),
            expectedStateRoot);
        final MutableWorldState genesisWorldState = fixture.getStateArchive().getWorldState();
        assertThat(genesisWorldState.rootHash())
            .as("Genesis state root should match block 0 state root in RLP file")
            .isEqualTo(expectedStateRoot);
        importedCount++;
        continue;
      }

      LOG.info(
          "Importing block {} (hash={}), expected stateRoot={}",
          blockNumber,
          block.getHash(),
          expectedStateRoot);

      final var protocolSpec = fixture.getProtocolSchedule().getByBlockHeader(block.getHeader());
      final BlockProcessingResult result =
          protocolSpec
              .getBlockValidator()
              .validateAndProcessBlock(
                  fixture.getProtocolContext(),
                  block,
                  HeaderValidationMode.LIGHT_SKIP_DETACHED,
                  HeaderValidationMode.NONE,
                  Optional.empty(),
                  true);

      if (result.isSuccessful()) {
        result
            .getYield()
            .ifPresent(
                processingOutputs -> {
                  fixture
                      .getBlockchain()
                      .appendBlock(
                          block,
                          processingOutputs.getReceipts(),
                          processingOutputs.getBlockAccessList());
                  fixture
                      .getStateArchive()
                      .getWorldState(
                          WorldStateQueryParams.newBuilder()
                              .withBlockHeader(block.getHeader())
                              .withShouldWorldStateUpdateHead(true)
                              .build());
                });
      }

      final MutableWorldState worldState = fixture.getStateArchive().getWorldState();
      final Hash actualStateRoot = worldState.rootHash();

      LOG.info(
          "Block {} import result={}, expected stateRoot={}, actual stateRoot={}",
          blockNumber,
          result.isSuccessful() ? "OK" : "FAILED",
          expectedStateRoot,
          actualStateRoot);

      assertThat(result.isSuccessful())
          .as("Block %d should be imported successfully", blockNumber)
          .isTrue();

      assertThat(actualStateRoot)
          .as("State root after block %d should match block header", blockNumber)
          .isEqualTo(expectedStateRoot);

      importedCount++;
    }

    LOG.info("Successfully imported and verified {} blocks", importedCount);
    assertThat(importedCount).isEqualTo(blocks.size());
  }

  @Test
  void testBlocksCanBeDecoded() throws IOException, URISyntaxException {
    final List<Block> blocks = loadBlocksFromRlp();
    assertThat(blocks).isNotEmpty();

    for (final Block block : blocks) {
      assertThat(block.getHeader()).isNotNull();
      assertThat(block.getHash()).isNotNull();
      assertThat(block.getHeader().getStateRoot()).isNotNull();

      LOG.info(
          "Block {}: hash={}, txCount={}, stateRoot={}",
          block.getHeader().getNumber(),
          block.getHash(),
          block.getBody().getTransactions().size(),
          block.getHeader().getStateRoot());
    }
  }

  private List<Block> loadBlocksFromRlp() throws IOException, URISyntaxException {
    final var blocksUrl = getClass().getResource(BLOCKS_RESOURCE);
    assertThat(blocksUrl).as("RLP blocks resource must exist at " + BLOCKS_RESOURCE).isNotNull();

    final Path blocksPath = Path.of(blocksUrl.toURI());
    final var blockHeaderFunctions =
        ScheduleBasedBlockHeaderFunctions.create(fixture.getProtocolSchedule());

    final List<Block> blocks = new ArrayList<>();
    try (final RawBlockIterator iterator = new RawBlockIterator(blocksPath, blockHeaderFunctions)) {
      while (iterator.hasNext()) {
        blocks.add(iterator.next());
      }
    }
    return blocks;
  }
}
