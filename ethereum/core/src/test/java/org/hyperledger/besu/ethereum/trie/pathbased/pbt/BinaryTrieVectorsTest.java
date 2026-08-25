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
package org.hyperledger.besu.ethereum.trie.pathbased.pbt;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary.DefaultBinaryStateRootCommitter;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.BasicDataEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.CodeChunkifier;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.StoredPartitionedBinaryTrie;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.BonsaiTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies Besu's partitioned binary trie against the official execution-specs test vectors
 * (projects/binary-trie/tests/binary_trie/vectors/binary_trie_vectors.json).
 *
 * <p>Covers all five sections of the vectors file: {@code trie_roots}, {@code embedding}, {@code
 * chunkify_code}, {@code encode_basic_data}, and {@code pbt_state}.
 */
class BinaryTrieVectorsTest {

  private static final GenesisConfig EMPTY_BINARY_GENESIS =
      GenesisConfig.fromResource(
          "/org/hyperledger/besu/ethereum/trie/pathbased/pbt/empty-amsterdam-genesis.json");

  private static final String VECTORS_RESOURCE = "binary_trie_vectors.json";
  private static final NodeLoader EMPTY_LOADER = (location, hash) -> Optional.empty();
  private static final NodeUpdater NOOP_UPDATER = (location, hash, value) -> {};

  // --------------------------------------------------------------------------------------------
  // trie_roots
  // --------------------------------------------------------------------------------------------

  static List<Object[]> vectors() throws IOException {
    try (InputStream in = BinaryTrieVectorsTest.class.getResourceAsStream(VECTORS_RESOURCE)) {
      assertThat(in).as("vectors resource %s", VECTORS_RESOURCE).isNotNull();
      final JsonNode root = new ObjectMapper().readTree(in);
      final JsonNode trieRoots = root.get("trie_roots");
      final List<Object[]> out = new ArrayList<>();
      for (final JsonNode vector : trieRoots) {
        final String name = vector.get("name").asText();
        final String expected = vector.get("root").asText();
        final List<String[]> entries = new ArrayList<>();
        for (final JsonNode entry : vector.get("entries")) {
          entries.add(new String[] {entry.get("key").asText(), entry.get("value").asText()});
        }
        out.add(new Object[] {name, entries, expected});
      }
      return out;
    }
  }

  @ParameterizedTest(name = "trie_roots:{0}")
  @MethodSource("vectors")
  void matchesOfficialVector(
      final String name, final List<String[]> entries, final String expectedHex) {
    final StoredPartitionedBinaryTrie trie =
        new StoredPartitionedBinaryTrie(EMPTY_LOADER, Bytes32.ZERO);
    for (final String[] entry : entries) {
      trie.put(Bytes.fromHexString(entry[0]), Bytes.fromHexString(entry[1]));
    }
    trie.commit(NOOP_UPDATER);
    final Bytes32 actual = trie.getRootHash();
    assertThat(actual).as(name).isEqualTo(Bytes32.fromHexString(expectedHex));
  }

  // --------------------------------------------------------------------------------------------
  // embedding
  // --------------------------------------------------------------------------------------------

  @Test
  void embeddingMatchesVector() throws IOException {
    final JsonNode emb = readRoot().get("embedding");

    final Bytes address20 = Bytes.fromHexString(emb.get("address20").asText());
    final Bytes32 expectedAddress32 = Bytes32.fromHexString(emb.get("address32").asText());
    final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address20);
    assertThat(address32).as("address20ToAddress32").isEqualTo(expectedAddress32);

    assertThat(TrieKeyDerivation.getTreeKeyForBasicData(address32))
        .as("basic_data_key")
        .isEqualTo(Bytes.fromHexString(emb.get("basic_data_key").asText()));
    assertThat(TrieKeyDerivation.getTreeKeyForCodeHash(address32))
        .as("code_hash_key")
        .isEqualTo(Bytes.fromHexString(emb.get("code_hash_key").asText()));
    assertThat(TrieKeyDerivation.getTreeKeyForDelegation(address32))
        .as("delegation_key")
        .isEqualTo(Bytes.fromHexString(emb.get("delegation_key").asText()));

    final Bytes32 codeHash = Bytes32.fromHexString(emb.get("code_hash").asText());
    final JsonNode storageSlotKeys = emb.get("storage_slot_keys");
    final Iterator<Map.Entry<String, JsonNode>> storageIt = storageSlotKeys.fields();
    while (storageIt.hasNext()) {
      final Map.Entry<String, JsonNode> e = storageIt.next();
      final UInt256 slot = UInt256.valueOf(new java.math.BigInteger(e.getKey()));
      final Bytes expected = Bytes.fromHexString(e.getValue().asText());
      assertThat(TrieKeyDerivation.getTreeKeyForStorageSlot(address32, slot))
          .as("storage_slot_keys[%s]", e.getKey())
          .isEqualTo(expected);
    }

    final JsonNode codeChunkKeys = emb.get("code_chunk_keys");
    final Iterator<Map.Entry<String, JsonNode>> chunkIt = codeChunkKeys.fields();
    while (chunkIt.hasNext()) {
      final Map.Entry<String, JsonNode> e = chunkIt.next();
      final int chunkIndex = Integer.parseInt(e.getKey());
      final Bytes expected = Bytes.fromHexString(e.getValue().asText());
      assertThat(TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash, chunkIndex))
          .as("code_chunk_keys[%s]", e.getKey())
          .isEqualTo(expected);
    }
  }

  // --------------------------------------------------------------------------------------------
  // chunkify_code
  // --------------------------------------------------------------------------------------------

  static List<Object[]> chunkifyCodeVectors() throws IOException {
    final JsonNode section = readRoot().get("chunkify_code");
    final List<Object[]> out = new ArrayList<>();
    for (final JsonNode vector : section) {
      final List<String> chunks = new ArrayList<>();
      for (final JsonNode chunk : vector.get("chunks")) {
        chunks.add(chunk.asText());
      }
      out.add(new Object[] {vector.get("name").asText(), vector.get("code").asText(), chunks});
    }
    return out;
  }

  @ParameterizedTest(name = "chunkify_code:{0}")
  @MethodSource("chunkifyCodeVectors")
  void chunkifyCodeMatchesVector(
      final String name, final String codeHex, final List<String> expectedChunksHex) {
    final List<Bytes32> actual = CodeChunkifier.chunkifyCode(Bytes.fromHexString(codeHex));
    assertThat(actual).as(name).hasSize(expectedChunksHex.size());
    for (int i = 0; i < expectedChunksHex.size(); i++) {
      assertThat(actual.get(i))
          .as("%s chunk[%d]", name, i)
          .isEqualTo(Bytes32.fromHexString(expectedChunksHex.get(i)));
    }
  }

  // --------------------------------------------------------------------------------------------
  // encode_basic_data
  // --------------------------------------------------------------------------------------------

  static List<Object[]> encodeBasicDataVectors() throws IOException {
    final JsonNode section = readRoot().get("encode_basic_data");
    final List<Object[]> out = new ArrayList<>();
    for (final JsonNode vector : section) {
      // asLong() truncates values beyond Long.MAX_VALUE to their low 64 bits (e.g. 2^64-1 -> -1),
      // matching the 8-byte big-endian nonce layout used by BasicDataEncoder.
      out.add(
          new Object[] {
            vector.get("code_size").asLong(),
            vector.get("nonce").asLong(),
            vector.get("balance").asText(),
            vector.get("encoded").asText()
          });
    }
    return out;
  }

  @ParameterizedTest(name = "encode_basic_data[code_size={0},nonce={1}]")
  @MethodSource("encodeBasicDataVectors")
  void encodeBasicDataMatchesVector(
      final long codeSize, final long nonce, final String balanceHex, final String expectedHex) {
    final Bytes32 actual =
        BasicDataEncoder.encodeBasicData(codeSize, nonce, UInt256.fromHexString(balanceHex));
    assertThat(actual).as("encoded").isEqualTo(Bytes32.fromHexString(expectedHex));
  }

  // --------------------------------------------------------------------------------------------
  // pbt_state
  // --------------------------------------------------------------------------------------------

  static List<Object[]> pbtStateVectors() throws IOException {
    final JsonNode section = readRoot().get("pbt_state");
    final List<Object[]> out = new ArrayList<>();
    for (final JsonNode vector : section) {
      out.add(
          new Object[] {
            vector.get("name").asText(), vector.get("accounts"), vector.get("root").asText()
          });
    }
    return out;
  }

  @ParameterizedTest(name = "pbt_state:{0}")
  @MethodSource("pbtStateVectors")
  void pbtStateMatchesVector(
      final String name, final JsonNode accountsNode, final String expectedHex) throws IOException {
    // Drive the REAL BinaryStateRootCommitter against a BINARY BonsaiWorldState so the
    // conformance vectors exercise the production code path (not a spec reimplementation
    // via StoredPartitionedBinaryTrie). Each vector starts from a fresh empty binary trie.
    final ExecutionContextTestFixture contextTestFixture =
        ExecutionContextTestFixture.builder(EMPTY_BINARY_GENESIS)
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    try (final var ignored = contextTestFixture.getStateArchive()) {
      final ProtocolContext protocolContext = contextTestFixture.getProtocolContext();
      final BlockHeader chainHeadHeader = contextTestFixture.getBlockchain().getChainHeadHeader();

      final Hash actual;
      try (BonsaiWorldState worldState =
          (BonsaiWorldState)
              protocolContext
                  .getWorldStateArchive()
                  .getWorldState(
                      WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(chainHeadHeader))
                  .orElseThrow()) {
        final BonsaiWorldStateUpdateAccumulator accumulator = worldState.updater();
        applyVectorAccounts(accumulator, accountsNode);
        accumulator.commit();
        actual =
            new DefaultBinaryStateRootCommitter().compute(worldState, null, accumulator).root();
      }
      assertThat(actual).as(name).isEqualTo(Hash.fromHexString(expectedHex));
    }
  }

  @ParameterizedTest(name = "pbt_state_rollback_rollforward:{0}")
  @MethodSource("pbtStateVectors")
  void pbtStateMatchesVectorAfterRollbackAndRollforward(
      final String name, final JsonNode accountsNode, final String expectedHex) throws IOException {
    // Drive the REAL BinaryStateRootCommitter through a rollback-then-rollforward cycle against a
    // BINARY BonsaiWorldState. This validates that the PBT trie log (which carries the slot-key
    // preimage via BonsaiTrieLogFactory) round-trips the binary state: apply -> expected root,
    // rollback -> empty root, rollforward -> expected root again.
    final ExecutionContextTestFixture contextTestFixture =
        ExecutionContextTestFixture.builder(EMPTY_BINARY_GENESIS)
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .build();
    try (final var ignored = contextTestFixture.getStateArchive()) {
      final ProtocolContext protocolContext = contextTestFixture.getProtocolContext();
      final BlockHeader chainHeadHeader = contextTestFixture.getBlockchain().getChainHeadHeader();

      final Hash expected = Hash.fromHexString(expectedHex);
      final DefaultBinaryStateRootCommitter binaryCommitter = new DefaultBinaryStateRootCommitter();

      // Obtain the non-frozen head world state (withBlockHeaderAndUpdateNodeHead) so persist()
      // writes the flat-db account info; a frozen world state (withBlockHeaderAndNoUpdateNodeHead)
      // skips those writes and the account becomes unreadable for the subsequent rollBack.
      try (BonsaiWorldState worldState =
          (BonsaiWorldState)
              protocolContext
                  .getWorldStateArchive()
                  .getWorldState(
                      WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(chainHeadHeader))
                  .orElseThrow()) {
        final BonsaiWorldStateUpdateAccumulator accumulator = worldState.updater();

        // Step 1: apply the vector's accounts and compute the root via the committer.
        applyVectorAccounts(accumulator, accountsNode);
        accumulator.commit();
        // Fire the copy BEFORE the computes so the clone captures the accumulator's pre-compute
        // state (including its introducedCodeHashes set) and the pre-compute does not pollute the
        // original accumulator passed to persist.
        final BonsaiWorldStateUpdateAccumulator clone = accumulator.copy();
        final Hash rootAfterApply = binaryCommitter.compute(worldState, null, clone).root();
        assertThat(rootAfterApply).as("%s: apply", name).isEqualTo(expected);

        // Persist with a header whose stateRoot matches the computed root so a PBT trie log layer
        // is captured (keyed by the header's block hash).
        final BlockHeader persistHeader =
            new BlockHeaderTestFixture()
                .parentHash(chainHeadHeader.getHash())
                .number(chainHeadHeader.getNumber() + 1)
                .stateRoot(rootAfterApply)
                .buildHeader();
        worldState.persist(persistHeader, binaryCommitter);

        // Read the captured trie log layer back from the world state's storage.
        final TrieLogLayer layer =
            worldState
                .getWorldStateStorage()
                .getTrieLog(persistHeader.getBlockHash())
                .map(
                    bytes ->
                        BonsaiTrieLogFactory.readFrom(
                            new BytesValueRLPInput(Bytes.wrap(bytes), false)))
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Missing trie log for " + persistHeader.getBlockHash()));

        // Step 2: rollback the layer. The state must return to the prior (empty) root.
        // persist() resets the accumulator, so re-fetch it before rolling.
        final BonsaiWorldStateUpdateAccumulator afterRollbackUpdater = worldState.updater();
        afterRollbackUpdater.rollBack(layer);
        afterRollbackUpdater.commit();
        worldState.persist(null, binaryCommitter);
        final Hash rootAfterRollback = worldState.rootHash();
        assertThat(rootAfterRollback).as("%s: rollback to empty root", name).isEqualTo(Hash.ZERO);

        // Step 3: rollforward the same layer. The state must return to the expected root.
        final BonsaiWorldStateUpdateAccumulator afterRollforwardUpdater = worldState.updater();
        afterRollforwardUpdater.rollForward(layer);
        afterRollforwardUpdater.commit();
        worldState.persist(null, binaryCommitter);
        final Hash rootAfterRollforward = worldState.rootHash();
        assertThat(rootAfterRollforward)
            .as("%s: rollforward to expected root", name)
            .isEqualTo(expected);
      }
    }
  }

  /**
   * Applies the accounts of a {@code pbt_state} vector to a world updater. EIP-7702 delegation
   * accounts (code == {@code 0xef0100} + 20 bytes) are set as-is; the committer detects delegation
   * via {@code CodeDelegationHelper.hasCodeDelegation} and stores the header DELEGATION leaf
   * instead of CODE_ZONE chunks.
   */
  private static void applyVectorAccounts(final WorldUpdater updater, final JsonNode accountsNode) {
    final Iterator<Map.Entry<String, JsonNode>> accounts = accountsNode.fields();
    while (accounts.hasNext()) {
      final Map.Entry<String, JsonNode> accountEntry = accounts.next();
      final Address address = Address.fromHexString(accountEntry.getKey());
      final JsonNode account = accountEntry.getValue();
      final MutableAccount mutableAccount = updater.getOrCreate(address);
      mutableAccount.setNonce(account.get("nonce").asLong());
      mutableAccount.setBalance(Wei.fromHexString(account.get("balance").asText()));
      final Bytes code = Bytes.fromHexString(account.get("code").asText());
      if (!code.isEmpty()) {
        mutableAccount.setCode(code);
      }
      final JsonNode storage = account.get("storage");
      final Iterator<Map.Entry<String, JsonNode>> storageIt = storage.fields();
      while (storageIt.hasNext()) {
        final Map.Entry<String, JsonNode> slot = storageIt.next();
        final UInt256 slotKey = UInt256.valueOf(new java.math.BigInteger(slot.getKey()));
        final UInt256 slotValue = UInt256.fromHexString(slot.getValue().asText());
        mutableAccount.setStorageValue(slotKey, slotValue);
      }
    }
  }

  // --------------------------------------------------------------------------------------------
  // helpers
  // --------------------------------------------------------------------------------------------

  private static JsonNode readRoot() throws IOException {
    try (InputStream in = BinaryTrieVectorsTest.class.getResourceAsStream(VECTORS_RESOURCE)) {
      assertThat(in).as("vectors resource %s", VECTORS_RESOURCE).isNotNull();
      return new ObjectMapper().readTree(in);
    }
  }
}
