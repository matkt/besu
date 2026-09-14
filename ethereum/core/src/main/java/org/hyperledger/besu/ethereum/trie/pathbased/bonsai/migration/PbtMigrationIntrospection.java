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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BinaryTrieForkSupport;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;

/**
 * Builds the geth-shaped migration progress payload that {@code debug_migrationProgress} returns.
 *
 * <p>Besu has a single background direction (binary, via {@link PbtMigrator}) and no merkle-shadow
 * follower. {@code Merkle} is therefore always null. {@code Phase} becomes {@code done} only once
 * the migrator has retired <em>and</em> the fork block is finalized — matching the contract
 * pbt-devnet's completion check enforces.
 */
public final class PbtMigrationIntrospection {

  private PbtMigrationIntrospection() {}

  public static MigrationProgressResult progress(
      final BonsaiWorldStateKeyValueStorage storage,
      final Blockchain blockchain,
      final Optional<Long> binaryTrieMilestone) {
    if (binaryTrieMilestone.isEmpty()) {
      return MigrationProgressResult.inactive();
    }

    final BlockHeader head = blockchain.getChainHeadHeader();
    final boolean forkFinalized =
        blockchain
            .getFinalized()
            .flatMap(blockchain::getBlockHeader)
            .map(BlockHeader.class::cast)
            .filter(
                h ->
                    BinaryTrieForkSupport.isBinaryTrieActive(h.getTimestamp(), binaryTrieMilestone))
            .isPresent();

    if (storage.isPbtMigratorRetired() && forkFinalized) {
      return MigrationProgressResult.done();
    }

    final Optional<Hash> cursorHash = storage.getWorldStateBlockHash(TrieBranchType.BINARY);
    if (cursorHash.isEmpty() || cursorHash.get().equals(Hash.ZERO)) {
      return MigrationProgressResult.running(DirectionProgressResult.idle());
    }

    final Hash hash = cursorHash.get();
    final Optional<BlockHeader> cursorHeader =
        blockchain.getBlockHeader(hash).map(BlockHeader.class::cast);
    if (cursorHeader.isEmpty()) {
      return MigrationProgressResult.running(
          new DirectionProgressResult("following", 0L, hash.toHexString(), "", ""));
    }

    final BlockHeader cursor = cursorHeader.get();
    final String shadow =
        storage
            .getWorldStateRootHash(TrieBranchType.BINARY)
            .map(bytes -> Hash.wrap(Bytes32.wrap(bytes)))
            .map(Hash::toHexString)
            .orElse("");

    final String phase;
    if (storage.isPbtMigratorRetired()
        || BinaryTrieForkSupport.isBinaryTrieActive(head.getTimestamp(), binaryTrieMilestone)) {
      // Migrator parked at the last PMT ancestor; FCU owns the binary column.
      phase = "parked";
    } else if (cursor.getNumber() >= head.getNumber()) {
      phase = "synced";
    } else {
      phase = "following";
    }

    return MigrationProgressResult.running(
        new DirectionProgressResult(
            phase, cursor.getNumber(), cursor.getBlockHash().toHexString(), shadow, ""));
  }

  public static Optional<Hash> shadowStateRoot(
      final BonsaiWorldStateKeyValueStorage storage,
      final Blockchain blockchain,
      final Optional<Long> binaryTrieMilestone,
      final Hash blockHash,
      final Optional<PbtMigrator> migrator) {
    final Optional<BlockHeader> header =
        blockchain.getBlockHeader(blockHash).map(BlockHeader.class::cast);
    if (header.isEmpty() || binaryTrieMilestone.isEmpty()) {
      return Optional.empty();
    }

    // Blocks the migrator rolled through answer from its cache, which outlives the cursor.
    final Optional<Hash> recorded = migrator.flatMap(m -> m.shadowRootFor(blockHash));
    if (recorded.isPresent()) {
      return recorded;
    }

    // Otherwise the only shadow on hand is the column whose mode is opposite to the one the block
    // header commits, and its worldBlockHash/worldRoot metadata describes that column's cursor.
    final TrieBranchType shadowMode =
        BinaryTrieForkSupport.isBinaryTrieActive(header.get().getTimestamp(), binaryTrieMilestone)
            ? TrieBranchType.PATRICIA
            : TrieBranchType.BINARY;
    if (storage.getWorldStateBlockHash(shadowMode).filter(blockHash::equals).isEmpty()) {
      return Optional.empty();
    }
    return storage.getWorldStateRootHash(shadowMode).map(bytes -> Hash.wrap(Bytes32.wrap(bytes)));
  }
}
