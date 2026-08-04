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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockProcessingExecutors;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.BasicDataEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.CodeChunkifier;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.ParallelStoredPartitionedBinaryTrie;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.StoredPartitionedBinaryTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;
import org.hyperledger.besu.plugin.services.worldstate.StateRootComputation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Path-based state-root committer that materializes updates into the partitioned binary trie
 * (EIP-8297).
 *
 * <p><b>Account deletion</b> ({@code updated == null}) is out of scope for the current fork:
 * post-EIP-6780, full account deletion is extremely rare. When the path fires, only account header
 * leaves ({@code BASIC_DATA}, {@code CODE_HASH}) and flat-db account info are cleared; storage
 * leaves and {@code CODE_ZONE} chunks are not removed.
 *
 * <p><b>Code updates</b>: {@code CODE_ZONE} chunks are content-addressed and may be shared across
 * accounts (EIP-8297). This committer never removes code-chunk leaves from the trie; it only writes
 * new code via {@link BinaryComputation#putCodeLeaves}. The account header {@code code_hash} leaf
 * is updated through {@link BinaryComputation#applyAccount} when the account changes.
 */
public class BinaryStateRootCommitter implements StateRootCommitter {

  @Override
  public StateRootComputation compute(
      final MutableWorldState mutableWorldState,
      final BlockHeader blockHeader,
      final WorldUpdater worldUpdater) {
    final PathBasedWorldStateUpdateAccumulator<?> accumulator =
        (PathBasedWorldStateUpdateAccumulator<?>)
            Objects.requireNonNull(
                worldUpdater, "Path-based state root committers require a non-null WorldUpdater");
    final BonsaiWorldState bonsai = (BonsaiWorldState) mutableWorldState;
    final boolean storageFrozen = mutableWorldState.isStorageFrozen();
    final List<StateRootComputations.UpdaterWrite> writes = new ArrayList<>();
    final Hash root =
        new BinaryComputation(
                bonsai, (BonsaiWorldStateUpdateAccumulator) accumulator, storageFrozen)
            .executeInto(writes);
    if (blockHeader != null && bonsai.isTrieDisabled()) {
      return StateRootComputations.pathBased(blockHeader.getStateRoot(), writes);
    }
    return StateRootComputations.pathBased(root, writes);
  }

  private static final class BinaryComputation {
    private final BonsaiWorldState bonsai;
    private final BonsaiWorldStateUpdateAccumulator worldStateUpdater;
    private final boolean storageFrozen;
    private final StoredPartitionedBinaryTrie stateTrie;
    private final List<StateRootComputations.UpdaterWrite> writes = new ArrayList<>();

    private BinaryComputation(
        final BonsaiWorldState bonsai,
        final BonsaiWorldStateUpdateAccumulator worldStateUpdater,
        final boolean storageFrozen) {
      this.bonsai = bonsai;
      this.worldStateUpdater = worldStateUpdater;
      this.storageFrozen = storageFrozen;
      this.stateTrie =
          new ParallelStoredPartitionedBinaryTrie(
              bonsai.getWorldStateStorage()::getAccountStateTrieNode,
              Bytes32.wrap(bonsai.getWorldStateRootHash().getBytes()),
              BlockProcessingExecutors.accountTrieForkJoinPool());
    }

    Hash executeInto(final List<StateRootComputations.UpdaterWrite> writeSink) {
      final Set<Address> touchedAddresses = new HashSet<>();
      touchedAddresses.addAll(worldStateUpdater.getAccountsToUpdate().keySet());
      touchedAddresses.addAll(worldStateUpdater.getCodeToUpdate().keySet());
      touchedAddresses.addAll(worldStateUpdater.getStorageToUpdate().keySet());

      for (final Address address : touchedAddresses) {
        applyAccount(address);
        applyCode(address);
        applyStorage(address);
      }

      if (!storageFrozen) {
        stateTrie.commit(
            (location, hash, value) ->
                writes.add(updater -> updater.putAccountStateTrieNode(location, hash, value)));
      }
      writeSink.addAll(writes);
      return Hash.wrap(stateTrie.getRootHash());
    }

    private void applyAccount(final Address address) {
      final PathBasedValue<BonsaiAccount> accountUpdate =
          worldStateUpdater.getAccountsToUpdate().get(address);
      if (accountUpdate == null || accountUpdate.isUnchanged()) {
        return;
      }

      final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
      final Bytes basicDataKey = TrieKeyDerivation.getTreeKeyForBasicData(address32);
      final Bytes codeHashKey = TrieKeyDerivation.getTreeKeyForCodeHash(address32);
      final Hash addressHash = address.addressHash();

      // Out-of-scope: storage and CODE_ZONE cleanup (post-EIP-6780 account deletion is rare).
      if (accountUpdate.getUpdated() == null) {
        // should not exist
        return;
      }

      final BonsaiAccount updatedAccount = accountUpdate.getUpdated();
      final Bytes code =
          resolveCurrentCode(address, updatedAccount.getCodeHash()).orElse(Bytes.EMPTY);
      stateTrie.writeState(
          basicDataKey,
          BasicDataEncoder.encodeBasicData(
              code.size(), updatedAccount.getNonce(), updatedAccount.getBalance().toUInt256()));
      stateTrie.writeState(codeHashKey, updatedAccount.getCodeHash().getBytes());

      if (!storageFrozen) {
        writes.add(
            updater -> updater.putAccountInfoState(addressHash, updatedAccount.serializeAccount()));
      }
    }

    private void applyCode(final Address address) {
      final PathBasedValue<Bytes> codeUpdate = worldStateUpdater.getCodeToUpdate().get(address);
      if (codeUpdate == null
          || codeUpdate.isUnchanged()
          || (isEmpty(codeUpdate.getPrior()) && isEmpty(codeUpdate.getUpdated()))) {
        return;
      }

      final Hash accountHash = address.addressHash();
      final Hash priorCodeHash =
          codeUpdate.getPrior() == null ? Hash.EMPTY : Hash.hash(codeUpdate.getPrior());
      final Hash updatedCodeHash =
          codeUpdate.getUpdated() == null ? Hash.EMPTY : Hash.hash(codeUpdate.getUpdated());
      final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());

      // Never remove CODE_ZONE leaves: content-addressed code may be shared (EIP-8297).
      if (!isEmpty(codeUpdate.getUpdated())) {
        putCodeLeaves(address32, Bytes32.wrap(updatedCodeHash.getBytes()), codeUpdate.getUpdated());
      }

      if (!storageFrozen) {
        if (isEmpty(codeUpdate.getUpdated())) {
          writes.add(updater -> updater.removeCode(accountHash, priorCodeHash));
        } else {
          writes.add(
              updater -> updater.putCode(accountHash, updatedCodeHash, codeUpdate.getUpdated()));
        }
      }
    }

    private void applyStorage(final Address address) {
      final StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>> storageUpdates =
          worldStateUpdater.getStorageToUpdate().get(address);
      if (storageUpdates == null || storageUpdates.isEmpty()) {
        return;
      }

      final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
      final Hash accountHash = address.addressHash();

      for (final Map.Entry<StorageSlotKey, PathBasedValue<UInt256>> storageUpdate :
          storageUpdates.entrySet()) {
        if (storageUpdate.getValue().isUnchanged()) {
          continue;
        }

        final UInt256 updatedStorage = storageUpdate.getValue().getUpdated();
        final Hash slotHash = storageUpdate.getKey().getSlotHash();
        final Bytes storageKey =
            TrieKeyDerivation.getTreeKeyForStorageSlot(
                address32, storageUpdate.getKey().getSlotKey().orElseThrow());

        if (updatedStorage == null || UInt256.ZERO.equals(updatedStorage)) {
          stateTrie.remove(storageKey);
          if (!storageFrozen) {
            writes.add(updater -> updater.removeStorageValueBySlotHash(accountHash, slotHash));
          }
        } else {
          stateTrie.writeState(storageKey, Bytes32.leftPad(updatedStorage));
          if (!storageFrozen) {
            writes.add(
                updater ->
                    updater.putStorageValueBySlotHash(accountHash, slotHash, updatedStorage));
          }
        }
      }
    }

    private Optional<Bytes> resolveCurrentCode(final Address address, final Hash codeHash) {
      final Optional<Bytes> pendingCode = worldStateUpdater.getCode(address, codeHash);
      return pendingCode.isPresent() ? pendingCode : bonsai.getCode(address, codeHash);
    }

    private void putCodeLeaves(
        final Bytes32 address32, final Bytes32 codeHash, final Bytes codeBytes) {
      final List<Bytes32> chunks = CodeChunkifier.chunkifyCode(codeBytes);
      stateTrie.writeState(TrieKeyDerivation.getTreeKeyForCodeHash(address32), codeHash);
      for (int i = 0; i < chunks.size(); i++) {
        stateTrie.writeState(
            TrieKeyDerivation.getTreeKeyForCodeChunk(address32, codeHash, i), chunks.get(i));
      }
    }

    private static boolean isEmpty(final Bytes value) {
      return value == null || value.isEmpty();
    }
  }
}
