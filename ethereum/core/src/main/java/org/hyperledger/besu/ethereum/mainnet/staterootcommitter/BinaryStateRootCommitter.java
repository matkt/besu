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

import static org.hyperledger.besu.evm.worldstate.CodeDelegationHelper.hasCodeDelegation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockProcessingExecutors;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.BasicDataEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.CodeChunkifier;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.DelegationEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.ParallelStoredPartitionedBinaryTrie;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.StoredPartitionedBinaryTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
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
 * <p><b>Account deletion</b> ({@code updated == null}): when {@code prior != null}, clears account
 * header leaves ({@code BASIC_DATA}, and whichever of {@code CODE_HASH}/{@code DELEGATION} was
 * present) and removes flat-db account info. Storage leaves and {@code CODE_ZONE} chunks are not
 * removed here (post-EIP-6780 full account deletion is rare; storage clears arrive via the storage
 * accumulator when in scope).
 *
 * <p><b>Code updates</b>: {@code CODE_ZONE} chunks are content-addressed and may be shared across
 * accounts (EIP-8297). This committer never removes code-chunk leaves from the trie; it only writes
 * new contract code via {@link BinaryComputation#putCodeLeaves}. EIP-7702 delegation indicators are
 * stored in the account header {@code DELEGATION} leaf instead of {@code CODE_ZONE}.
 *
 * <p><b>Delegation vs code-hash</b>: an existing account holds exactly one of {@code
 * CODE_HASH_LEAF_KEY} and {@code DELEGATION_LEAF_KEY}. Mutual-exclusion removes run only on a real
 * mode transition (delegation ↔ code-hash), not on every account write.
 */
public class BinaryStateRootCommitter implements StateRootCommitter {

  @Override
  public StateRootComputation compute(
      final MutableWorldState mutableWorldState,
      final BlockHeader blockHeader,
      final WorldUpdater worldUpdater) {
    final BonsaiWorldStateUpdateAccumulator accumulator =
        (BonsaiWorldStateUpdateAccumulator)
            Objects.requireNonNull(
                worldUpdater, "Path-based state root committers require a non-null WorldUpdater");
    final BonsaiWorldState bonsai = (BonsaiWorldState) mutableWorldState;
    final boolean storageFrozen = mutableWorldState.isStorageFrozen();
    final List<StateRootComputations.UpdaterWrite> writes = new ArrayList<>();
    final Hash root = new BinaryComputation(bonsai, accumulator, storageFrozen).executeInto(writes);
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
              (location, hash) -> bonsai.getWorldStateStorage().getTrieNode(location, hash),
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
                writes.add(updater -> updater.putTrieNode(location, hash, value)));
      }
      writeSink.addAll(writes);
      System.out.println(
          "Used ParallelStoredPartitionedBinaryTrie for  " + stateTrie.getRootHash());
      return Hash.wrap(stateTrie.getRootHash());
    }

    private void applyAccount(final Address address) {
      final BonsaiValue<BonsaiAccount> accountUpdate =
          worldStateUpdater.getAccountsToUpdate().get(address);
      if (accountUpdate == null) {
        return;
      }

      final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
      final Bytes basicDataKey = TrieKeyDerivation.getTreeKeyForBasicData(address32);
      final Bytes codeHashKey = TrieKeyDerivation.getTreeKeyForCodeHash(address32);
      final Bytes delegationKey = TrieKeyDerivation.getTreeKeyForDelegation(address32);
      final Hash addressHash = address.addressHash();
      final BonsaiAccount priorAccount = accountUpdate.getPrior();
      final BonsaiAccount updatedAccount = accountUpdate.getUpdated();

      // Handle deletion before isUnchanged(): prior == updated == null is "unchanged" but still a
      // clear. Flat-DB and trie stay consistent, so cleanup runs only when prior exists.
      if (updatedAccount == null) {
        if (priorAccount != null) {
          clearAccountHeader(address, basicDataKey, codeHashKey, delegationKey, priorAccount);
          if (!storageFrozen) {
            writes.add(updater -> updater.removeAccountInfoState(addressHash));
          }
        }
        return;
      }

      if (accountUpdate.isUnchanged()) {
        return;
      }

      final Bytes priorCode = resolvePriorCode(address, priorAccount);
      final Bytes updatedCode = resolveUpdatedCode(address, updatedAccount);
      final boolean priorDelegation = hasCodeDelegation(priorCode);
      final boolean updatedDelegation = hasCodeDelegation(updatedCode);

      final long codeSize =
          updatedDelegation ? EmbeddingParameters.DELEGATION_CODE_SIZE : updatedCode.size();
      final Bytes32 basicData =
          BasicDataEncoder.encodeBasicData(
              codeSize, updatedAccount.getNonce(), updatedAccount.getBalance().toUInt256());
      // EIP-8297: a zero-valued leaf is absent from the tree. The basic-data encoding is all zeros
      // only when version=0, code_size=0, nonce=0, and balance=0 (e.g. an empty EOA); remove any
      // prior leaf instead of storing the zero value, mirroring storage-slot and code-chunk
      // zero-absent handling.
      if (Bytes32.ZERO.equals(basicData)) {
        stateTrie.remove(basicDataKey);
      } else {
        stateTrie.put(basicDataKey, basicData);
      }

      if (updatedDelegation) {
        final Bytes32 delegationValue =
            DelegationEncoder.encodeDelegation(
                CodeDelegationHelper.getTargetAddress(updatedCode).getBytes());
        if (!priorDelegation || !delegationLeafUnchanged(priorCode, updatedCode)) {
          stateTrie.put(delegationKey, delegationValue);
        }
        // Mutual exclusion: only when leaving code-hash mode for an existing account.
        if (priorAccount != null && !priorDelegation) {
          stateTrie.remove(codeHashKey);
        }
      } else {
        final Hash updatedCodeHash = updatedAccount.getCodeHash();
        final boolean codeHashChanged =
            priorAccount == null
                || priorDelegation
                || !updatedCodeHash.equals(priorAccount.getCodeHash());
        if (codeHashChanged) {
          stateTrie.put(codeHashKey, updatedCodeHash.getBytes());
        }
        // Mutual exclusion: only when leaving delegation mode.
        if (priorDelegation) {
          stateTrie.remove(delegationKey);
        }
      }

      if (!storageFrozen) {
        writes.add(
            updater -> updater.putAccountInfoState(addressHash, updatedAccount.serializeAccount()));
      }
    }

    private void clearAccountHeader(
        final Address address,
        final Bytes basicDataKey,
        final Bytes codeHashKey,
        final Bytes delegationKey,
        final BonsaiAccount priorAccount) {
      stateTrie.remove(basicDataKey);
      // Caller guarantees priorAccount != null; remove only the active header leaf.
      final Bytes priorCode = resolvePriorCode(address, priorAccount);
      if (hasCodeDelegation(priorCode)) {
        stateTrie.remove(delegationKey);
      } else {
        stateTrie.remove(codeHashKey);
      }
    }

    private void applyCode(final Address address) {
      final BonsaiValue<Bytes> codeUpdate = worldStateUpdater.getCodeToUpdate().get(address);
      if (codeUpdate == null
          || codeUpdate.isUnchanged()
          || (isEmpty(codeUpdate.getPrior()) && isEmpty(codeUpdate.getUpdated()))) {
        return;
      }

      final Hash accountHash = address.addressHash();
      final Bytes priorCode = codeUpdate.getPrior();
      final Bytes updatedCode = codeUpdate.getUpdated();
      final Hash priorCodeHash = isEmpty(priorCode) ? Hash.EMPTY : Hash.hash(priorCode);
      final Hash updatedCodeHash = isEmpty(updatedCode) ? Hash.EMPTY : Hash.hash(updatedCode);

      // Delegation indicators live in the header DELEGATION leaf (applyAccount), not CODE_ZONE, so
      // only real contract code is written to / removed from CODE_ZONE here.
      final boolean writingCode = !isEmpty(updatedCode);
      final boolean removingCode = isEmpty(updatedCode) && !isEmpty(priorCode);

      if (writingCode && recordIfNewlyIntroduced(updatedCodeHash)) {
        if (!hasCodeDelegation(updatedCode)) {
          putCodeLeaves(Bytes32.wrap(updatedCodeHash.getBytes()), updatedCode);
        }
        if (!storageFrozen) {
          writes.add(updater -> updater.putCode(accountHash, updatedCodeHash, updatedCode));
        }
      }

      if (removingCode && worldStateUpdater.getIntroducedCodeHashes().contains(priorCodeHash)) {
        if (!hasCodeDelegation(priorCode)) {
          // Drop the chunks only for a code hash newly introduced
          removeCodeLeaves(Bytes32.wrap(priorCodeHash.getBytes()), priorCode);
        }
        if (!storageFrozen) {
          writes.add(updater -> updater.removeCode(accountHash, priorCodeHash));
        }
      }
    }

    /**
     * Records {@code codeHash} as newly introduced by this block if it was absent from the parent
     * flat DB, and returns whether it was newly introduced. The flat-DB read sees the parent state
     * because flat-DB writes are queued until persist; the result is recorded in the accumulator's
     * {@code introducedCodeHashes} set so {@code PbtTrieLogFactory} can persist it for the rollback
     * chunk-removal decision.
     */
    private boolean recordIfNewlyIntroduced(final Hash codeHash) {
      if (worldStateUpdater.getIntroducedCodeHashes().contains(codeHash)) {
        return false;
      }
      if (bonsai.getWorldStateStorage().getCode(codeHash, null).isPresent()) {
        return false;
      }
      worldStateUpdater.getIntroducedCodeHashes().add(codeHash);
      return true;
    }

    /**
     * Removes the CODE_ZONE chunk leaves for {@code codeHash}, mirroring {@link #putCodeLeaves} in
     * reverse. Zero chunks are never written (EIP-8297 zero-absent), so they are skipped; only
     * non-zero chunk indices are removed.
     */
    private void removeCodeLeaves(final Bytes32 codeHash, final Bytes codeBytes) {
      final List<Bytes32> chunks = CodeChunkifier.chunkifyCode(codeBytes);
      for (int i = 0; i < chunks.size(); i++) {
        if (Bytes32.ZERO.equals(chunks.get(i))) {
          continue;
        }
        stateTrie.remove(TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash, i));
      }
    }

    private void applyStorage(final Address address) {
      final StorageConsumingMap<StorageSlotKey, BonsaiValue<UInt256>> storageUpdates =
          worldStateUpdater.getStorageToUpdate().get(address);
      if (storageUpdates == null || storageUpdates.isEmpty()) {
        return;
      }

      final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
      final Hash accountHash = address.addressHash();

      for (final Map.Entry<StorageSlotKey, BonsaiValue<UInt256>> storageUpdate :
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
          stateTrie.put(storageKey, Bytes32.leftPad(updatedStorage));
          if (!storageFrozen) {
            writes.add(
                updater ->
                    updater.putStorageValueBySlotHash(accountHash, slotHash, updatedStorage));
          }
        }
      }
    }

    private Bytes resolvePriorCode(final Address address, final BonsaiAccount priorAccount) {
      final BonsaiValue<Bytes> codeUpdate = worldStateUpdater.getCodeToUpdate().get(address);
      if (codeUpdate != null) {
        return isEmpty(codeUpdate.getPrior()) ? Bytes.EMPTY : codeUpdate.getPrior();
      }
      if (priorAccount == null || Hash.EMPTY.equals(priorAccount.getCodeHash())) {
        return Bytes.EMPTY;
      }
      // Read from flat storage, not the accumulator (getCode returns the updated value).
      return bonsai.getCode(address, priorAccount.getCodeHash()).orElse(Bytes.EMPTY);
    }

    private Bytes resolveUpdatedCode(final Address address, final BonsaiAccount updatedAccount) {
      final BonsaiValue<Bytes> codeUpdate = worldStateUpdater.getCodeToUpdate().get(address);
      if (codeUpdate != null) {
        return isEmpty(codeUpdate.getUpdated()) ? Bytes.EMPTY : codeUpdate.getUpdated();
      }
      return resolveCurrentCode(address, updatedAccount.getCodeHash()).orElse(Bytes.EMPTY);
    }

    private Optional<Bytes> resolveCurrentCode(final Address address, final Hash codeHash) {
      final Optional<Bytes> pendingCode = worldStateUpdater.getCode(address, codeHash);
      return pendingCode.isPresent() ? pendingCode : bonsai.getCode(address, codeHash);
    }

    private void putCodeLeaves(final Bytes32 codeHash, final Bytes codeBytes) {
      final List<Bytes32> chunks = CodeChunkifier.chunkifyCode(codeBytes);
      // Header mutual exclusion (delegation ↔ code-hash) is owned by applyAccount.
      for (int i = 0; i < chunks.size(); i++) {
        final Bytes32 chunk = chunks.get(i);
        final Bytes chunkKey = TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash, i);
        // EIP-8297: a zero chunk is absent from the tree.
        if (Bytes32.ZERO.equals(chunk)) {
          stateTrie.remove(chunkKey);
        } else {
          stateTrie.put(chunkKey, chunk);
        }
      }
    }

    private static boolean delegationLeafUnchanged(final Bytes priorCode, final Bytes updatedCode) {
      return CodeDelegationHelper.getTargetAddress(priorCode)
          .equals(CodeDelegationHelper.getTargetAddress(updatedCode));
    }

    private static boolean isEmpty(final Bytes value) {
      return value == null || value.isEmpty();
    }
  }
}
