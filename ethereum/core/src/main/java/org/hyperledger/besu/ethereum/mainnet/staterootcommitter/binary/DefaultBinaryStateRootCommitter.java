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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter.binary;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.StateRootComputations;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.BinaryTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;
import org.hyperledger.besu.plugin.services.worldstate.StateRootComputation;
import org.hyperledger.besu.plugin.services.worldstate.TrieBranchType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Path-based state-root committer that materializes accumulator updates into the partitioned binary
 * trie (EIP-8297) via {@link BinaryTrieWriter}.
 *
 * <p><b>Account deletion</b> ({@code updated == null}): when {@code prior != null}, clears account
 * header leaves and removes flat-db account info. Storage leaves and {@code CODE_ZONE} chunks are
 * not removed here (post-EIP-6780 full account deletion is rare; storage clears arrive via the
 * storage accumulator when in scope).
 */
public class DefaultBinaryStateRootCommitter implements StateRootCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultBinaryStateRootCommitter.class);

  @Override
  public TrieBranchType getTrieBranchType() {
    return TrieBranchType.BINARY;
  }

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
    private final BinaryTrieWriter writer;

    private BinaryComputation(
        final BonsaiWorldState bonsai,
        final BonsaiWorldStateUpdateAccumulator worldStateUpdater,
        final boolean storageFrozen) {
      this.bonsai = bonsai;
      this.worldStateUpdater = worldStateUpdater;
      this.writer =
          new BinaryTrieWriter(
              bonsai,
              storageFrozen,
              worldStateUpdater.getIntroducedCodeHashes(),
              BinaryTrieFactory.createStateTrie(bonsai));
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

      final Hash root = writer.commit();
      writeSink.addAll(writer.writes());
      LOG.atInfo().setMessage("DIRECT binary state root computed: root={}").addArgument(root).log();
      return root;
    }

    private void applyAccount(final Address address) {
      final BonsaiValue<BonsaiAccount> accountUpdate =
          worldStateUpdater.getAccountsToUpdate().get(address);
      if (accountUpdate == null) {
        return;
      }

      final BonsaiAccount priorAccount = accountUpdate.getPrior();
      final BonsaiAccount updatedAccount = accountUpdate.getUpdated();

      // Handle deletion before isUnchanged(): prior == updated == null is "unchanged" but still a
      // clear. Flat-DB and trie stay consistent, so cleanup runs only when prior exists.
      if (updatedAccount == null) {
        if (priorAccount != null) {
          writer.clearAccountHeader(address, resolvePriorCode(address, priorAccount));
        }
        return;
      }

      if (accountUpdate.isUnchanged()) {
        return;
      }

      final Bytes priorCode = resolvePriorCode(address, priorAccount);
      final Bytes updatedCode = resolveUpdatedCode(address, updatedAccount);
      writer.putAccountHeader(
          address,
          priorAccount != null,
          priorAccount != null ? priorAccount.getCodeHash() : Hash.EMPTY,
          priorCode,
          updatedAccount.getNonce(),
          updatedAccount.getBalance(),
          updatedCode,
          updatedAccount.getCodeHash(),
          RLP.encode(
              new BinaryTrieAccountValue(
                      updatedAccount.getNonce(),
                      updatedAccount.getBalance(),
                      updatedAccount.getCodeHash())
                  ::writeTo));
    }

    private void applyCode(final Address address) {
      final BonsaiValue<Bytes> codeUpdate = worldStateUpdater.getCodeToUpdate().get(address);
      if (codeUpdate == null || codeUpdate.isUnchanged()) {
        return;
      }
      writer.putCode(address, codeUpdate.getPrior(), codeUpdate.getUpdated());
    }

    private void applyStorage(final Address address) {
      final StorageConsumingMap<StorageSlotKey, BonsaiValue<UInt256>> storageUpdates =
          worldStateUpdater.getStorageToUpdate().get(address);
      if (storageUpdates == null || storageUpdates.isEmpty()) {
        return;
      }

      for (final Map.Entry<StorageSlotKey, BonsaiValue<UInt256>> storageUpdate :
          storageUpdates.entrySet()) {
        if (storageUpdate.getValue().isUnchanged()) {
          continue;
        }
        writer.putStorageValue(
            address, storageUpdate.getKey(), storageUpdate.getValue().getUpdated());
      }
    }

    private Bytes resolvePriorCode(final Address address, final BonsaiAccount priorAccount) {
      final BonsaiValue<Bytes> codeUpdate = worldStateUpdater.getCodeToUpdate().get(address);
      if (codeUpdate != null) {
        return BinaryTrieWriter.isEmpty(codeUpdate.getPrior())
            ? Bytes.EMPTY
            : codeUpdate.getPrior();
      }
      if (priorAccount == null || Hash.EMPTY.equals(priorAccount.getCodeHash())) {
        return Bytes.EMPTY;
      }
      return bonsai.getCode(address, priorAccount.getCodeHash()).orElse(Bytes.EMPTY);
    }

    private Bytes resolveUpdatedCode(final Address address, final BonsaiAccount updatedAccount) {
      final BonsaiValue<Bytes> codeUpdate = worldStateUpdater.getCodeToUpdate().get(address);
      if (codeUpdate != null) {
        return BinaryTrieWriter.isEmpty(codeUpdate.getUpdated())
            ? Bytes.EMPTY
            : codeUpdate.getUpdated();
      }
      return resolveCurrentCode(address, updatedAccount.getCodeHash()).orElse(Bytes.EMPTY);
    }

    private Optional<Bytes> resolveCurrentCode(final Address address, final Hash codeHash) {
      final Optional<Bytes> pendingCode = worldStateUpdater.getCode(address, codeHash);
      return pendingCode.isPresent() ? pendingCode : bonsai.getCode(address, codeHash);
    }
  }
}
