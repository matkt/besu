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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter.patricia;

import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldView.encodeTrieValue;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockProcessingExecutors;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.BalStateRootCommitter;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.StateRootComputations;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Patricia BAL root: replays BAL changes onto the account trie and per-account storage tries,
 * then returns storage roots to patch into the EVM accumulator.
 */
public final class PatriciaBalEngine implements BalStateRootCommitter.Engine {

  public static final PatriciaBalEngine INSTANCE = new PatriciaBalEngine();

  private PatriciaBalEngine() {}

  @Override
  public boolean useBalOverlay() {
    return true;
  }

  @Override
  public BalStateRootCommitter.Result compute(
      final BonsaiWorldState parent,
      final BlockAccessListAccountLookup accountLookup,
      final boolean storageFrozen) {
    return new Computation(parent, accountLookup, storageFrozen).execute();
  }

  private static final class Computation {

    private final BonsaiWorldState worldState;
    private final BlockAccessListAccountLookup accountLookup;
    private final boolean storageFrozen;
    private final ConcurrentLinkedQueue<StateRootComputations.UpdaterWrite> writes =
        new ConcurrentLinkedQueue<>();
    private final Map<Address, Hash> storageRoots = new ConcurrentHashMap<>();
    private final Map<Address, CompletableFuture<Hash>> storageFutures = new ConcurrentHashMap<>();

    Computation(
        final BonsaiWorldState worldState,
        final BlockAccessListAccountLookup accountLookup,
        final boolean storageFrozen) {
      this.worldState = worldState;
      this.accountLookup = accountLookup;
      this.storageFrozen = storageFrozen;
    }

    /**
     * Runs the three-phase BAL commit:
     *
     * <ol>
     *   <li>Launch storage-trie updates concurrently for accounts with storage changes.
     *   <li>Resolve each changed account in the account trie via {@code putDeferred}.
     *   <li>Unless {@link #storageFrozen}, commit the account trie and collect deferred writes.
     * </ol>
     */
    BalStateRootCommitter.Result execute() {
      final MerkleTrie<Bytes, Bytes> accountTrie =
          PatriciaTrieFactory.createAccountStateTrie(worldState);

      for (final BlockAccessList.AccountChanges changes : accountLookup.accountChanges()) {
        if (!changes.storageChanges().isEmpty()) {
          final Address address = changes.address();
          final Hash accountHash = address.addressHash();
          storageFutures.put(
              address,
              CompletableFuture.supplyAsync(
                  () -> updateStorageTrie(address, accountHash, changes),
                  BlockProcessingExecutors.storageTrieExecutor()));
        }
      }

      for (final BlockAccessList.AccountChanges changes : accountLookup.accountChanges()) {
        if (changes.hasAnyChange()) {
          final Address address = changes.address();
          final Hash accountHash = address.addressHash();
          accountTrie.putDeferred(
              accountHash.getBytes(),
              existingRlp -> resolveAccount(accountHash, address, changes, existingRlp));
        }
      }

      if (!storageFrozen) {
        accountTrie.commit(
            (location, hash, value) -> writes.add(u -> u.putTrieNode(location, hash, value)));
      }
      return new BalStateRootCommitter.Result(
          StateRootComputations.pathBased(
              Hash.wrap(accountTrie.getRootHash()), new ArrayList<>(writes)),
          storageRoots,
          Set.of());
    }

    private Optional<Bytes> resolveAccount(
        final Hash accountHash,
        final Address address,
        final BlockAccessList.AccountChanges changes,
        final Optional<Bytes> maybeRlp) {

      final PmtStateTrieAccountValue priorAccount =
          maybeRlp.map(rlp -> PmtStateTrieAccountValue.readFrom(RLP.input(rlp))).orElse(null);

      final long newNonce;
      if (changes.nonceChanges().isEmpty()) {
        newNonce = priorAccount != null ? priorAccount.getNonce() : 0L;
      } else {
        newNonce = changes.nonceChanges().getLast().newNonce();
      }

      final Wei newBalance;
      if (changes.balanceChanges().isEmpty()) {
        newBalance = priorAccount != null ? priorAccount.getBalance() : Wei.ZERO;
      } else {
        newBalance = changes.balanceChanges().getLast().postBalance();
      }

      final Hash newCodeHash;
      if (changes.codeChanges().isEmpty()) {
        newCodeHash = priorAccount != null ? priorAccount.getCodeHash() : Hash.EMPTY;
      } else {
        final BlockAccessList.CodeChange codeChange = changes.codeChanges().getLast();
        newCodeHash = Hash.hash(codeChange.newCode());
        if (!storageFrozen) {
          if (codeChange.newCode().isEmpty()) {
            if (priorAccount != null && !Hash.EMPTY.equals(priorAccount.getCodeHash())) {
              final Hash priorCodeHash = priorAccount.getCodeHash();
              writes.add(updater -> updater.removeCode(accountHash, priorCodeHash));
            }
          } else {
            writes.add(updater -> updater.putCode(accountHash, newCodeHash, codeChange.newCode()));
          }
        }
      }

      final Hash newStorageRoot;
      if (changes.storageChanges().isEmpty()) {
        newStorageRoot =
            priorAccount != null ? priorAccount.getStorageRoot() : Hash.EMPTY_TRIE_HASH;
      } else {
        newStorageRoot = storageFutures.get(address).join();
      }
      storageRoots.put(address, newStorageRoot);

      final PmtStateTrieAccountValue updatedAccount =
          new PmtStateTrieAccountValue(newNonce, newBalance, newStorageRoot, newCodeHash);
      if (isAccountEmpty(updatedAccount)) {
        if (!storageFrozen) {
          writes.add(updater -> updater.removeAccountInfoState(accountHash));
        }
        return Optional.empty();
      } else {
        final Bytes encoded = RLP.encode(updatedAccount::writeTo);
        if (!storageFrozen) {
          writes.add(updater -> updater.putAccountInfoState(accountHash, encoded));
        }
        return Optional.of(encoded);
      }
    }

    private Hash updateStorageTrie(
        final Address address,
        final Hash accountHash,
        final BlockAccessList.AccountChanges accountChanges) {

      final Hash priorStorageRoot = priorStorageRoot(address);

      final MerkleTrie<Bytes, Bytes> storageTrie =
          PatriciaTrieFactory.createStorageTrie(worldState, accountHash, priorStorageRoot);

      for (final BlockAccessList.SlotChanges slotChanges : accountChanges.storageChanges()) {
        final Hash slotHash = slotChanges.slot().getSlotHash();
        final UInt256 rawValue = slotChanges.changes().getLast().newValue();
        final UInt256 value = rawValue == null ? UInt256.ZERO : rawValue;
        if (value.equals(UInt256.ZERO)) {
          if (!storageFrozen) {
            writes.add(updater -> updater.removeStorageValueBySlotHash(accountHash, slotHash));
          }
          storageTrie.remove(slotHash.getBytes());
        } else {
          if (!storageFrozen) {
            writes.add(updater -> updater.putStorageValueBySlotHash(accountHash, slotHash, value));
          }
          storageTrie.put(slotHash.getBytes(), encodeTrieValue(value));
        }
      }

      if (!storageFrozen) {
        storageTrie.commit(
            (location, nodeHash, value) ->
                writes.add(
                    u ->
                        u.putTrieNode(
                            Bytes.concatenate(accountHash.getBytes(), location), nodeHash, value)));
      }
      return Hash.wrap(storageTrie.getRootHash());
    }

    private Hash priorStorageRoot(final Address address) {
      return worldState
          .getWorldStateStorage()
          .getAccount(address.addressHash())
          .map(rlp -> PmtStateTrieAccountValue.readFrom(RLP.input(rlp)).getStorageRoot())
          .orElse(Hash.EMPTY_TRIE_HASH);
    }

    private boolean isAccountEmpty(final PmtStateTrieAccountValue account) {
      return account.getNonce() == 0
          && account.getBalance().isZero()
          && Hash.EMPTY_TRIE_HASH.equals(account.getStorageRoot())
          && Hash.EMPTY.equals(account.getCodeHash());
    }
  }
}
