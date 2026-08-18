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

import static org.hyperledger.besu.evm.worldstate.CodeDelegationHelper.hasCodeDelegation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.StateRootComputations;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.BasicDataEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.CodeChunkifier;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.DelegationEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.StoredPartitionedBinaryTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Shared EIP-8297 leaf writer for the partitioned binary trie obtained from {@link
 * BinaryTrieFactory}. Used by {@link DefaultBinaryStateRootCommitter} (accumulator) and {@link
 * BinaryBalEngine} (BAL).
 */
public final class BinaryTrieWriter {

  private final BonsaiWorldState worldState;
  private final boolean storageFrozen;
  private final StoredPartitionedBinaryTrie stateTrie;
  private final List<StateRootComputations.UpdaterWrite> writes = new ArrayList<>();
  private final Set<Hash> introducedCodeHashes;

  public BinaryTrieWriter(
      final BonsaiWorldState worldState,
      final boolean storageFrozen,
      final Set<Hash> introducedCodeHashes,
      final StoredPartitionedBinaryTrie stateTrie) {
    this.worldState = worldState;
    this.storageFrozen = storageFrozen;
    this.introducedCodeHashes = introducedCodeHashes;
    this.stateTrie = stateTrie;
  }

  public List<StateRootComputations.UpdaterWrite> writes() {
    return writes;
  }

  /**
   * Commits dirty trie nodes into {@link #writes} unless storage is frozen, and returns the new
   * root.
   */
  public Hash commit() {
    if (!storageFrozen) {
      stateTrie.commit(
          (location, hash, value) -> writes.add(u -> {
            if(value==null){
              u.removeTrieNode(location);
            } else {
              u.putTrieNode(location, hash, value);
            }
          }));
    }
    return Hash.wrap(stateTrie.getRootHash());
  }

  /**
   * Writes account header leaves ({@code BASIC_DATA}, {@code CODE_HASH} / {@code DELEGATION}) and
   * the flat-DB account encoding.
   */
  public void putAccountHeader(
      final Address address,
      final boolean accountExisted,
      final Hash priorCodeHash,
      final Bytes priorCode,
      final long nonce,
      final Wei balance,
      final Bytes updatedCode,
      final Hash updatedCodeHash,
      final Bytes flatDbAccount) {
    final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
    final Bytes basicDataKey = TrieKeyDerivation.getTreeKeyForBasicData(address32);
    final Bytes codeHashKey = TrieKeyDerivation.getTreeKeyForCodeHash(address32);
    final Bytes delegationKey = TrieKeyDerivation.getTreeKeyForDelegation(address32);

    final boolean priorDelegation = hasCodeDelegation(priorCode);
    final boolean updatedDelegation = hasCodeDelegation(updatedCode);
    final long codeSize =
        updatedDelegation ? EmbeddingParameters.DELEGATION_CODE_SIZE : updatedCode.size();
    final Bytes32 basicData =
        BasicDataEncoder.encodeBasicData(codeSize, nonce, balance.toUInt256());
    // EIP-8297: a zero-valued leaf is absent from the tree.
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
      if (accountExisted && !priorDelegation) {
        stateTrie.remove(codeHashKey);
      }
    } else {
      final boolean codeHashChanged =
          !accountExisted || priorDelegation || !updatedCodeHash.equals(priorCodeHash);
      if (codeHashChanged) {
        stateTrie.put(codeHashKey, updatedCodeHash.getBytes());
      }
      if (priorDelegation) {
        stateTrie.remove(delegationKey);
      }
    }

    if (!storageFrozen) {
      writes.add(updater -> updater.putAccountInfoState(address.addressHash(), flatDbAccount));
    }
  }

  /**
   * Removes header leaves for a deleted account. Storage and {@code CODE_ZONE} chunks are not
   * cleared here.
   */
  public void clearAccountHeader(final Address address, final Bytes priorCode) {
    final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
    stateTrie.remove(TrieKeyDerivation.getTreeKeyForBasicData(address32));
    if (hasCodeDelegation(priorCode)) {
      stateTrie.remove(TrieKeyDerivation.getTreeKeyForDelegation(address32));
    } else {
      stateTrie.remove(TrieKeyDerivation.getTreeKeyForCodeHash(address32));
    }
    if (!storageFrozen) {
      writes.add(updater -> updater.removeAccountInfoState(address.addressHash()));
    }
  }

  /**
   * Writes or removes {@code CODE_ZONE} chunks and flat-DB code. Delegation indicators are header
   * leaves ({@link #putAccountHeader}), not code chunks.
   */
  public void putCode(final Address address, final Bytes priorCode, final Bytes updatedCode) {
    if (isEmpty(priorCode) && isEmpty(updatedCode)) {
      return;
    }

    final Hash accountHash = address.addressHash();
    final Hash priorCodeHash = isEmpty(priorCode) ? Hash.EMPTY : Hash.hash(priorCode);
    final Hash updatedCodeHash = isEmpty(updatedCode) ? Hash.EMPTY : Hash.hash(updatedCode);
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

    if (removingCode && introducedCodeHashes.contains(priorCodeHash)) {
      if (!hasCodeDelegation(priorCode)) {
        removeCodeLeaves(Bytes32.wrap(priorCodeHash.getBytes()), priorCode);
      }
      if (!storageFrozen) {
        writes.add(updater -> updater.removeCode(accountHash, priorCodeHash));
      }
    }
  }

  /** Writes or removes one storage slot leaf and the corresponding flat-DB entry. */
  public void putStorageValue(
      final Address address, final StorageSlotKey slotKey, final UInt256 value) {
    final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
    final Hash accountHash = address.addressHash();
    final Hash slotHash = slotKey.getSlotHash();
    final Bytes storageKey =
        TrieKeyDerivation.getTreeKeyForStorageSlot(address32, slotKey.getSlotKey().orElseThrow());

    if (value == null || UInt256.ZERO.equals(value)) {
      stateTrie.remove(storageKey);
      if (!storageFrozen) {
        writes.add(updater -> updater.removeStorageValueBySlotHash(accountHash, slotHash));
      }
    } else {
      stateTrie.put(storageKey, Bytes32.leftPad(value));
      if (!storageFrozen) {
        writes.add(updater -> updater.putStorageValueBySlotHash(accountHash, slotHash, value));
      }
    }
  }

  private boolean recordIfNewlyIntroduced(final Hash codeHash) {
    if (introducedCodeHashes.contains(codeHash)) {
      return false;
    }
    if (worldState.getWorldStateStorage().getCode(codeHash, null).isPresent()) {
      return false;
    }
    introducedCodeHashes.add(codeHash);
    return true;
  }

  private void putCodeLeaves(final Bytes32 codeHash, final Bytes codeBytes) {
    final List<Bytes32> chunks = CodeChunkifier.chunkifyCode(codeBytes);
    for (int i = 0; i < chunks.size(); i++) {
      final Bytes32 chunk = chunks.get(i);
      final Bytes chunkKey = TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash, i);
      if (Bytes32.ZERO.equals(chunk)) {
        stateTrie.remove(chunkKey);
      } else {
        stateTrie.put(chunkKey, chunk);
      }
    }
  }

  private void removeCodeLeaves(final Bytes32 codeHash, final Bytes codeBytes) {
    final List<Bytes32> chunks = CodeChunkifier.chunkifyCode(codeBytes);
    for (int i = 0; i < chunks.size(); i++) {
      if (Bytes32.ZERO.equals(chunks.get(i))) {
        continue;
      }
      stateTrie.remove(TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash, i));
    }
  }

  private static boolean delegationLeafUnchanged(final Bytes priorCode, final Bytes updatedCode) {
    return CodeDelegationHelper.getTargetAddress(priorCode)
        .equals(CodeDelegationHelper.getTargetAddress(updatedCode));
  }

  static boolean isEmpty(final Bytes value) {
    return value == null || value.isEmpty();
  }
}
