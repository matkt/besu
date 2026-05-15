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
package org.hyperledger.besu.evm.gascalculator.stateless;

import static org.hyperledger.besu.evm.internal.Words.clampedAdd;

import org.hyperledger.besu.datatypes.AccessEvent;
import org.hyperledger.besu.datatypes.AccessWitness;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the EIP-4762 Access Witness for the Binary Trie.
 *
 * <p>Tracks all branch and leaf accesses during a transaction and charges gas accordingly:
 *
 * <ul>
 *   <li>First access to a branch (stem) → WITNESS_BRANCH_COST (1900)
 *   <li>First access to a leaf (chunk) → WITNESS_CHUNK_COST (200)
 *   <li>First write to a branch → SUBTREE_EDIT_COST (3000)
 *   <li>Write-reset on existing leaf → CHUNK_EDIT_COST (500)
 *   <li>Write-set on new leaf → CHUNK_FILL_COST (6200)
 *   <li>Subsequent accesses → 0 (already witnessed)
 * </ul>
 *
 * <p>If a gas operation runs out of gas, all accesses since the last {@code enterWitness()} are
 * reverted atomically.
 */
public class Eip4762AccessWitness implements AccessWitness {

  private static final Logger LOG = LoggerFactory.getLogger(Eip4762AccessWitness.class);

  // EIP-4762 trie parameters (inlined from besu-stateless Parameters to avoid module coupling)
  public static final UInt256 BASIC_DATA_LEAF_KEY = UInt256.valueOf(0);
  public static final UInt256 CODE_HASH_LEAF_KEY = UInt256.valueOf(1);
  private static final UInt256 CODE_OFFSET = UInt256.valueOf(128);
  private static final UInt256 VERKLE_NODE_WIDTH = UInt256.valueOf(256);
  private static final UInt256 VERKLE_NODE_WIDTH_LOG2 = UInt256.valueOf(8);
  private static final UInt256 HEADER_STORAGE_OFFSET = UInt256.valueOf(64);
  private static final UInt256 HEADER_STORAGE_SIZE =
      UInt256.valueOf(64); // CODE_OFFSET - HEADER_STORAGE_OFFSET
  private static final UInt256 MAIN_STORAGE_OFFSET_SHIFT =
      UInt256.ONE.shiftLeft(UInt256.valueOf(8 * 31).subtract(VERKLE_NODE_WIDTH_LOG2).intValue());

  private static final UInt256 ZERO_TREE_INDEX = UInt256.ZERO;

  private final Map<AccessEvent<?>, AccessEvent<?>> accesses;
  private final List<AccessEvent<?>> revertableEvents;

  /** Creates a new empty access witness. */
  public Eip4762AccessWitness() {
    this(new HashMap<>(), new ArrayList<>());
  }

  /**
   * Creates an access witness with pre-populated accesses (for copy/merge scenarios).
   *
   * @param accesses existing access events map
   * @param revertableEvents list for tracking revertable events within the current atomic operation
   */
  public Eip4762AccessWitness(
      final Map<AccessEvent<?>, AccessEvent<?>> accesses,
      final List<AccessEvent<?>> revertableEvents) {
    this.accesses = accesses;
    this.revertableEvents = revertableEvents;
  }

  // ── Public API ─────────────────────────────────────────────────────────────

  /**
   * Charge for reading an account's basic data or code hash leaf.
   *
   * @param address account address
   * @param leafKey BASIC_DATA_LEAF_KEY or CODE_HASH_LEAF_KEY
   * @param remainingGas gas remaining before the charge
   * @return gas to charge (0 if already witnessed)
   */
  @Override
  public long touchAddressAndChargeRead(
      final Address address, final UInt256 leafKey, final long remainingGas) {
    return touchAddressAtomic(
        () -> touchAddressOnReadAndComputeGas(address, ZERO_TREE_INDEX, leafKey), remainingGas);
  }

  /**
   * Charge for a CALL that transfers value: write-reset on caller, read or write-set on target.
   *
   * @param caller caller address
   * @param target call target address
   * @param isAccountCreation true if target account does not exist yet
   * @param warmReadCost fallback cost when the witness entry was already present (warm read)
   * @param remainingGas gas remaining before the charge
   * @return total gas to charge
   */
  @Override
  public long touchAndChargeValueTransfer(
      final Address caller,
      final Address target,
      final boolean isAccountCreation,
      final long warmReadCost,
      final long remainingGas) {
    long gasRemaining = remainingGas;

    long gas =
        touchAddressAtomic(
            () ->
                touchAddressOnWriteResetAndComputeGas(caller, ZERO_TREE_INDEX, BASIC_DATA_LEAF_KEY),
            gasRemaining);
    gasRemaining -= gas;

    if (isAccountCreation) {
      return clampedAdd(
          gas,
          touchAddressAtomic(
              () ->
                  clampedAdd(
                      touchAddressOnWriteSetAndComputeGas(
                          target, ZERO_TREE_INDEX, BASIC_DATA_LEAF_KEY),
                      touchAddressOnWriteSetAndComputeGas(
                          target, ZERO_TREE_INDEX, CODE_HASH_LEAF_KEY)),
              gasRemaining));
    }

    long readTargetGas =
        touchAddressAtomic(
            () -> touchAddressOnReadAndComputeGas(target, ZERO_TREE_INDEX, BASIC_DATA_LEAF_KEY),
            gasRemaining);
    if (readTargetGas == 0) {
      readTargetGas = warmReadCost;
    }
    gasRemaining -= readTargetGas;
    gas = clampedAdd(gas, readTargetGas);

    return clampedAdd(
        gas,
        touchAddressAtomic(
            () ->
                touchAddressOnWriteResetAndComputeGas(target, ZERO_TREE_INDEX, BASIC_DATA_LEAF_KEY),
            gasRemaining));
  }

  /**
   * Charge for a SELFDESTRUCT that transfers value.
   *
   * @param caller originator address
   * @param target beneficiary address
   * @param isAccountCreation true if beneficiary does not exist
   * @param warmReadCost fallback warm read cost
   * @param remainingGas gas remaining
   * @return total gas to charge
   */
  @Override
  public long touchAndChargeValueTransferSelfDestruct(
      final Address caller,
      final Address target,
      final boolean isAccountCreation,
      final long warmReadCost,
      final long remainingGas) {
    long gasRemaining = remainingGas;

    long gas =
        touchAddressAtomic(
            () ->
                touchAddressOnWriteResetAndComputeGas(caller, ZERO_TREE_INDEX, BASIC_DATA_LEAF_KEY),
            gasRemaining);
    gasRemaining -= gas;

    if (caller.equals(target)) {
      return gas;
    }

    if (isAccountCreation) {
      return clampedAdd(
          gas,
          touchAddressAtomic(
              () ->
                  clampedAdd(
                      touchAddressOnWriteSetAndComputeGas(
                          target, ZERO_TREE_INDEX, BASIC_DATA_LEAF_KEY),
                      touchAddressOnWriteSetAndComputeGas(
                          target, ZERO_TREE_INDEX, CODE_HASH_LEAF_KEY)),
              gasRemaining));
    }

    long readTargetGas =
        touchAddressAtomic(
            () -> touchAddressOnReadAndComputeGas(target, ZERO_TREE_INDEX, BASIC_DATA_LEAF_KEY),
            gasRemaining);
    if (readTargetGas == 0) {
      readTargetGas = warmReadCost;
    }
    gasRemaining -= readTargetGas;
    gas = clampedAdd(gas, readTargetGas);

    return clampedAdd(
        gas,
        touchAddressAtomic(
            () ->
                touchAddressOnWriteResetAndComputeGas(target, ZERO_TREE_INDEX, BASIC_DATA_LEAF_KEY),
            gasRemaining));
  }

  /**
   * Charge for proof-of-absence: read BASIC_DATA + CODE_HASH for an address.
   *
   * @param address the address
   * @param remainingGas gas remaining
   * @return gas to charge
   */
  @Override
  public long touchAndChargeProofOfAbsence(final Address address, final long remainingGas) {
    return touchAddressAtomic(
        () ->
            clampedAdd(
                touchAddressOnReadAndComputeGas(address, ZERO_TREE_INDEX, BASIC_DATA_LEAF_KEY),
                touchAddressOnReadAndComputeGas(address, ZERO_TREE_INDEX, CODE_HASH_LEAF_KEY)),
        remainingGas);
  }

  /**
   * Charge for completing contract creation: write-reset on BASIC_DATA + CODE_HASH.
   *
   * @param address contract address
   * @param remainingGas gas remaining
   * @return gas to charge
   */
  @Override
  public long touchAndChargeContractCreateCompleted(
      final Address address, final long remainingGas) {
    return touchAddressAtomic(
        () ->
            clampedAdd(
                touchAddressOnWriteResetAndComputeGas(
                    address, ZERO_TREE_INDEX, BASIC_DATA_LEAF_KEY),
                touchAddressOnWriteResetAndComputeGas(
                    address, ZERO_TREE_INDEX, CODE_HASH_LEAF_KEY)),
        remainingGas);
  }

  /**
   * Pre-warm the witness for base transaction accesses (no gas charge).
   *
   * @param origin transaction origin
   * @param target transaction target (empty for contract creation)
   * @param value transaction value
   */
  @Override
  public void touchBaseTx(final Address origin, final Optional<Address> target, final Wei value) {
    LOG.atDebug().log("START OF UNCHARGED COSTS");
    touchAddressOnWriteResetAndComputeGas(origin, ZERO_TREE_INDEX, BASIC_DATA_LEAF_KEY);
    touchAddressOnReadAndComputeGas(origin, ZERO_TREE_INDEX, CODE_HASH_LEAF_KEY);
    if (target.isPresent()) {
      final Address to = target.get();
      final boolean sendsValue = !Wei.ZERO.equals(value);
      touchAddressOnReadAndComputeGas(to, ZERO_TREE_INDEX, CODE_HASH_LEAF_KEY);
      if (!sendsValue) {
        touchAddressOnReadAndComputeGas(to, ZERO_TREE_INDEX, BASIC_DATA_LEAF_KEY);
      } else {
        touchAddressOnWriteResetAndComputeGas(to, ZERO_TREE_INDEX, BASIC_DATA_LEAF_KEY);
      }
    }
    LOG.atDebug().log("END OF UNCHARGED COSTS");
  }

  /**
   * Charge for writing code chunks upon contract creation (one chunk-fill per 31-byte chunk).
   *
   * @param address contract address
   * @param codeLength code size in bytes
   * @param remainingGas gas remaining
   * @return gas to charge
   */
  @Override
  public long touchCodeChunksUponContractCreation(
      final Address address, final long codeLength, final long remainingGas) {
    long gasRemaining = remainingGas;
    long gas = 0;
    for (long i = 0; i < (codeLength + 30) / 31; i++) {
      final long chunkIndex = i;
      final long statelessGas =
          touchAddressAtomic(
              () ->
                  touchAddressOnWriteSetAndComputeGas(
                      address,
                      CODE_OFFSET.add(chunkIndex).divide(VERKLE_NODE_WIDTH),
                      CODE_OFFSET.add(chunkIndex).mod(VERKLE_NODE_WIDTH)),
              gasRemaining);
      gasRemaining -= statelessGas;
      gas = clampedAdd(gas, statelessGas);
    }
    return gas;
  }

  /**
   * Charge for reading code chunks (CODECOPY, PUSH, EXTCODECOPY, etc.).
   *
   * @param contractAddress contract address
   * @param isContractInDeployment true if the contract is being deployed (no charge)
   * @param startPc start program counter
   * @param readSize number of bytes read
   * @param codeLength total code length
   * @param remainingGas gas remaining
   * @return gas to charge
   */
  @Override
  public long touchCodeChunks(
      final Address contractAddress,
      final boolean isContractInDeployment,
      final long startPc,
      final long readSize,
      final long codeLength,
      final long remainingGas) {
    if (isContractInDeployment || readSize == 0 || startPc >= codeLength) {
      return 0;
    }
    long gasRemaining = remainingGas;
    long gas = 0;
    long endPc = Math.min(startPc + readSize, codeLength) - 1L;
    for (long i = startPc / 31L; i <= endPc / 31L; i++) {
      final long chunkIndex = i;
      final long statelessGas =
          touchAddressAtomic(
              () ->
                  touchAddressOnReadAndComputeGas(
                      contractAddress,
                      CODE_OFFSET.add(chunkIndex).divide(VERKLE_NODE_WIDTH),
                      CODE_OFFSET.add(chunkIndex).mod(VERKLE_NODE_WIDTH)),
              gasRemaining);
      gasRemaining -= statelessGas;
      gas = clampedAdd(gas, statelessGas);
    }
    return gas;
  }

  /**
   * Charge for reading a storage slot (SLOAD).
   *
   * @param address account address
   * @param storageKey storage slot key
   * @param remainingGas gas remaining
   * @return gas to charge
   */
  @Override
  public long touchAndChargeStorageLoad(
      final Address address, final UInt256 storageKey, final long remainingGas) {
    final List<UInt256> indexes = storageSlotTreeIndexes(storageKey);
    return touchAddressAtomic(
        () -> touchAddressOnReadAndComputeGas(address, indexes.get(0), indexes.get(1)),
        remainingGas);
  }

  /**
   * Charge for writing a storage slot (SSTORE).
   *
   * @param address account address
   * @param storageKey storage slot key
   * @param hasPreviousValue true if slot previously held a non-zero value
   * @param remainingGas gas remaining
   * @return gas to charge
   */
  @Override
  public long touchAndChargeStorageStore(
      final Address address,
      final UInt256 storageKey,
      final boolean hasPreviousValue,
      final long remainingGas) {
    final List<UInt256> indexes = storageSlotTreeIndexes(storageKey);
    return touchAddressAtomic(
        () -> {
          if (!hasPreviousValue) {
            return touchAddressOnWriteSetAndComputeGas(address, indexes.get(0), indexes.get(1));
          }
          return touchAddressOnWriteResetAndComputeGas(address, indexes.get(0), indexes.get(1));
        },
        remainingGas);
  }

  /**
   * Get all leaf access events recorded by this witness.
   *
   * @return list of leaf access events
   */
  @Override
  public List<AccessEvent<?>> getLeafAccesses() {
    return accesses.keySet().stream().filter(a -> a instanceof LeafAccessEvent).toList();
  }

  // ── Internal implementation ────────────────────────────────────────────────

  /**
   * Executes a gas-charging operation atomically: clears the revertable list, computes the cost,
   * and if the remaining gas is insufficient, reverts all witness additions made during this call.
   */
  private long touchAddressAtomic(final LongSupplier gasSupplier, final long remainingGas) {
    enterWitness();
    final long gas = gasSupplier.getAsLong();
    if (remainingGas < gas) {
      revertWitnesses();
    }
    return gas;
  }

  private long touchAddressOnReadAndComputeGas(
      final Address address, final UInt256 treeIndex, final UInt256 subIndex) {
    return touchAddressAndChargeGas(address, treeIndex, subIndex, AccessEvent.LEAF_READ);
  }

  private long touchAddressOnWriteResetAndComputeGas(
      final Address address, final UInt256 treeIndex, final UInt256 subIndex) {
    return touchAddressAndChargeGas(address, treeIndex, subIndex, AccessEvent.LEAF_RESET);
  }

  private long touchAddressOnWriteSetAndComputeGas(
      final Address address, final UInt256 treeIndex, final UInt256 subIndex) {
    // NOTE: EIP-4762 devnet-7 uses LEAF_RESET for both set and reset pending CHUNK_FILL
    // implementation. Keep aligned with the source branch behavior.
    return touchAddressAndChargeGas(address, treeIndex, subIndex, AccessEvent.LEAF_RESET);
  }

  private long touchAddressAndChargeGas(
      final Address address,
      final UInt256 treeIndex,
      final UInt256 subIndex,
      final int accessMode) {

    final BranchAccessEvent branchAccess = new BranchAccessEvent(address, treeIndex);
    touchAddressForBranch(branchAccess, accessMode);

    AccessEvent<?> witnessAccess = branchAccess;
    if (subIndex != null) {
      final LeafAccessEvent leafAccessEvent = new LeafAccessEvent(branchAccess, subIndex);
      touchAddressForLeaf(leafAccessEvent, accessMode);
      witnessAccess = leafAccessEvent;
    }

    long gas = 0;
    if (witnessAccess.getBranchEvent().isBranchRead()) {
      gas = clampedAdd(gas, AccessEvent.getBranchReadCost());
    }
    if (witnessAccess.isLeafRead()) {
      gas = clampedAdd(gas, AccessEvent.getLeafReadCost());
    }
    if (witnessAccess.getBranchEvent().isBranchWrite()) {
      gas = clampedAdd(gas, AccessEvent.getBranchWriteCost());
    }
    if (witnessAccess.isLeafReset()) {
      gas = clampedAdd(gas, AccessEvent.getLeafResetCost());
    }
    if (witnessAccess.isLeafSet()) {
      gas = clampedAdd(gas, AccessEvent.getLeafSetCost());
    }

    final long gasView = gas;
    final AccessEvent<?> accessView = witnessAccess;
    LOG.atDebug().log(
        () ->
            "touch witness "
                + accessView
                + "\ntotal charges "
                + gasView
                + accessView.costSchedulePrettyPrint());

    revertableEvents.add(witnessAccess);
    return gas;
  }

  private void touchAddressForBranch(final BranchAccessEvent accessEvent, final int accessMode) {
    AccessEvent<?> current = accesses.putIfAbsent(accessEvent, accessEvent);
    if (current == null) {
      current = accessEvent;
      accessEvent.branchRead();
    }
    if (AccessEvent.isWrite(accessMode) && !current.isBranchWrite()) {
      accessEvent.branchWrite();
    }
    current.seenAccess();
    current.mergeFlags(accessEvent);
    accesses.put(current, current);
  }

  private void touchAddressForLeaf(final LeafAccessEvent accessEvent, final int accessMode) {
    AccessEvent<?> current = accesses.putIfAbsent(accessEvent, accessEvent);
    if (current == null) {
      current = accessEvent;
      accessEvent.leafRead();
    }
    if (AccessEvent.isWrite(accessMode)) {
      if (!current.isLeafReset()) {
        accessEvent.leafReset();
      }
      if (AccessEvent.isLeafSet(accessMode) && !current.isLeafSet()) {
        accessEvent.leafSet();
      }
    }
    current.seenAccess();
    current.mergeFlags(accessEvent);
    accesses.put(current, current);
  }

  private void revertWitnesses() {
    revertableEvents.forEach(
        key -> {
          if (accesses.containsKey(key)) {
            LOG.atDebug().log("rolling back {}", key);
            rollbackAccess(key.getBranchEvent());
            rollbackAccess(key);
          }
        });
  }

  private void rollbackAccess(final AccessEvent<?> key) {
    if (accesses.get(key).rollbackAccessAndGet() > 0) {
      return;
    }
    LOG.atDebug().log("removed {}", key);
    accesses.remove(key);
  }

  private void enterWitness() {
    revertableEvents.clear();
  }

  // ── Storage key index helpers (inlined from besu-stateless TrieKeyUtils) ──

  private static List<UInt256> storageSlotTreeIndexes(final UInt256 storageKey) {
    final Bytes32 key = Bytes32.wrap(storageKey);
    return List.of(
        UInt256.fromBytes(getStorageKeyTrieIndex(key)),
        UInt256.fromBytes(getStorageKeySuffix(key)));
  }

  private static Bytes32 getStorageKeyTrieIndex(final Bytes32 storageKey) {
    final UInt256 k = UInt256.fromBytes(storageKey);
    if (k.compareTo(HEADER_STORAGE_SIZE) < 0) {
      return k.add(HEADER_STORAGE_OFFSET).divide(VERKLE_NODE_WIDTH);
    }
    return Bytes32.wrap(
        k.shiftRight(VERKLE_NODE_WIDTH_LOG2.intValue()).add(MAIN_STORAGE_OFFSET_SHIFT));
  }

  private static Bytes32 getStorageKeySuffix(final Bytes32 storageKey) {
    final UInt256 k = UInt256.fromBytes(storageKey);
    if (k.compareTo(HEADER_STORAGE_SIZE) < 0) {
      final UInt256 mod = k.add(HEADER_STORAGE_OFFSET).mod(VERKLE_NODE_WIDTH);
      return Bytes32.leftPad(mod.slice(31, 1));
    }
    return Bytes32.leftPad(storageKey.slice(31, 1));
  }

  // ── equals / hashCode / toString ──────────────────────────────────────────

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    return Objects.equals(accesses, ((Eip4762AccessWitness) o).accesses);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accesses);
  }

  @Override
  public String toString() {
    return String.format(
        "Eip4762AccessWitness { leaves=%s, branches=%s }",
        getLeafAccesses(),
        accesses.keySet().stream().filter(a -> a instanceof BranchAccessEvent).toList());
  }
}
