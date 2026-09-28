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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.convert;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Read-only view of MPT account state used by {@link Eip8347SnapshotGenerator}.
 *
 * <p>EIP-8347 converter step: given preimages and the state committed by {@code ANCHOR_BLOCK}, look
 * up nonce/balance/code/storage to derive PBT leaves. Production convert uses {@link
 * Eip8347WorldStateSource}; tests may stub this interface directly.
 */
@FunctionalInterface
public interface Eip8347StateSource {

  /**
   * Account fields needed to emit EIP-8297 leaves for one address.
   *
   * @param nonce account nonce
   * @param balance account balance
   * @param code account code (empty for EOAs; may be an EIP-7702 delegation indicator)
   */
  record AccountView(long nonce, Wei balance, Bytes code) {}

  /** Returns the account at {@code address}, or empty if absent from the anchor state. */
  Optional<AccountView> getAccount(Address address);

  /** Storage value at {@code slotKey} (32-byte big-endian slot), or zero if unset. */
  default UInt256 getStorage(final Address address, final Bytes32 slotKey) {
    return UInt256.ZERO;
  }

  /** True when {@code code} is an EIP-7702 delegation indicator ({@code 0xef0100 ‖ address}). */
  static boolean isDelegationCode(final Bytes code) {
    return CodeDelegationHelper.hasCodeDelegation(code);
  }

  /** Code hash for a non-delegation account (empty code → {@link Hash#EMPTY}). */
  static Hash codeHashOf(final Bytes code) {
    return code == null || code.isEmpty() ? Hash.EMPTY : Hash.hash(code);
  }
}
