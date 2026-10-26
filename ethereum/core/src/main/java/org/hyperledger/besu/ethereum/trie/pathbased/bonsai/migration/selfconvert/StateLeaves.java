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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.selfconvert;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.TrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.Leaf;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.convert.Eip8347Leaves;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter.Entry;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.selfconvert.CodeLeaves.CodeRef;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * Turns flat database entries into PBT leaves. A contract waits for its code, and a key without
 * preimage for the BALs.
 */
final class StateLeaves {

  /** The leaves, contracts and keys without preimage of part of the state. */
  record Converted(List<Leaf> leaves, List<Entry> codeRefs, List<Entry> misses) {}

  /** An account ({@code slotKey} null) or a slot, with its preimage. */
  record Known(Address address, Bytes32 slotKey, byte[] value) {}

  private final List<Leaf> leaves = new ArrayList<>();
  private final List<Entry> codeRefs = new ArrayList<>();
  private final List<Entry> misses = new ArrayList<>();
  private Address address;
  private Bytes32 address32;
  private Bytes32 addressKeyHash;

  void add(final Known known) {
    if (known.slotKey() == null) {
      account(known.address(), known.value());
    } else {
      slot(known.address(), known.slotKey(), known.value());
    }
  }

  void account(final Address address, final byte[] value) {
    // trie logs past the fork give binary trie accounts
    final TrieAccountValue account = TrieAccountValue.readFrom(RLP.input(Bytes.wrap(value)));
    if (Hash.EMPTY.equals(account.getCodeHash())) {
      setAddress(address);
      leaves.addAll(
          Eip8347Leaves.header(
              address32, account.getNonce(), account.getBalance(), Hash.EMPTY, Bytes.EMPTY));
    } else {
      codeRefs.add(
          CodeRef.of(account.getCodeHash(), address, account.getNonce(), account.getBalance())
              .encode());
    }
  }

  void slot(final Address address, final Bytes32 slotKey, final byte[] value) {
    setAddress(address);
    leaves.add(
        Eip8347Leaves.storage(
            address32,
            addressKeyHash,
            UInt256.fromBytes(slotKey),
            UInt256.fromBytes(Bytes.wrap(value))));
  }

  /** A key without preimage in the file, with its value. */
  void miss(final Entry entry) {
    misses.add(entry);
  }

  Converted converted() {
    return new Converted(leaves, codeRefs, misses);
  }

  /** Derives the 32-byte form and key hash of {@code address}, kept for the slots that follow. */
  private void setAddress(final Address address) {
    if (!address.equals(this.address)) {
      this.address = address;
      address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
      addressKeyHash = TrieKeyDerivation.keyHash(address32);
    }
  }
}
