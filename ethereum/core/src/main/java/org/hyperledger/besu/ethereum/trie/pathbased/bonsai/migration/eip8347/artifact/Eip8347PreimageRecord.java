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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes32;

/**
 * One preimage record: {@code address[20] | slotCount[4, BE] | slotKey[32] * slotCount}.
 *
 * <p>Records are ordered by {@code keccak256(address)}; slots within a record by {@code
 * keccak256(slotKey)}. Address and slot-key hashes are computed once in the constructor so verify /
 * merge paths do not rehash on every access.
 */
public final class Eip8347PreimageRecord {

  private final Address address;
  private final Hash addressHash;
  private final List<Bytes32> slotKeys;
  private final List<Hash> slotKeyHashes;

  public Eip8347PreimageRecord(final Address address, final List<Bytes32> slotKeys) {
    if (address == null) {
      throw new Eip8347ArtifactVerificationException("preimage address must be present");
    }
    if (slotKeys == null) {
      throw new Eip8347ArtifactVerificationException("preimage slotKeys must be present");
    }
    this.address = address;
    this.addressHash = address.addressHash();
    this.slotKeys = List.copyOf(slotKeys);
    final List<Hash> hashes = new ArrayList<>(this.slotKeys.size());
    for (final Bytes32 slotKey : this.slotKeys) {
      hashes.add(Hash.hash(slotKey));
    }
    this.slotKeyHashes = List.copyOf(hashes);
  }

  public Address address() {
    return address;
  }

  /** Cached {@code keccak256(address)}. */
  public Hash addressHash() {
    return addressHash;
  }

  public List<Bytes32> slotKeys() {
    return slotKeys;
  }

  /** Cached {@code keccak256(slotKey)} values in the same order as {@link #slotKeys()}. */
  public List<Hash> slotKeyHashes() {
    return slotKeyHashes;
  }
}
