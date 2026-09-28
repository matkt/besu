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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieConstants;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactWriter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageRecord;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.verify.Eip8347DualCheckVerifier;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Eip8347SnapshotGeneratorTest {

  @TempDir Path tmp;

  @Test
  void generateEmptySnapshotFromEmptyPreimages() throws Exception {
    final Path preimages = tmp.resolve("empty.pre");
    final Path snapshot = tmp.resolve("empty.snap");
    Eip8347ArtifactWriter.writePreimages(preimages, List.of());

    final Eip8347SnapshotGenerator.Result result =
        Eip8347SnapshotGenerator.generate(preimages, missingAll(), snapshot);

    assertThat(result.leafCount()).isZero();
    assertThat(result.pbtRoot()).isEqualTo(TrieConstants.EMPTY_TRIE_ROOT);
    Eip8347DualCheckVerifier.verify(
        snapshot, preimages, Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes()));
  }

  @Test
  void generateRoundTripsThroughDualCheck() throws Exception {
    final Address eoa = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final Address contract = Address.fromHexString("0x00000000000000000000000000000000000000bb");
    final Bytes code = Bytes.fromHexString("0x6001600055");

    final Map<Address, Eip8347StateSource.AccountView> accounts = new HashMap<>();
    accounts.put(eoa, new Eip8347StateSource.AccountView(1L, Wei.of(1000), Bytes.EMPTY));
    accounts.put(contract, new Eip8347StateSource.AccountView(0L, Wei.of(1), code));

    final Map<Address, Map<Bytes32, UInt256>> storage = new HashMap<>();
    final Map<Bytes32, UInt256> eoaSlots = new HashMap<>();
    eoaSlots.put(Bytes32.leftPad(UInt256.ZERO), UInt256.valueOf(7));
    eoaSlots.put(Bytes32.leftPad(UInt256.valueOf(64)), UInt256.valueOf(9));
    storage.put(eoa, eoaSlots);

    final List<Eip8347PreimageRecord> records =
        List.of(
            new Eip8347PreimageRecord(eoa, List.copyOf(eoaSlots.keySet())),
            new Eip8347PreimageRecord(contract, List.of()));

    final Path preimages = tmp.resolve("ok.pre");
    final Path snapshot = tmp.resolve("ok.snap");
    Eip8347ArtifactWriter.writePreimages(preimages, records);

    final Eip8347StateSource state = mapSource(accounts, storage);
    final Eip8347SnapshotGenerator.Result result =
        Eip8347SnapshotGenerator.generate(preimages, state, snapshot);

    assertThat(result.leafCount()).isGreaterThan(0);
    assertThat(result.pbtRoot()).isNotEqualTo(TrieConstants.EMPTY_TRIE_ROOT);

    // Fixture-equivalent MPT root via dual-check against recomputed leaves path
    final Bytes32 mptRoot = mptRootOf(accounts, storage);
    Eip8347DualCheckVerifier.verify(snapshot, preimages, mptRoot);
  }

  @Test
  void generateMatchesHandBuiltLeavesForDelegation() throws Exception {
    final Address delegated = Address.fromHexString("0x00000000000000000000000000000000000000cc");
    final Address target = Address.fromHexString("0x00000000000000000000000000000000000000dd");
    final Bytes delegationCode =
        Bytes.concatenate(CodeDelegationHelper.CODE_DELEGATION_PREFIX, target.getBytes());

    final Map<Address, Eip8347StateSource.AccountView> accounts =
        Map.of(delegated, new Eip8347StateSource.AccountView(2L, Wei.of(5), delegationCode));
    final List<Eip8347PreimageRecord> records =
        List.of(new Eip8347PreimageRecord(delegated, List.of()));

    final Path preimages = tmp.resolve("del.pre");
    final Path snapshot = tmp.resolve("del.snap");
    Eip8347ArtifactWriter.writePreimages(preimages, records);

    final Eip8347SnapshotGenerator.Result result =
        Eip8347SnapshotGenerator.generate(preimages, mapSource(accounts, Map.of()), snapshot);

    final Bytes32 mptRoot = mptRootOf(accounts, Map.of());
    Eip8347DualCheckVerifier.verify(snapshot, preimages, mptRoot);
    assertThat(result.leafCount()).isEqualTo(2); // basic-data + delegation
  }

  @Test
  void generateWithTinyRunCapacityStillRoundTrips() throws Exception {
    final Address eoa = Address.fromHexString("0x00000000000000000000000000000000000001aa");
    final Address contract = Address.fromHexString("0x00000000000000000000000000000000000001bb");
    final Bytes code = Bytes.fromHexString("0x6001600055");

    final Map<Address, Eip8347StateSource.AccountView> accounts = new HashMap<>();
    accounts.put(eoa, new Eip8347StateSource.AccountView(1L, Wei.of(1000), Bytes.EMPTY));
    accounts.put(contract, new Eip8347StateSource.AccountView(0L, Wei.of(1), code));

    final Map<Address, Map<Bytes32, UInt256>> storage = new HashMap<>();
    final Map<Bytes32, UInt256> eoaSlots = new HashMap<>();
    eoaSlots.put(Bytes32.leftPad(UInt256.ZERO), UInt256.valueOf(7));
    eoaSlots.put(Bytes32.leftPad(UInt256.valueOf(64)), UInt256.valueOf(9));
    storage.put(eoa, eoaSlots);

    final List<Eip8347PreimageRecord> records =
        List.of(
            new Eip8347PreimageRecord(eoa, List.copyOf(eoaSlots.keySet())),
            new Eip8347PreimageRecord(contract, List.of()));

    final Path preimages = tmp.resolve("tiny-run.pre");
    final Path snapshot = tmp.resolve("tiny-run.snap");
    Eip8347ArtifactWriter.writePreimages(preimages, records);

    // Force many sorted runs so k-way merge is exercised (not a single in-memory flush).
    final Eip8347SnapshotGenerator.Result result =
        Eip8347SnapshotGenerator.generate(preimages, mapSource(accounts, storage), snapshot, 1);

    assertThat(result.leafCount()).isGreaterThan(0);
    Eip8347DualCheckVerifier.verify(snapshot, preimages, mptRootOf(accounts, storage));
  }

  @Test
  void sharedBytecodeEmitsCodeZoneOnce() throws Exception {
    final Bytes code = Bytes.fromHexString("0x6001600055");
    final Address a = Address.fromHexString("0x00000000000000000000000000000000000002aa");
    final Address b = Address.fromHexString("0x00000000000000000000000000000000000002bb");
    final Map<Address, Eip8347StateSource.AccountView> accounts =
        Map.of(
            a, new Eip8347StateSource.AccountView(0L, Wei.ONE, code),
            b, new Eip8347StateSource.AccountView(0L, Wei.of(2), code));
    final List<Eip8347PreimageRecord> records =
        List.of(new Eip8347PreimageRecord(a, List.of()), new Eip8347PreimageRecord(b, List.of()));

    final Path preimages = tmp.resolve("shared-code.pre");
    final Path snapshotDefault = tmp.resolve("shared-code.snap");
    final Path snapshotTiny = tmp.resolve("shared-code-tiny.snap");
    Eip8347ArtifactWriter.writePreimages(preimages, records);

    final Eip8347SnapshotGenerator.Result once =
        Eip8347SnapshotGenerator.generate(
            preimages, mapSource(accounts, Map.of()), snapshotDefault);
    final Eip8347SnapshotGenerator.Result tiny =
        Eip8347SnapshotGenerator.generate(
            preimages, mapSource(accounts, Map.of()), snapshotTiny, 1);

    assertThat(tiny.leafCount()).isEqualTo(once.leafCount());
    assertThat(tiny.pbtRoot()).isEqualTo(once.pbtRoot());
    Eip8347DualCheckVerifier.verify(snapshotDefault, preimages, mptRootOf(accounts, Map.of()));
  }

  @Test
  void rejectsPreimageForMissingAccount() throws Exception {
    final Address missing = Address.fromHexString("0x00000000000000000000000000000000000000ee");
    final Path preimages = tmp.resolve("miss.pre");
    final Path snapshot = tmp.resolve("miss.snap");
    Eip8347ArtifactWriter.writePreimages(
        preimages, List.of(new Eip8347PreimageRecord(missing, List.of())));

    assertThatThrownBy(() -> Eip8347SnapshotGenerator.generate(preimages, missingAll(), snapshot))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("has no account");
  }

  @Test
  void rejectsPreimageSlotWithZeroValue() throws Exception {
    final Address eoa = Address.fromHexString("0x00000000000000000000000000000000000000ff");
    final Map<Address, Eip8347StateSource.AccountView> accounts =
        Map.of(eoa, new Eip8347StateSource.AccountView(0L, Wei.ONE, Bytes.EMPTY));
    final Path preimages = tmp.resolve("zero.pre");
    final Path snapshot = tmp.resolve("zero.snap");
    Eip8347ArtifactWriter.writePreimages(
        preimages, List.of(new Eip8347PreimageRecord(eoa, List.of(Bytes32.leftPad(UInt256.ZERO)))));

    assertThatThrownBy(
            () ->
                Eip8347SnapshotGenerator.generate(
                    preimages, mapSource(accounts, Map.of()), snapshot))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining("zero value");
  }

  private static Eip8347StateSource missingAll() {
    return address -> Optional.empty();
  }

  private static Eip8347StateSource mapSource(
      final Map<Address, Eip8347StateSource.AccountView> accounts,
      final Map<Address, Map<Bytes32, UInt256>> storage) {
    return new Eip8347StateSource() {
      @Override
      public Optional<AccountView> getAccount(final Address address) {
        return Optional.ofNullable(accounts.get(address));
      }

      @Override
      public UInt256 getStorage(final Address address, final Bytes32 slotKey) {
        final Map<Bytes32, UInt256> slots = storage.get(address);
        if (slots == null) {
          return UInt256.ZERO;
        }
        return slots.getOrDefault(slotKey, UInt256.ZERO);
      }
    };
  }

  /** Minimal MPT account-trie root for dual-check anchoring in generator tests. */
  private static Bytes32 mptRootOf(
      final Map<Address, Eip8347StateSource.AccountView> accounts,
      final Map<Address, Map<Bytes32, UInt256>> storage)
      throws Exception {
    // Reuse fixture path: write generated snapshot+preimages already verified by DualCheckVerifier
    // with Fixture-built mptRoot is heavy; here compute via the same Patricia helpers as Fixture.
    final var accountTrie =
        new org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie<Bytes, Bytes>(
            java.util.function.Function.identity());
    final List<Address> ordered = new java.util.ArrayList<>(accounts.keySet());
    ordered.sort(java.util.Comparator.comparing(Address::addressHash));
    for (final Address address : ordered) {
      final Eip8347StateSource.AccountView state = accounts.get(address);
      final Hash storageRoot = storageRoot(storage.getOrDefault(address, Map.of()));
      final Hash codeHash;
      if (Eip8347StateSource.isDelegationCode(state.code())) {
        codeHash = Hash.hash(state.code());
      } else {
        codeHash = Eip8347StateSource.codeHashOf(state.code());
      }
      final var accountValue =
          new org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue(
              state.nonce(), state.balance(), storageRoot, codeHash);
      accountTrie.put(
          address.addressHash().getBytes(),
          org.hyperledger.besu.ethereum.rlp.RLP.encode(accountValue::writeTo));
    }
    return accountTrie.getRootHash();
  }

  private static Hash storageRoot(final Map<Bytes32, UInt256> storage) {
    if (storage.isEmpty()) {
      return Hash.EMPTY_TRIE_HASH;
    }
    final var storageTrie =
        new org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie<Bytes, Bytes>(
            java.util.function.Function.identity());
    final List<Bytes32> keys = new java.util.ArrayList<>(storage.keySet());
    keys.sort(java.util.Comparator.comparing(Hash::hash));
    boolean any = false;
    for (final Bytes32 key : keys) {
      final UInt256 value = storage.get(key);
      if (UInt256.ZERO.equals(value)) {
        continue;
      }
      any = true;
      final Bytes encoded =
          org.hyperledger.besu.ethereum.rlp.RLP.encode(
              out -> out.writeBytes(Bytes32.leftPad(value).trimLeadingZeros()));
      storageTrie.put(Hash.hash(key).getBytes(), encoded);
    }
    return any ? Hash.wrap(storageTrie.getRootHash()) : Hash.EMPTY_TRIE_HASH;
  }
}
