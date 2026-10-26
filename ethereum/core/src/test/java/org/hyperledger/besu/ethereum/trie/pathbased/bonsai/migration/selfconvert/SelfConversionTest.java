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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.SlotChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.StorageChange;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.RocksDbWorldStates;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.Eip8347Fixture;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile.AccountPreimages;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.verify.Eip8347DualCheckVerifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ForkJoinPool;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The self-conversion, on RocksDB, against the snapshot load of the state it converts. */
class SelfConversionTest {

  private static final Bytes CODE = Bytes.fromHexString("0x" + "60016001".repeat(40));
  private static final Address CONTRACT = address(0x100);
  private static final Address SHARING_CONTRACT = address(0x101);

  @TempDir Path tmp;

  private final ForkJoinPool pool = new ForkJoinPool(2);
  private final TestChain blocks = new TestChain(3);
  private final Eip8347Fixture fixture = state().build();
  private RocksDbWorldStates databases;
  private BonsaiWorldStateKeyValueStorage storage;

  @BeforeEach
  void openDatabase() {
    databases = new RocksDbWorldStates(tmp);
    storage = RocksDbWorldStates.worldStateStorage(databases.database("flat"));
  }

  @AfterEach
  void close() throws Exception {
    pool.shutdownNow();
    databases.close();
  }

  @Test
  void writesTheNodesAndReferenceCountsOfTheSnapshotOfItsState() throws Exception {
    fixture.writeFlatDatabase(storage, blocks.get(1).getBlockHash());
    final Map<Bytes, Bytes> nodes = new TreeMap<>();

    final SelfConversion.Result result = convert(fixture.preimages(), 1, 1, nodes);

    assertThat(result)
        .isEqualTo(new SelfConversion.Result(fixture.pbtRoot(), fixture.leaves().size(), 1));
    assertThat(nodes).isEqualTo(snapshotNodes());
  }

  @Test
  void keysCreatedAfterThePreimageFileTakeTheirPreimagesFromTheBals() throws Exception {
    fixture.writeFlatDatabase(storage, blocks.get(3).getBlockHash());
    // After block 1: an account and a contract are created, a slot is set, an account deleted.
    final List<AccountPreimages> preimages = new ArrayList<>();
    for (final AccountPreimages account : fixture.preimages()) {
      if (account.address().equals(CONTRACT)) {
        final Bytes32 created = Bytes32.leftPad(slot(0));
        preimages.add(
            new AccountPreimages(
                CONTRACT,
                account.slotKeys().stream().filter(key -> !key.equals(created)).toList()));
      } else if (!List.of(address(5), SHARING_CONTRACT).contains(account.address())) {
        preimages.add(account);
      }
    }
    preimages.add(new AccountPreimages(address(0x400), List.of(Bytes32.leftPad(UInt256.ONE))));
    blocks.bal(2, balance(address(5)), slots(CONTRACT, slot(0)));
    blocks.bal(3, slots(SHARING_CONTRACT, UInt256.ONE));

    assertThat(convert(preimages, 1, 3)).isEqualTo(snapshotNodes());
  }

  @Test
  void aKeyNeitherThePreimageFileNorTheBalsHaveIsRejected() throws Exception {
    fixture.writeFlatDatabase(storage, blocks.get(3).getBlockHash());
    final List<AccountPreimages> preimages =
        fixture.preimages().stream()
            .filter(account -> !account.address().equals(address(5)))
            .toList();

    assertThatThrownBy(() -> convert(preimages, 1, 3))
        .isInstanceOf(Eip8347ArtifactVerificationException.class)
        .hasMessageContaining(
            "no preimage for flat key " + Bytes.wrap(FlatState.account(address(5))).toHexString());
  }

  @Test
  void entriesChangedAfterTheTargetArePutBackFromTheTrieLogs() throws Exception {
    // The flat database is at block 3, the target block 1.
    fixture.writeFlatDatabase(storage, blocks.get(3).getBlockHash());
    change(2, address(1), eoa(1, 1000), eoa(2, 10));
    change(3, address(1), eoa(2, 10), eoa(3, 20));
    change(2, address(2), eoa(2, 2000), null);
    change(3, address(0x200), null, eoa(0, 1));
    change(3, address(0x200), UInt256.ONE, null, UInt256.ONE);
    change(3, CONTRACT, slot(0), value(0), UInt256.valueOf(99));
    change(2, CONTRACT, slot(1), value(1), null);
    change(2, CONTRACT, UInt256.valueOf(7777), null, UInt256.ONE);

    assertThat(convert(fixture.preimages(), 1, 1)).isEqualTo(snapshotNodes());
  }

  @Test
  void slotsOfNoAccountAreLeftOut() throws Exception {
    fixture.writeFlatDatabase(storage, blocks.get(1).getBlockHash());
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    updater.putStorageValueBySlotHash(address(0x300).addressHash(), slotHash(slot(0)), value(0));
    updater.commit();

    assertThat(convert(fixture.preimages(), 1, 1)).isEqualTo(snapshotNodes());
  }

  /** EOAs, two contracts sharing a code, a delegation, and storage. */
  private static Eip8347Fixture.Builder state() {
    final Eip8347Fixture.Builder builder = Eip8347Fixture.builder();
    for (int i = 1; i <= 20; i++) {
      builder.eoa(address(i), i, Wei.of(1000L * i), Map.of());
    }
    final Map<UInt256, UInt256> storage = new HashMap<>();
    for (int i = 0; i < 30; i++) {
      storage.put(slot(i), value(i));
    }
    return builder
        .contract(CONTRACT, 1L, Wei.ONE, CODE, storage)
        .contract(SHARING_CONTRACT, 2L, Wei.ONE, CODE, Map.of(UInt256.ONE, UInt256.ONE))
        .contract(
            address(0x102),
            3L,
            Wei.ONE,
            Bytes.concatenate(Bytes.fromHexString("0xef0100"), address(7).getBytes()),
            Map.of());
  }

  /** The nodes a conversion of {@code target} writes, the preimages anchored at {@code anchor}. */
  private Map<Bytes, Bytes> convert(
      final List<AccountPreimages> preimages, final int anchor, final int target)
      throws IOException {
    final Map<Bytes, Bytes> nodes = new TreeMap<>();
    convert(preimages, anchor, target, nodes);
    return nodes;
  }

  private SelfConversion.Result convert(
      final List<AccountPreimages> preimages,
      final int anchor,
      final int target,
      final Map<Bytes, Bytes> nodes)
      throws IOException {
    final Path file = tmp.resolve("preimages");
    Eip8347PreimageFile.write(file, preimages);
    // Two entries per snapshot, and sorts that spill every few records.
    return new SelfConversion(storage, blocks, tmp.resolve("work"), 1 << 10, pool, 2)
        .convert(
            file,
            blocks.get(anchor),
            blocks.get(target),
            (location, hash, value) -> nodes.put(location, value));
  }

  /** The nodes and code reference counts the snapshot load of the fixture writes. */
  private Map<Bytes, Bytes> snapshotNodes() throws IOException {
    final Eip8347Fixture.Artifacts artifacts = fixture.write(tmp, "snapshot");
    final Map<Bytes, Bytes> nodes = new TreeMap<>();
    Eip8347DualCheckVerifier.verifyAndLoad(
        artifacts.snapshot(),
        artifacts.preimages(),
        fixture.mptRoot(),
        tmp.resolve("verify"),
        (location, hash, value) -> nodes.put(location, value));
    return nodes;
  }

  /** An account change of {@code block}, in its trie log and in the flat database. */
  private void change(
      final int block,
      final Address address,
      final AccountValue prior,
      final AccountValue updated) {
    blocks.trieLogOf(block).addAccountChange(address, prior, updated);
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    if (updated == null) {
      updater.removeAccountInfoState(address.addressHash());
    } else {
      updater.putAccountInfoState(address.addressHash(), RLP.encode(updated::writeTo));
    }
    updater.commit();
  }

  /** A slot change of {@code block}, in its trie log and in the flat database. */
  private void change(
      final int block,
      final Address address,
      final UInt256 slot,
      final UInt256 prior,
      final UInt256 updated) {
    blocks.trieLogOf(block).addStorageChange(address, new StorageSlotKey(slot), prior, updated);
    final BonsaiWorldStateKeyValueStorage.Updater updater = storage.updater();
    if (updated == null) {
      updater.removeStorageValueBySlotHash(address.addressHash(), slotHash(slot));
    } else {
      updater.putStorageValueBySlotHash(address.addressHash(), slotHash(slot), updated);
    }
    updater.commit();
  }

  private static AccountValue eoa(final long nonce, final long balance) {
    return new PatriciaTrieAccountValue(nonce, Wei.of(balance), Hash.EMPTY_TRIE_HASH, Hash.EMPTY);
  }

  /** A BAL entry of an account whose balance changed. */
  private static AccountChanges balance(final Address address) {
    return new AccountChanges(
        address,
        List.of(),
        List.of(),
        List.of(new BalanceChange(0, Wei.ONE)),
        List.of(),
        List.of());
  }

  /** A BAL entry of an account whose {@code slots} were set. */
  private static AccountChanges slots(final Address address, final UInt256... slots) {
    return new AccountChanges(
        address,
        Arrays.stream(slots)
            .map(
                slot ->
                    new SlotChanges(
                        new StorageSlotKey(slot), List.of(new StorageChange(0, UInt256.ONE))))
            .toList(),
        List.of(),
        List.of(),
        List.of(),
        List.of());
  }

  private static UInt256 slot(final int index) {
    return UInt256.valueOf(index * 13L);
  }

  private static UInt256 value(final int index) {
    return UInt256.valueOf(index + 1L);
  }

  private static Hash slotHash(final UInt256 slot) {
    return Hash.hash(Bytes32.leftPad(slot));
  }

  private static Address address(final int index) {
    return Address.fromHexString(String.format("0x%040x", index));
  }
}
