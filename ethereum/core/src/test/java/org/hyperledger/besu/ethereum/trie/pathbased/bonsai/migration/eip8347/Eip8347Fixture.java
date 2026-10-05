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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.BasicDataEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.CodeChunkifier;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.DelegationEncoder;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.trie.AscendingCollapseBinaryTrie;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347PreimageFile.AccountPreimages;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotWriter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.Leaf;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.convert.Eip8347SnapshotGenerator.StateSource;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * A small anchor state and everything EIP-8347 derives from it, computed independently of the code
 * under test: the PBT leaves and root (straight from the PBT key derivation), the preimages, and
 * the MPT root (from {@link SimpleMerklePatriciaTrie}).
 *
 * @param accounts the anchor state, by address
 * @param leaves PBT leaves in snapshot order
 * @param preimages one record per account
 * @param pbtRoot PBT root of {@code leaves}
 * @param mptRoot MPT state root of {@code accounts}
 */
public record Eip8347Fixture(
    Map<Address, Account> accounts,
    List<Leaf> leaves,
    List<AccountPreimages> preimages,
    Bytes32 pbtRoot,
    Bytes32 mptRoot) {

  /** One account; {@code code} may be an EIP-7702 delegation indicator. */
  public record Account(long nonce, Wei balance, Bytes code, Map<UInt256, UInt256> storage) {}

  /** Paths of a written snapshot / preimage pair. */
  public record Artifacts(Path snapshot, Path preimages) {}

  public static Builder builder() {
    return new Builder();
  }

  /** Writes the snapshot (claiming {@link #pbtRoot}) and the preimages under {@code dir}. */
  public Artifacts write(final Path dir, final String name) throws IOException {
    return write(dir, name, pbtRoot, leaves);
  }

  /**
   * Writes a snapshot of {@code snapshotLeaves} claiming {@code root}, with this fixture's
   * preimages.
   */
  public Artifacts write(
      final Path dir, final String name, final Bytes32 root, final List<Leaf> snapshotLeaves)
      throws IOException {
    final Artifacts artifacts =
        new Artifacts(dir.resolve(name + ".snap"), dir.resolve(name + ".pre"));
    Eip8347SnapshotWriter.write(artifacts.snapshot(), root, snapshotLeaves);
    Eip8347PreimageFile.write(artifacts.preimages(), preimages);
    return artifacts;
  }

  /** The anchor state as the converter reads it. */
  public StateSource stateSource() {
    return new StateSource() {
      @Override
      public Optional<AccountView> getAccount(final Address address) {
        return Optional.ofNullable(accounts.get(address))
            .map(a -> new AccountView(a.nonce(), a.balance(), Hash.hash(a.code()), a.code()));
      }

      @Override
      public UInt256 getStorage(final Address address, final Bytes32 slotKey) {
        final Account account = accounts.get(address);
        return account == null
            ? UInt256.ZERO
            : account.storage().getOrDefault(UInt256.fromBytes(slotKey), UInt256.ZERO);
      }
    };
  }

  /** {@code leaves} with the value at {@code key} replaced. */
  public List<Leaf> withLeaf(final Bytes key, final Bytes32 value) {
    return leaves.stream()
        .map(leaf -> leaf.key().equals(key) ? new Leaf(key, value) : leaf)
        .toList();
  }

  /** PBT root of arbitrary leaves (e.g. tampered ones). */
  public static Bytes32 pbtRootOf(final List<Leaf> leaves) {
    final AscendingCollapseBinaryTrie pbt = new AscendingCollapseBinaryTrie();
    leaves.stream()
        .sorted(Comparator.comparing(Leaf::key, Eip8347TypedSnapshotCodec::compare))
        .forEach(leaf -> pbt.insert(leaf.key(), leaf.value()));
    return pbt.rootHash();
  }

  /** Writes preimage records in the given order, unsorted (for negative tests). */
  public static void writePreimagesInGivenOrder(
      final Path path, final List<AccountPreimages> records) throws IOException {
    try (final OutputStream out = Files.newOutputStream(path)) {
      for (final AccountPreimages record : records) {
        out.write(record.address().getBytes().toArrayUnsafe());
        out.write(ByteBuffer.allocate(4).putInt(record.slotKeys().size()).array());
        for (final Bytes32 slot : record.slotKeys()) {
          out.write(slot.toArrayUnsafe());
        }
      }
    }
  }

  /** Builds a fixture account by account; zero storage values are left out, as in the MPT. */
  public static final class Builder {
    private final Map<Address, Account> accounts = new LinkedHashMap<>();

    public Builder eoa(
        final Address address,
        final long nonce,
        final Wei balance,
        final Map<UInt256, UInt256> storage) {
      return contract(address, nonce, balance, Bytes.EMPTY, storage);
    }

    public Builder contract(
        final Address address,
        final long nonce,
        final Wei balance,
        final Bytes code,
        final Map<UInt256, UInt256> storage) {
      accounts.put(address, new Account(nonce, balance, code, storage));
      return this;
    }

    public Eip8347Fixture build() {
      final Map<Bytes, Bytes32> leafMap = new TreeMap<>(Eip8347TypedSnapshotCodec::compare);
      final List<AccountPreimages> preimages = new ArrayList<>();
      final Map<Bytes32, Bytes> codeByHash = new HashMap<>();
      accounts.forEach(
          (address, account) -> {
            final Bytes32 address32 = TrieKeyDerivation.address20ToAddress32(address.getBytes());
            final List<Bytes32> slotKeys = new ArrayList<>();
            account
                .storage()
                .forEach(
                    (slot, value) -> {
                      if (!value.isZero()) {
                        slotKeys.add(Bytes32.leftPad(slot));
                        leafMap.put(
                            TrieKeyDerivation.getTreeKeyForStorageSlot(address32, slot),
                            Bytes32.leftPad(value));
                      }
                    });
            preimages.add(new AccountPreimages(address, slotKeys));

            final Bytes code = account.code();
            final UInt256 balance = account.balance().toUInt256();
            if (CodeDelegationHelper.hasCodeDelegation(code)) {
              leafMap.put(
                  TrieKeyDerivation.getTreeKeyForBasicData(address32),
                  BasicDataEncoder.encodeBasicData(
                      EmbeddingParameters.DELEGATION_CODE_SIZE, account.nonce(), balance));
              leafMap.put(
                  TrieKeyDerivation.getTreeKeyForDelegation(address32),
                  DelegationEncoder.encodeDelegation(
                      CodeDelegationHelper.getTargetAddress(code).getBytes()));
            } else {
              final Bytes32 codeHash = Bytes32.wrap(Hash.hash(code).getBytes());
              leafMap.put(
                  TrieKeyDerivation.getTreeKeyForBasicData(address32),
                  BasicDataEncoder.encodeBasicData(code.size(), account.nonce(), balance));
              leafMap.put(TrieKeyDerivation.getTreeKeyForCodeHash(address32), codeHash);
              if (!code.isEmpty()) {
                codeByHash.put(codeHash, code);
              }
            }
          });
      codeByHash.forEach(
          (codeHash, code) -> {
            final List<Bytes32> chunks = CodeChunkifier.chunkifyCode(code);
            for (int i = 0; i < chunks.size(); i++) {
              if (!chunks.get(i).isZero()) {
                leafMap.put(TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash, i), chunks.get(i));
              }
            }
          });
      final List<Leaf> leaves = new ArrayList<>();
      leafMap.forEach((key, value) -> leaves.add(new Leaf(key, value)));
      return new Eip8347Fixture(
          Map.copyOf(accounts), leaves, preimages, pbtRootOf(leaves), mptRoot(accounts));
    }

    private static Bytes32 mptRoot(final Map<Address, Account> accounts) {
      final SimpleMerklePatriciaTrie<Bytes, Bytes> accountTrie =
          new SimpleMerklePatriciaTrie<>(Function.identity());
      accounts.forEach(
          (address, account) -> {
            final SimpleMerklePatriciaTrie<Bytes, Bytes> storageTrie =
                new SimpleMerklePatriciaTrie<>(Function.identity());
            account
                .storage()
                .forEach(
                    (slot, value) -> {
                      if (!value.isZero()) {
                        storageTrie.put(
                            Hash.hash(Bytes32.leftPad(slot)).getBytes(),
                            RLP.encode(out -> out.writeBytes(value.trimLeadingZeros())));
                      }
                    });
            final PatriciaTrieAccountValue value =
                new PatriciaTrieAccountValue(
                    account.nonce(),
                    account.balance(),
                    Hash.wrap(storageTrie.getRootHash()),
                    Hash.hash(account.code()));
            accountTrie.put(address.addressHash().getBytes(), RLP.encode(value::writeTo));
          });
      return accountTrie.getRootHash();
    }
  }
}
