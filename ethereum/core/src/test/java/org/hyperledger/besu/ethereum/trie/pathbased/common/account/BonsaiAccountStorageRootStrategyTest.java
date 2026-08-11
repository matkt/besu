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
package org.hyperledger.besu.ethereum.trie.pathbased.common.account;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BinaryStorageRootStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.MptStorageRootStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class BonsaiAccountStorageRootStrategyTest {

  @Test
  void binaryRoundTripProducesThreeFieldRlpAndThrowsOnRootAccessors() {
    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000aa");
    final long nonce = 7L;
    final Wei balance = Wei.of(1_234_567L);
    final Hash codeHash = Hash.hash(Bytes.fromHexString("0x60016000"));

    final BonsaiAccount original =
        new BonsaiAccount(
            null,
            address,
            address.addressHash(),
            nonce,
            balance,
            BinaryStorageRootStrategy.INSTANCE,
            codeHash,
            true,
            new BonsaiCodeCache());

    // Binary account => no storage root. Safe accessor:
    assertThat(original.hasStorageRoot()).isFalse();
    // MPT-specific accessors throw for binary accounts.
    assertThatThrownBy(original::getStorageRoot).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(original::isStorageEmpty).isInstanceOf(UnsupportedOperationException.class);

    // writeTo produces 3-field RLP via the binary strategy (no throw).
    final Bytes encoded = original.serializeAccount();
    // Binary 3-field RLP must be strictly shorter than the MPT 4-field RLP of an equivalent
    // account.
    final BonsaiAccount mptEquivalent =
        new BonsaiAccount(
            null,
            address,
            address.addressHash(),
            nonce,
            balance,
            new MptStorageRootStrategy(Hash.EMPTY_TRIE_HASH),
            codeHash,
            true,
            new BonsaiCodeCache());
    assertThat(encoded.size()).isLessThan(mptEquivalent.serializeAccount().size());

    final BonsaiAccount decoded =
        BonsaiAccount.fromFlatBytes(
            null,
            address,
            encoded,
            true,
            new BonsaiCodeCache(),
            BinaryStorageRootStrategy.INSTANCE);

    assertThat(decoded.getNonce()).isEqualTo(nonce);
    assertThat(decoded.getBalance()).isEqualTo(balance);
    assertThat(decoded.getCodeHash()).isEqualTo(codeHash);
    assertThat(decoded.hasStorageRoot()).isFalse();
    assertThatThrownBy(decoded::getStorageRoot).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(decoded::isStorageEmpty).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void mptRoundTripProducesFourFieldRlp() {
    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000bb");
    final Hash storageRoot = Hash.hash(Bytes.of(1, 2, 3));
    final BonsaiAccount account =
        new BonsaiAccount(
            null,
            address,
            address.addressHash(),
            1L,
            Wei.ONE,
            new MptStorageRootStrategy(storageRoot),
            Hash.EMPTY,
            true,
            new BonsaiCodeCache());

    final Bytes encoded = account.serializeAccount();
    final BonsaiAccount decoded =
        BonsaiAccount.fromFlatBytes(
            null,
            address,
            encoded,
            true,
            new BonsaiCodeCache(),
            new MptStorageRootStrategy(Hash.EMPTY_TRIE_HASH));

    assertThat(decoded.getNonce()).isEqualTo(1L);
    assertThat(decoded.getBalance()).isEqualTo(Wei.ONE);
    assertThat(decoded.getCodeHash()).isEqualTo(Hash.EMPTY);
    assertThat(decoded.hasStorageRoot()).isTrue();
    assertThat(decoded.getStorageRoot()).isEqualTo(storageRoot);
  }

  @Test
  void binaryStrategyReadsLegacyMptRlpAndConsumesStorageRoot() {
    // A node migrating from MPT to binary still has flat-DB accounts encoded as 4-field MPT RLP
    // [nonce, balance, storageRoot, codeHash]. The binary strategy must consume+discard the
    // storageRoot so the following codeHash read stays aligned.
    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000cc");
    final long nonce = 42L;
    final Wei balance = Wei.of(99L);
    final Hash legacyStorageRoot = Hash.hash(Bytes.of(7, 7, 7));
    final Hash codeHash = Hash.hash(Bytes.fromHexString("0x6001"));

    // Encode as MPT (4 fields) using an MPT-strategy account.
    final BonsaiAccount mptEncoded =
        new BonsaiAccount(
            null,
            address,
            address.addressHash(),
            nonce,
            balance,
            new MptStorageRootStrategy(legacyStorageRoot),
            codeHash,
            true,
            new BonsaiCodeCache());
    final Bytes legacyMptRlp = mptEncoded.serializeAccount();

    // Decode the legacy 4-field RLP with the BINARY strategy: storageRoot consumed+ignored.
    final BonsaiAccount decoded =
        BonsaiAccount.fromFlatBytes(
            null,
            address,
            legacyMptRlp,
            true,
            new BonsaiCodeCache(),
            BinaryStorageRootStrategy.INSTANCE);

    assertThat(decoded.getNonce()).isEqualTo(nonce);
    assertThat(decoded.getBalance()).isEqualTo(balance);
    // codeHash must be the real codeHash, not the legacy storageRoot (alignment check).
    assertThat(decoded.getCodeHash()).isEqualTo(codeHash);
    assertThat(decoded.hasStorageRoot()).isFalse();
    assertThatThrownBy(decoded::getStorageRoot).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void assertStorageRootMatchesMptVsMptEqual() {
    final Hash root = Hash.hash(Bytes.of(1, 2, 3));
    final MptStorageRootStrategy a = new MptStorageRootStrategy(root);
    final BonsaiAccount b = mptAccountWithRoot(root);
    a.assertStorageRootMatches(b, "test");
    // no exception thrown
  }

  @Test
  void assertStorageRootMatchesMptVsMptDifferThrows() {
    final MptStorageRootStrategy a = new MptStorageRootStrategy(Hash.hash(Bytes.of(1)));
    final BonsaiAccount b = mptAccountWithRoot(Hash.hash(Bytes.of(2)));
    assertThatThrownBy(() -> a.assertStorageRootMatches(b, "ctx"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Storage Roots differ");
  }

  @Test
  void assertStorageRootMatchesMptVsBinaryThrows() {
    final MptStorageRootStrategy mpt = new MptStorageRootStrategy(Hash.EMPTY_TRIE_HASH);
    final BonsaiAccount binary = binaryAccount();
    assertThatThrownBy(() -> mpt.assertStorageRootMatches(binary, "ctx"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("mpt vs binary");
  }

  @Test
  void assertStorageRootMatchesBinaryVsMptIsNoOp() {
    BinaryStorageRootStrategy.INSTANCE.assertStorageRootMatches(
        mptAccountWithRoot(Hash.EMPTY_TRIE_HASH), "ctx");
    BinaryStorageRootStrategy.INSTANCE.assertStorageRootMatches(binaryAccount(), "ctx");
    // no exception thrown — binary diffs do not assert on storage root
  }

  private static BonsaiAccount mptAccountWithRoot(final Hash storageRoot) {
    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000dd");
    return new BonsaiAccount(
        null,
        address,
        address.addressHash(),
        1L,
        Wei.ONE,
        new MptStorageRootStrategy(storageRoot),
        Hash.EMPTY,
        true,
        new BonsaiCodeCache());
  }

  private static BonsaiAccount binaryAccount() {
    final Address address = Address.fromHexString("0x00000000000000000000000000000000000000ee");
    return new BonsaiAccount(
        null,
        address,
        address.addressHash(),
        1L,
        Wei.ONE,
        BinaryStorageRootStrategy.INSTANCE,
        Hash.EMPTY,
        true,
        new BonsaiCodeCache());
  }
}
