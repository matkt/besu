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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.evm.account.AccountStorageEntry;
import org.hyperledger.besu.evm.code.CodeV0;
import org.hyperledger.besu.evm.worldstate.UpdateTrackingAccount;

import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class BinTrieAccount extends PathBasedAccount {

  private long codeSize;
  private int hashCode;

  public BinTrieAccount(
      final PathBasedWorldView context,
      final Address address,
      final Hash addressHash,
      final long nonce,
      final Wei balance,
      final long codeSize,
      final Hash codeHash,
      final boolean mutable) {
    super(context, address, addressHash, nonce, balance, codeHash, mutable);
    this.codeSize = codeSize;
  }

  public BinTrieAccount(
      final PathBasedWorldView context,
      final Address address,
      final AccountValue stateTrieAccount,
      final boolean mutable) {
    super(context, address, stateTrieAccount, mutable);
    this.codeSize = stateTrieAccount.getCodeSize().orElse(0L);
  }

  public BinTrieAccount(final BinTrieAccount toCopy) {
    this(toCopy, toCopy.context, false);
  }

  public BinTrieAccount(
      final BinTrieAccount toCopy, final PathBasedWorldView context, final boolean mutable) {
    super(toCopy, context, mutable);
    this.codeSize = toCopy.codeSize;
  }

  public BinTrieAccount(
      final PathBasedWorldView context, final UpdateTrackingAccount<BinTrieAccount> tracked) {
    super(
        context,
        tracked.getAddress(),
        tracked.getAddressHash(),
        tracked.getNonce(),
        tracked.getBalance(),
        tracked.getCodeHash(),
        new CodeV0(tracked.getCode()),
        true);
    this.codeSize = tracked.getCodeSize().orElse(0L);
    updatedStorage.putAll(tracked.getUpdatedStorage());
  }

  public static BinTrieAccount fromLegacyFormat(
      final PathBasedWorldView context,
      final Address address,
      final Bytes encoded,
      final boolean mutable)
      throws RLPException {
    final RLPInput in = RLP.input(encoded);
    in.enterList();

    final long nonce = in.readLongScalar();
    final Wei balance = Wei.of(in.readUInt256Scalar());
    final Hash codeHash = Hash.wrap(in.readBytes32());
    final long codeSize = in.readLongScalar();
    in.leaveList();

    return new BinTrieAccount(
        context, address, address.addressHash(), nonce, balance, codeSize, codeHash, mutable);
  }

  @Override
  public Optional<Long> getCodeSize() {
    return Optional.of(codeSize);
  }

  @Override
  public void setCode(final Bytes code) {
    super.setCode(code);
    if (code == null || code.isEmpty()) {
      this.codeSize = 0L;
    } else {
      this.codeSize = code.size();
    }
  }

  @Override
  public Hash getStorageRoot() {
    // BinTrie accounts do not have a storage root.
    // For now, a BinTrie account will always return EMPTY for this value,
    // but this will need to be addressed in a future refactor.
    return Hash.EMPTY_TRIE_HASH;
  }

  @Override
  public NavigableMap<Bytes32, AccountStorageEntry> storageEntriesFrom(
      final Bytes32 startKeyHash, final int limit) {
    throw new RuntimeException(
        "The method storageEntriesFrom is not supported when using Binary tries. Binary tries manage storage differently");
  }

  @Override
  public void writeTo(final RLPOutput out) {
    out.startList();

    out.writeLongScalar(nonce);
    out.writeUInt256Scalar(balance);
    out.writeBytes(codeHash);
    out.writeLongScalar(codeSize);

    out.endList();
  }

  @Override
  public String toString() {
    return "AccountState{"
        + "address="
        + address
        + ", nonce="
        + nonce
        + ", balance="
        + balance
        + ", codeSize="
        + codeSize
        + ", codeHash="
        + codeHash
        + '}';
  }

  /**
   * Throws an exception if the two accounts represent different stored states
   *
   * @param source The bonsai account to compare
   * @param account The State Trie account to compare
   * @param context a description to be added to the thrown exceptions
   * @throws IllegalStateException if the stored values differ
   */
  public static void assertCloseEnoughForDiffing(
      final BinTrieAccount source, final AccountValue account, final String context) {
    if (source == null) {
      throw new IllegalStateException(context + ": source is null but target isn't");
    } else {
      if (source.nonce != account.getNonce()) {
        throw new IllegalStateException(context + ": nonces differ");
      }
      if (!Objects.equals(source.balance, account.getBalance())) {
        throw new IllegalStateException(context + ": balances differ");
      }
      if (source.codeSize != account.getCodeSize().orElse(0L)) {
        throw new IllegalStateException(context + ": codeSize differ");
      }
    }
  }

  @Override
  public boolean isStorageEmpty() {
    return true; // TODO need to find a way to manage that with binary tries
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof BinTrieAccount)) {
      return false;
    }
    BinTrieAccount otherAccount = (BinTrieAccount) other;
    return Objects.equals(this.address, otherAccount.address)
        && this.nonce == otherAccount.nonce
        && Objects.equals(this.balance, otherAccount.balance)
        && Objects.equals(this.codeHash, otherAccount.codeHash)
        && this.codeSize == otherAccount.codeSize;
  }

  @Override
  public int hashCode() {
    if (!immutable) {
      return computeHashCode();
    }
    if (hashCode == 0) {
      hashCode = computeHashCode();
    }
    return hashCode;
  }

  private int computeHashCode() {
    return Objects.hash(address, nonce, balance, codeHash, codeSize);
  }
}
