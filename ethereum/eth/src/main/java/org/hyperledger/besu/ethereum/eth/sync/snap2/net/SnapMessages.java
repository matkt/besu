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
package org.hyperledger.besu.ethereum.eth.sync.snap2.net;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV2;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Encodes snap/2 requests and decodes snap/2 responses without intermediate maps. */
public final class SnapMessages {

  private static final Bytes EMPTY_TRIE_ROOT = Hash.EMPTY_TRIE_HASH.getBytes();
  private static final Bytes EMPTY_CODE_HASH = Hash.EMPTY.getBytes();

  private SnapMessages() {}

  /** Request for a range of accounts. */
  public static MessageData getAccountRange(
      final Bytes32 root, final Bytes32 origin, final Bytes32 limit, final long bytes) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(root);
    out.writeBytes(origin);
    out.writeBytes(limit);
    out.writeLongScalar(bytes);
    out.endList();
    return new Request(SnapV2.GET_ACCOUNT_RANGE, out.encoded());
  }

  /** Request for the storage of several accounts, or a slice of the storage of one account. */
  public static MessageData getStorageRanges(
      final Bytes32 root,
      final List<Bytes32> accounts,
      final Bytes32 origin,
      final Bytes32 limit,
      final long bytes) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeBytes(root);
    out.writeList(accounts, (account, o) -> o.writeBytes(account));
    out.writeBytes(origin);
    out.writeBytes(limit);
    out.writeLongScalar(bytes);
    out.endList();
    return new Request(SnapV2.GET_STORAGE_RANGE, out.encoded());
  }

  /** Request for bytecodes by hash. */
  public static MessageData getByteCodes(final List<Bytes32> codeHashes, final long bytes) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeList(codeHashes, (hash, o) -> o.writeBytes(hash));
    out.writeLongScalar(bytes);
    out.endList();
    return new Request(SnapV2.GET_BYTECODES, out.encoded());
  }

  /** Request for block access lists by block hash. */
  public static MessageData getBlockAccessLists(final List<Hash> blockHashes, final long bytes) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeList(blockHashes, (hash, o) -> o.writeBytes(hash.getBytes()));
    out.writeLongScalar(bytes);
    out.endList();
    return new Request(SnapV2.GET_BLOCK_ACCESS_LISTS, out.encoded());
  }

  /**
   * Decoded account range.
   *
   * @param hashes the account hashes
   * @param accounts the accounts, re-encoded in the full trie format
   * @param proof the boundary proof nodes
   */
  public record AccountRange(List<Bytes32> hashes, List<Bytes> accounts, List<Bytes> proof) {}

  /**
   * Decoded storage ranges, one list per served account.
   *
   * @param slotHashes the slot hashes per account
   * @param slotValues the RLP encoded slot values per account, as stored in the trie
   * @param proof the proof of the last range, empty when every range is complete
   */
  public record StorageRanges(
      List<List<Bytes32>> slotHashes, List<List<Bytes>> slotValues, List<Bytes> proof) {}

  /** Decodes an AccountRange response. */
  public static AccountRange accountRange(final MessageData message) {
    checkCode(message, SnapV2.ACCOUNT_RANGE);
    final RLPInput in = enterResponse(message);
    final List<Bytes32> hashes = new ArrayList<>();
    final List<Bytes> accounts = new ArrayList<>();
    in.enterList();
    while (!in.isEndOfCurrentList()) {
      in.enterList();
      hashes.add(in.readBytes32());
      accounts.add(toFullAccount(in));
      in.leaveList();
    }
    in.leaveList();
    final List<Bytes> proof = readByteList(in);
    return new AccountRange(hashes, accounts, proof);
  }

  /** Decodes a StorageRanges response. */
  public static StorageRanges storageRanges(final MessageData message) {
    checkCode(message, SnapV2.STORAGE_RANGE);
    final RLPInput in = enterResponse(message);
    final List<List<Bytes32>> slotHashes = new ArrayList<>();
    final List<List<Bytes>> slotValues = new ArrayList<>();
    in.enterList();
    while (!in.isEndOfCurrentList()) {
      final List<Bytes32> hashes = new ArrayList<>();
      final List<Bytes> values = new ArrayList<>();
      in.enterList();
      while (!in.isEndOfCurrentList()) {
        in.enterList();
        hashes.add(in.readBytes32());
        values.add(in.readBytes());
        in.leaveList();
      }
      in.leaveList();
      slotHashes.add(hashes);
      slotValues.add(values);
    }
    in.leaveList();
    final List<Bytes> proof = readByteList(in);
    return new StorageRanges(slotHashes, slotValues, proof);
  }

  /** Decodes a ByteCodes response. */
  public static List<Bytes> byteCodes(final MessageData message) {
    checkCode(message, SnapV2.BYTECODES);
    return readByteList(enterResponse(message));
  }

  /**
   * Decodes a BlockAccessLists response; unavailable entries are returned as empty bytes.
   *
   * @return the raw RLP of each block access list, in request order
   */
  public static List<Bytes> blockAccessLists(final MessageData message) {
    checkCode(message, SnapV2.BLOCK_ACCESS_LISTS);
    final RLPInput in = enterResponse(message);
    final List<Bytes> out = new ArrayList<>();
    in.enterList();
    while (!in.isEndOfCurrentList()) {
      if (in.nextIsList()) {
        out.add(in.readAsRlp().raw());
      } else {
        out.add(in.readBytes());
      }
    }
    in.leaveList();
    return out;
  }

  /** Converts a slim account body, positioned on {@code in}, to the full trie encoding. */
  static Bytes toFullAccount(final RLPInput in) {
    in.enterList();
    if (in.nextIsList()) {
      throw new RLPException("invalid account nonce");
    }
    final Bytes nonce = in.readAsRlp().raw();
    if (in.nextIsList()) {
      throw new RLPException("invalid account balance");
    }
    final Bytes balance = in.readAsRlp().raw();
    final Bytes storageRoot = readHashOrDefault(in, EMPTY_TRIE_ROOT);
    final Bytes codeHash = readHashOrDefault(in, EMPTY_CODE_HASH);
    in.leaveList();
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeRaw(nonce);
    out.writeRaw(balance);
    out.writeBytes(storageRoot);
    out.writeBytes(codeHash);
    out.endList();
    return out.encoded();
  }

  private static Bytes readHashOrDefault(final RLPInput in, final Bytes defaultValue) {
    final Bytes value = in.readBytes();
    if (value.isEmpty()) {
      return defaultValue;
    }
    if (value.size() != 32) {
      throw new RLPException("invalid hash size " + value.size());
    }
    return value;
  }

  private static List<Bytes> readByteList(final RLPInput in) {
    final List<Bytes> out = new ArrayList<>();
    if (in.isEndOfCurrentList()) {
      return out;
    }
    in.enterList();
    while (!in.isEndOfCurrentList()) {
      out.add(in.readBytes());
    }
    in.leaveList();
    return out;
  }

  // positions the input after the request id
  private static RLPInput enterResponse(final MessageData message) {
    final RLPInput in = RLP.input(message.getData());
    in.enterList();
    in.skipNext();
    return in;
  }

  private static void checkCode(final MessageData message, final int expected) {
    if (message.getCode() != expected) {
      throw new IllegalArgumentException(
          "unexpected message code " + message.getCode() + ", expected " + expected);
    }
  }

  private static final class Request extends AbstractSnapMessageData {
    private final int code;

    private Request(final int code, final Bytes data) {
      super(data);
      this.code = code;
    }

    @Override
    public int getCode() {
      return code;
    }
  }
}
