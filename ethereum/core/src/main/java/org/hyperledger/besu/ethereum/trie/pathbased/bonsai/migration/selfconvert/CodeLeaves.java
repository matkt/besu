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

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec.Leaf;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.convert.Eip8347Leaves;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter.Entry;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347Pipelines;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.code.CodeStorageStrategy;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;
import org.hyperledger.besu.plugin.services.storage.SnappableKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SnappedKeyValueStorage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * The leaves of the contracts. Each code is read and chunked once, then gives the header leaves of
 * the accounts holding it and its reference count.
 */
final class CodeLeaves {

  /** Max accounts of one code in a group. */
  private static final int REFS_PER_GROUP = 1024;

  /** The weight of a code's chunks in a worker's list: at most 32 codes per list. */
  private static final int CODE_WEIGHT = Eip8347Pipelines.LIST_WEIGHT / 32;

  /** Leaves of {@code contracts} contracts, and the reference counts of their codes by hash. */
  record Coded(List<Leaf> leaves, List<Entry> referenceCounts, int contracts) {}

  /** Accounts holding one code; the code's last group carries all its {@code references}. */
  record Group(Bytes32 codeHash, List<CodeRef> refs, boolean first, long references) {}

  private CodeLeaves() {}

  /** A contract waiting for its code. */
  record CodeRef(Bytes32 codeHash, Address address, long nonce, Wei balance) {

    /** Key {@code codeHash | address}, to sort by code; value {@code nonce | balance}. */
    Entry encode() {
      final byte[] key =
          ByteBuffer.allocate(Bytes32.SIZE + Address.SIZE)
              .put(codeHash.toArrayUnsafe())
              .put(address.getBytes().toArrayUnsafe())
              .array();
      final byte[] balanceBytes = balance.toMinimalBytes().toArrayUnsafe();
      final byte[] value =
          ByteBuffer.allocate(Long.BYTES + balanceBytes.length)
              .putLong(nonce)
              .put(balanceBytes)
              .array();
      return new Entry(key, value);
    }

    static CodeRef of(
        final Hash codeHash, final Address address, final long nonce, final Wei balance) {
      return new CodeRef(Bytes32.wrap(codeHash.getBytes()), address, nonce, balance);
    }

    static CodeRef decode(final Entry entry) {
      final byte[] value = entry.value();
      return new CodeRef(
          Bytes32.wrap(entry.key(), 0),
          Address.wrap(Bytes.wrap(entry.key(), Bytes32.SIZE, Address.SIZE)),
          ByteBuffer.wrap(value).getLong(),
          // no balance bytes for a zero balance
          Wei.of(UInt256.fromBytes(Bytes.wrap(value).slice(Long.BYTES))));
    }
  }

  /** The weight of a group in a worker's list: its accounts, and its code's chunks once. */
  static int weight(final Group group) {
    return group.refs().size() + (group.first() ? CODE_WEIGHT : 0);
  }

  /** Groups contracts sorted by code, at most {@link #REFS_PER_GROUP} accounts per group. */
  static Iterator<Group> groups(final Iterator<Entry> sortedRefs) {
    final PeekingIterator<Entry> refs = Iterators.peekingIterator(sortedRefs);
    return new Iterator<>() {
      private Bytes32 codeHash;
      private long references;

      @Override
      public boolean hasNext() {
        return refs.hasNext();
      }

      @Override
      public Group next() {
        final CodeRef firstRef = CodeRef.decode(refs.next());
        final boolean first = !firstRef.codeHash().equals(codeHash);
        if (first) {
          codeHash = firstRef.codeHash();
          references = 0;
        }
        final List<CodeRef> group = new ArrayList<>();
        group.add(firstRef);
        while (group.size() < REFS_PER_GROUP && refs.hasNext() && sameCode(refs.peek())) {
          group.add(CodeRef.decode(refs.next()));
        }
        references += group.size();
        final boolean last = !refs.hasNext() || !sameCode(refs.peek());
        return new Group(codeHash, group, first, last ? references : 0);
      }

      private boolean sameCode(final Entry ref) {
        return Arrays.equals(ref.key(), 0, Bytes32.SIZE, codeHash.toArrayUnsafe(), 0, Bytes32.SIZE);
      }
    };
  }

  /** The leaves of {@code groups}; their codes come from one snapshot, outside the block cache. */
  static Coded leaves(
      final SnappableKeyValueStorage storage,
      final CodeStorageStrategy codes,
      final List<Group> groups) {
    final List<Leaf> leaves = new ArrayList<>();
    final List<Entry> referenceCounts = new ArrayList<>();
    int contracts = 0;
    try (final SnappedKeyValueStorage snapshot = storage.takeSnapshot()) {
      Bytes32 codeHash = null;
      Bytes code = null;
      for (final Group group : groups) {
        if (!group.codeHash().equals(codeHash)) {
          codeHash = group.codeHash();
          code = code(snapshot, codes, codeHash);
        }
        contracts += group.refs().size();
        for (final CodeRef ref : group.refs()) {
          leaves.addAll(
              Eip8347Leaves.header(
                  TrieKeyDerivation.address20ToAddress32(ref.address().getBytes()),
                  ref.nonce(),
                  ref.balance(),
                  Hash.wrap(codeHash),
                  code));
        }
        if (CodeDelegationHelper.hasCodeDelegation(code)) {
          continue;
        }
        if (group.first()) {
          leaves.addAll(Eip8347Leaves.code(codeHash, code));
        }
        if (group.references() > 0) {
          referenceCounts.add(
              new Entry(
                  codeHash.toArrayUnsafe(),
                  Eip8347Leaves.referenceCount(group.references(), code.size()).toArrayUnsafe()));
        }
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return new Coded(leaves, referenceCounts, contracts);
  }

  private static Bytes code(
      final SnappedKeyValueStorage snapshot,
      final CodeStorageStrategy codes,
      final Bytes32 codeHash) {
    return snapshot
        .getUncached(CODE_STORAGE, codeHash.toArrayUnsafe())
        .map(codes::codeBytes)
        .orElseThrow(() -> new IllegalStateException("no code for code hash " + codeHash));
  }
}
