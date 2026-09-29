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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.verify;

import static org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters.STEM_SUBTREE_WIDTH;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.codec.CodeChunkifier;
import org.hyperledger.besu.ethereum.partitionedbinarytrie.keys.TrieKeyDerivation;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347ArtifactVerificationException;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347SnapshotReader.CodeUnit;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact.Eip8347TypedSnapshotCodec;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347ExternalSorter.Entry;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.pipeline.Eip8347Pipelines;
import org.hyperledger.besu.evm.worldstate.CodeDelegationHelper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.bytes.MutableBytes;

/**
 * Code limb of the dual-check: anchors CODE_ZONE leaves and {@code codeSize} to the {@code
 * code_hash} values the MPT commits.
 *
 * <ol>
 *   <li>Each {@code kind 0x01} header requests the code groups its {@code (codeHash, codeSize)}
 *       spans, keyed by group stem and externally sorted. Equal stems collapse, so each code is
 *       checked once; two sizes for one {@code code_hash} are rejected.
 *   <li>The CODE_ZONE section is merged against those requests in stem order: an unrequested group
 *       is a surplus code leaf; a requested but absent group is all-zero chunks.
 *   <li>Groups are re-sorted by {@code (codeHash, groupIndex)} and each code is reassembled, hashed
 *       and re-chunked on its own, in parallel. Heap holds a few codes, never all of them.
 * </ol>
 *
 * <pre>
 * request key  CODE_ZONE|stemHash (33 B)      value  codeHash[32] | group[4] | codeSize[4]
 * group key    codeHash[32] | group[4]        value  codeSize[4] | (subIndex[1] | chunk[32])*
 * </pre>
 */
final class Eip8347CodeCheck {

  /**
   * Largest {@code codeSize} this verifier reassembles. Not a spec limit: a DoS guard so a hostile
   * header cannot make the verifier allocate gigabytes. Far above any deployable code size.
   */
  static final long MAX_CODE_SIZE = 1L << 20;

  private static final int CHUNK_BYTES = 31;
  private static final int CODES_IN_FLIGHT = 16;

  private final Eip8347ExternalSorter requests;
  private final Eip8347ExternalSorter groups;
  private PeekingIterator<Entry> sortedRequests;

  /** One code with every group its size spans, in group order (empty map: no leaf there). */
  private record Code(
      Bytes32 codeHash, int codeSize, List<NavigableMap<Integer, Bytes32>> groups) {}

  Eip8347CodeCheck(final Eip8347ExternalSorter requests, final Eip8347ExternalSorter groups) {
    this.requests = requests;
    this.groups = groups;
  }

  /** Registers a {@code kind 0x01} header. Must precede the first {@link #group} call. */
  void request(final Bytes32 codeHash, final long codeSize) throws IOException {
    if (sortedRequests != null) {
      throw new IllegalStateException("code requested after the CODE_ZONE section started");
    }
    if (codeSize > MAX_CODE_SIZE) {
      throw new Eip8347ArtifactVerificationException(
          "codeSize " + codeSize + " of " + codeHash.toHexString() + " exceeds " + MAX_CODE_SIZE);
    }
    for (int g = 0; g < groupCount(codeSize); g++) {
      requests.add(
          stem(codeHash, g).toArray(),
          ByteBuffer.allocate(40)
              .put(codeHash.toArrayUnsafe())
              .putInt(g)
              .putInt((int) codeSize)
              .array());
    }
  }

  /** Merges one CODE_ZONE group (units arrive in ascending stem order). */
  void group(final CodeUnit unit) throws IOException {
    final PeekingIterator<Entry> pending = sortedRequests();
    final byte[] stem = unit.stem().toArrayUnsafe();
    while (pending.hasNext() && Arrays.compareUnsigned(pending.peek().key(), stem) < 0) {
      emit(nextDistinct(pending), null);
    }
    if (!pending.hasNext() || Arrays.compareUnsigned(pending.peek().key(), stem) > 0) {
      throw new Eip8347ArtifactVerificationException(
          "code group "
              + unit.stem().toHexString()
              + " is not referenced by any header record's code_hash");
    }
    emit(nextDistinct(pending), unit.group().entries());
  }

  /** Completes the merge, then verifies every referenced code. */
  void finish() throws IOException {
    final PeekingIterator<Entry> pending = sortedRequests();
    while (pending.hasNext()) {
      emit(nextDistinct(pending), null);
    }
    Eip8347Pipelines.run(
        Eip8347Pipelines.from("eip8347-verify-codes", codes(groups.sorted()), CODES_IN_FLIGHT)
            .thenProcessInParallel(
                "eip8347-verify-code", Eip8347CodeCheck::verify, Eip8347Pipelines.PARALLELISM)
            .andFinishWith("eip8347-verify-code-done", ok -> {}));
  }

  private PeekingIterator<Entry> sortedRequests() throws IOException {
    if (sortedRequests == null) {
      sortedRequests = Iterators.peekingIterator(requests.sorted());
    }
    return sortedRequests;
  }

  /** Next request, folding identical ones (headers sharing a code) and rejecting size conflicts. */
  private static Entry nextDistinct(final PeekingIterator<Entry> pending) {
    final Entry first = pending.next();
    while (pending.hasNext() && Arrays.equals(pending.peek().key(), first.key())) {
      if (!Arrays.equals(pending.next().value(), first.value())) {
        throw new Eip8347ArtifactVerificationException(
            "header records disagree on codeSize for code_hash "
                + Bytes.wrap(first.value(), 0, 32).toHexString());
      }
    }
    return first;
  }

  /** Re-keys a request (with its group's entries, if present) by {@code codeHash | group}. */
  private void emit(final Entry request, final NavigableMap<Integer, Bytes32> entries)
      throws IOException {
    final ByteBuffer value =
        ByteBuffer.allocate(4 + (entries == null ? 0 : entries.size() * 33))
            .put(request.value(), 36, 4);
    if (entries != null) {
      entries.forEach((sub, chunk) -> value.put(sub.byteValue()).put(chunk.toArrayUnsafe()));
    }
    groups.add(Arrays.copyOf(request.value(), 36), value.array());
  }

  /** Regroups the sorted group rows into one {@link Code} per {@code code_hash}. */
  private static Iterator<Code> codes(final Iterator<Entry> rows) {
    final PeekingIterator<Entry> it = Iterators.peekingIterator(rows);
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public Code next() {
        if (!it.hasNext()) {
          throw new NoSuchElementException();
        }
        final Bytes32 codeHash = Bytes32.wrap(it.peek().key(), 0);
        final int codeSize = ByteBuffer.wrap(it.peek().value()).getInt();
        final List<NavigableMap<Integer, Bytes32>> codeGroups = new ArrayList<>();
        while (it.hasNext() && Bytes32.wrap(it.peek().key(), 0).equals(codeHash)) {
          final ByteBuffer row = ByteBuffer.wrap(it.next().value()).position(4);
          final NavigableMap<Integer, Bytes32> entries = new TreeMap<>();
          while (row.hasRemaining()) {
            final int sub = row.get() & 0xFF;
            final byte[] chunk = new byte[32];
            row.get(chunk);
            entries.put(sub, Bytes32.wrap(chunk));
          }
          codeGroups.add(entries);
        }
        return new Code(codeHash, codeSize, codeGroups);
      }
    };
  }

  /** Reassembles, hashes and re-chunks one code (EIP-8347 dual-check, code limb). */
  private static Boolean verify(final Code code) {
    final int chunkCount = (code.codeSize() + CHUNK_BYTES - 1) / CHUNK_BYTES;
    final MutableBytes bytecode = MutableBytes.create(code.codeSize());
    for (int g = 0; g < code.groups().size(); g++) {
      for (final var entry : code.groups().get(g).entrySet()) {
        final int chunkId = g * STEM_SUBTREE_WIDTH + entry.getKey();
        if (chunkId >= chunkCount) {
          throw new Eip8347ArtifactVerificationException(
              "code leaf "
                  + chunkId
                  + " lies beyond codeSize for "
                  + code.codeHash().toHexString());
        }
        final int offset = chunkId * CHUNK_BYTES;
        final int length = Math.min(CHUNK_BYTES, code.codeSize() - offset);
        bytecode.set(offset, entry.getValue().slice(1, length));
      }
    }
    if (CodeDelegationHelper.hasCodeDelegation(bytecode)) {
      throw new Eip8347ArtifactVerificationException(
          "kind 0x01 account recovered a delegation indicator for "
              + code.codeHash().toHexString());
    }
    if (!Hash.hash(bytecode).getBytes().equals(code.codeHash())) {
      throw new Eip8347ArtifactVerificationException(
          "reassembled code hash mismatch for claimed " + code.codeHash().toHexString());
    }
    final List<Bytes32> expected = CodeChunkifier.chunkifyCode(bytecode);
    for (int g = 0; g < code.groups().size(); g++) {
      final NavigableMap<Integer, Bytes32> expectedGroup = new TreeMap<>();
      final int end = Math.min(expected.size(), (g + 1) * STEM_SUBTREE_WIDTH);
      for (int chunkId = g * STEM_SUBTREE_WIDTH; chunkId < end; chunkId++) {
        if (!expected.get(chunkId).isZero()) {
          expectedGroup.put(chunkId % STEM_SUBTREE_WIDTH, expected.get(chunkId));
        }
      }
      if (!expectedGroup.equals(code.groups().get(g))) {
        throw new Eip8347ArtifactVerificationException(
            "code chunks of group "
                + g
                + " do not match the re-chunked bytecode for "
                + code.codeHash().toHexString());
      }
    }
    return Boolean.TRUE;
  }

  private static int groupCount(final long codeSize) {
    final long chunks = (codeSize + CHUNK_BYTES - 1) / CHUNK_BYTES;
    return (int) ((chunks + STEM_SUBTREE_WIDTH - 1) / STEM_SUBTREE_WIDTH);
  }

  private static Bytes stem(final Bytes32 codeHash, final int group) {
    return Eip8347TypedSnapshotCodec.stemOf(
        TrieKeyDerivation.getTreeKeyForCodeChunk(codeHash, group * STEM_SUBTREE_WIDTH));
  }
}
