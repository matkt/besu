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
package org.hyperledger.besu.ethereum.eth.sync.snap2.trie;

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Verifies snap range responses: the returned leaves must be exactly the leaves of the trie between
 * the requested origin and the last returned key, with no gap.
 *
 * <p>The proof nodes are walked along both range boundaries. Subtrees outside the range are kept as
 * sealed references, subtrees inside it are dropped, and the root is recomputed from the sealed
 * subtrees and the returned leaves with a {@link StackTrie}.
 */
public final class RangeProof {

  private static final byte[] MAX_KEY = Bytes32.fromHexString("ff".repeat(32)).toArrayUnsafe();

  private RangeProof() {}

  /**
   * Outcome of a verification.
   *
   * @param valid whether the response is a correct slice of the trie
   * @param hasMore whether the trie holds keys after the last returned key
   */
  public record Result(boolean valid, boolean hasMore) {
    static final Result INVALID = new Result(false, false);
  }

  /**
   * Verifies a range response.
   *
   * @param rootHash the trie root
   * @param origin the requested starting key
   * @param keys the returned keys, strictly increasing and not below origin
   * @param values the returned leaf values, RLP encoded as stored in the trie
   * @param proof the boundary proof nodes, empty when the response claims the whole trie
   * @return the verification result
   */
  public static Result verify(
      final Bytes32 rootHash,
      final Bytes32 origin,
      final List<Bytes32> keys,
      final List<Bytes> values,
      final List<Bytes> proof) {
    if (keys.size() != values.size() || !isSortedFrom(origin, keys)) {
      return Result.INVALID;
    }
    try {
      if (proof.isEmpty()) {
        if (keys.isEmpty() || !origin.isZero()) {
          return Result.INVALID;
        }
        return computeRoot(keys, values).equals(rootHash)
            ? new Result(true, false)
            : Result.INVALID;
      }
      final Map<Bytes32, Bytes> nodes = new HashMap<>(proof.size() * 2);
      for (final Bytes node : proof) {
        nodes.put(NodeEncoding.keccak(node), node);
      }
      final Walker walker =
          new Walker(
              nodes,
              origin.toArrayUnsafe(),
              keys.isEmpty() ? MAX_KEY : keys.get(keys.size() - 1).toArrayUnsafe());
      walker.visit(NodeEncoding.hashRef(rootHash), Bytes.EMPTY);

      final StackTrie trie = new StackTrie(null);
      for (final Sealed sealed : walker.left) {
        trie.addSealed(sealed.location(), sealed.ref());
      }
      for (int i = 0; i < keys.size(); i++) {
        trie.addLeaf(keys.get(i), values.get(i));
      }
      for (final Sealed sealed : walker.right) {
        trie.addSealed(sealed.location(), sealed.ref());
      }
      return trie.rootHash().equals(rootHash)
          ? new Result(true, !walker.right.isEmpty())
          : Result.INVALID;
    } catch (final RLPException | IllegalArgumentException | IllegalStateException e) {
      return Result.INVALID;
    }
  }

  /** Computes the root of a trie holding exactly the given sorted leaves. */
  public static Bytes32 computeRoot(final List<Bytes32> keys, final List<Bytes> values) {
    final StackTrie trie = new StackTrie(null);
    for (int i = 0; i < keys.size(); i++) {
      trie.addLeaf(keys.get(i), values.get(i));
    }
    return trie.rootHash();
  }

  private static boolean isSortedFrom(final Bytes32 origin, final List<Bytes32> keys) {
    Bytes32 previous = null;
    for (final Bytes32 key : keys) {
      if (previous == null ? key.compareTo(origin) < 0 : key.compareTo(previous) <= 0) {
        return false;
      }
      previous = key;
    }
    return true;
  }

  private record Sealed(Bytes location, Bytes ref) {}

  private enum Relation {
    LEFT,
    RIGHT,
    INSIDE,
    OVERLAP
  }

  private static final class Walker {
    private final Map<Bytes32, Bytes> nodes;
    private final byte[] origin;
    private final byte[] rightBound;
    private final List<Sealed> left = new ArrayList<>();
    private final List<Sealed> right = new ArrayList<>();

    private Walker(final Map<Bytes32, Bytes> nodes, final byte[] origin, final byte[] rightBound) {
      this.nodes = nodes;
      this.origin = origin;
      this.rightBound = rightBound;
    }

    // visits a node that overlaps a boundary of the range
    private void visit(final Bytes ref, final Bytes location) {
      final Bytes rlp = resolve(ref);
      final RLPInput in = RLP.input(rlp);
      final int items = in.enterList();
      if (items == 17) {
        for (int i = 0; i < 16; i++) {
          final Bytes child = readRef(in);
          if (child != null) {
            final Bytes childLocation = Bytes.concatenate(location, Bytes.of(i));
            dispatch(child, childLocation, relation(childLocation), true);
          }
        }
        if (!in.readBytes().isEmpty()) {
          throw new IllegalArgumentException("branch value in a 32-byte key trie");
        }
      } else if (items == 2) {
        final Bytes path = decodePath(in.readBytes());
        final boolean leaf = path.size() > 0 && path.get(path.size() - 1) == 16;
        final Bytes full =
            Bytes.concatenate(location, leaf ? path.slice(0, path.size() - 1) : path);
        if (leaf) {
          if (full.size() != 64) {
            throw new IllegalArgumentException("leaf at the wrong depth");
          }
          in.readBytes();
          final Relation relation = relation(full);
          dispatch(ref, location, relation == Relation.OVERLAP ? Relation.INSIDE : relation, false);
        } else {
          if (path.isEmpty() || full.size() >= 64) {
            throw new IllegalArgumentException("invalid extension");
          }
          final Relation relation = relation(full);
          if (relation == Relation.OVERLAP) {
            final Bytes child = readRef(in);
            if (child == null) {
              throw new IllegalArgumentException("extension without child");
            }
            visit(child, full);
          } else {
            dispatch(ref, location, relation, false);
          }
        }
      } else {
        throw new IllegalArgumentException("invalid node");
      }
    }

    private void dispatch(
        final Bytes ref, final Bytes location, final Relation relation, final boolean descend) {
      switch (relation) {
        case LEFT -> left.add(new Sealed(location, ref));
        case RIGHT -> right.add(new Sealed(location, ref));
        case INSIDE -> {
          // rebuilt from the returned leaves
        }
        case OVERLAP -> {
          if (!descend) {
            throw new IllegalStateException("unexpected overlap");
          }
          visit(ref, location);
        }
      }
    }

    private Bytes resolve(final Bytes ref) {
      if (!NodeEncoding.isHashRef(ref)) {
        return ref;
      }
      final Bytes node = nodes.get(NodeEncoding.refHash(ref));
      if (node == null) {
        throw new IllegalArgumentException("missing proof node");
      }
      return node;
    }

    private Relation relation(final Bytes location) {
      final int toOrigin = compare(location, origin);
      if (toOrigin < 0) {
        return Relation.LEFT;
      }
      final int toRight = compare(location, rightBound);
      if (toRight > 0) {
        return Relation.RIGHT;
      }
      return toOrigin > 0 && toRight < 0 ? Relation.INSIDE : Relation.OVERLAP;
    }

    private static int compare(final Bytes location, final byte[] key) {
      for (int i = 0; i < location.size(); i++) {
        final int diff = (location.get(i) & 0x0f) - NodeEncoding.nibble(key, i);
        if (diff != 0) {
          return diff;
        }
      }
      return 0;
    }

    private static Bytes readRef(final RLPInput in) {
      if (in.nextIsList()) {
        return in.readAsRlp().raw();
      }
      final Bytes value = in.readBytes();
      if (value.isEmpty()) {
        return null;
      }
      if (value.size() != 32) {
        throw new IllegalArgumentException("invalid child reference");
      }
      return NodeEncoding.hashRef(Bytes32.wrap(value));
    }

    // decodes a hex-prefix path, appending the leaf terminator 16 for leaves
    private static Bytes decodePath(final Bytes encoded) {
      if (encoded.isEmpty()) {
        throw new IllegalArgumentException("empty path");
      }
      final int flag = (encoded.get(0) & 0xff) >> 4;
      if (flag > 3) {
        throw new IllegalArgumentException("invalid path flag");
      }
      final boolean odd = (flag & 1) == 1;
      final boolean leaf = flag >= 2;
      final int nibbles = (encoded.size() - 1) * 2 + (odd ? 1 : 0);
      final byte[] out = new byte[nibbles + (leaf ? 1 : 0)];
      int pos = 0;
      if (odd) {
        out[pos++] = (byte) (encoded.get(0) & 0x0f);
      } else if ((encoded.get(0) & 0x0f) != 0) {
        throw new IllegalArgumentException("invalid path padding");
      }
      for (int i = 1; i < encoded.size(); i++) {
        final int b = encoded.get(i) & 0xff;
        out[pos++] = (byte) (b >> 4);
        out[pos++] = (byte) (b & 0x0f);
      }
      if (leaf) {
        out[pos] = 16;
      }
      return Bytes.wrap(out);
    }
  }
}
