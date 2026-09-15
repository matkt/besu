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
package org.hyperledger.besu.ethereum.trie.immutabletree;

import org.hyperledger.besu.ethereum.trie.CommitVisitor;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeLoader;
import org.hyperledger.besu.ethereum.trie.NodeUpdater;
import org.hyperledger.besu.ethereum.trie.PathNodeVisitor;
import org.hyperledger.besu.ethereum.trie.Proof;
import org.hyperledger.besu.ethereum.trie.TrieIterator;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * {@link MerkleTrie} backed by {@link PersistentImmutableTreeCache}. Puts/removes are copy-on-write
 * on immutable nodes; {@link #getRootHash()} is the hash of the current immutable root.
 */
public final class ImmutableMerkleTrie implements MerkleTrie<Bytes, Bytes> {

  private final PersistentImmutableTreeCache cache;
  private final RootKind kind;
  private final ImmutableTreeOps.NodeResolver resolver;
  private ImmutableTreeNode root;

  public ImmutableMerkleTrie(
      final PersistentImmutableTreeCache cache,
      final Bytes32 rootHash,
      final RootKind kind,
      final NodeLoader diskLoader) {
    this.cache = cache;
    this.kind = kind;
    this.resolver =
        node -> {
          ImmutableTreeNode current = node;
          if (current.isStored()) {
            final StoredTreeNode stored = (StoredTreeNode) current;
            Optional<Bytes> rlp = cache.getNodeRlp(stored.location(), stored.hash());
            if (rlp.isEmpty()) {
              rlp = diskLoader.getNode(stored.location(), stored.hash());
              rlp.ifPresent(bytes -> cache.cacheNodeRlp(stored.location(), stored.hash(), bytes));
            }
            if (rlp.isEmpty()) {
              throw new MerkleTrieException(
                  "Unable to load trie node", stored.hash(), stored.location());
            }
            current = cache.resolve(new StoredTreeNode(stored.location(), stored.hash()));
            if (current.isStored()) {
              // Force materialize from the RLP we just cached.
              current =
                  TreeNodeDecoder.decode(stored.location(), stored.hash(), rlp.orElseThrow());
              cache.cacheNodeRlp(stored.location(), stored.hash(), rlp.orElseThrow());
            }
          } else {
            current = cache.resolve(current);
          }
          return current;
        };
    this.root = cache.treeForRoot(rootHash, kind).rootNode();
    if (!(this.root instanceof EmptyTreeNode)) {
      this.root = resolver.resolve(this.root);
    }
  }

  public ImmutableTreeNode immutableRoot() {
    return root;
  }

  public RootKind kind() {
    return kind;
  }

  @Override
  public Optional<Bytes> get(final Bytes key) {
    return ImmutableTreeOps.get(root, key, resolver);
  }

  @Override
  public Optional<Bytes> getPath(final Bytes path) {
    throw new UnsupportedOperationException("getPath not supported on ImmutableMerkleTrie");
  }

  @Override
  public Proof<Bytes> getValueWithProof(final Bytes key) {
    throw new UnsupportedOperationException("proofs not supported on ImmutableMerkleTrie");
  }

  @Override
  public void put(final Bytes key, final Bytes value) {
    root = ImmutableTreeOps.put(root, key, value, resolver);
  }

  @Override
  public void putPath(final Bytes path, final Bytes value) {
    throw new UnsupportedOperationException("putPath not supported on ImmutableMerkleTrie");
  }

  @Override
  public void put(final Bytes key, final PathNodeVisitor<Bytes> putVisitor) {
    throw new UnsupportedOperationException(
        "custom put visitor not supported on ImmutableMerkleTrie");
  }

  @Override
  public void remove(final Bytes key) {
    root = ImmutableTreeOps.remove(root, key, resolver);
  }

  @Override
  public void removePath(final Bytes path, final PathNodeVisitor<Bytes> removeVisitor) {
    throw new UnsupportedOperationException("removePath not supported on ImmutableMerkleTrie");
  }

  @Override
  public Bytes32 getRootHash() {
    return root.hash();
  }

  @Override
  public void commit(final NodeUpdater nodeUpdater) {
    commitNodes(root, Bytes.EMPTY, nodeUpdater);
    cache.registerRoot(root, kind, TreeRole.FORK);
  }

  @Override
  public void commit(final NodeUpdater nodeUpdater, final CommitVisitor<Bytes> commitVisitor) {
    commit(nodeUpdater);
  }

  private void commitNodes(
      final ImmutableTreeNode node, final Bytes location, final NodeUpdater updater) {
    final ImmutableTreeNode current = resolver.resolve(node);
    if (current instanceof EmptyTreeNode) {
      return;
    }
    if (!current.isStored()) {
      updater.store(location, current.hash(), current.rlp());
    }
    switch (current) {
      case ExtensionTreeNode ext ->
          commitNodes(ext.child(), Bytes.concatenate(location, ext.path()), updater);
      case BranchTreeNode branch -> {
        for (int i = 0; i < 16; i++) {
          final ImmutableTreeNode child = branch.child(i);
          if (!(child instanceof EmptyTreeNode)) {
            commitNodes(child, Bytes.concatenate(location, Bytes.of((byte) i)), updater);
          }
        }
      }
      default -> {
        // leaf
      }
    }
  }

  @Override
  public Map<Bytes32, Bytes> entriesFrom(final Bytes32 startKeyHash, final int limit) {
    final Map<Bytes32, Bytes> out = new LinkedHashMap<>();
    collectEntries(root, Bytes.EMPTY, startKeyHash, limit, out);
    return out;
  }

  private void collectEntries(
      final ImmutableTreeNode node,
      final Bytes pathSoFar,
      final Bytes32 startKeyHash,
      final int limit,
      final Map<Bytes32, Bytes> out) {
    if (out.size() >= limit) {
      return;
    }
    final ImmutableTreeNode current = resolver.resolve(node);
    switch (current) {
      case EmptyTreeNode ignored -> {}
      case LeafTreeNode leaf ->
          leaf.value()
              .ifPresent(
                  value -> {
                    final Optional<Bytes32> keyHash = keyHashFromPath(pathSoFar, leaf.path());
                    keyHash.ifPresent(
                        hash -> {
                          if (hash.compareTo(startKeyHash) >= 0) {
                            out.put(hash, value);
                          }
                        });
                  });
      case ExtensionTreeNode ext ->
          collectEntries(
              ext.child(), Bytes.concatenate(pathSoFar, ext.path()), startKeyHash, limit, out);
      case BranchTreeNode branch -> {
        branch
            .value()
            .ifPresent(
                value -> {
                  final Optional<Bytes32> keyHash =
                      keyHashFromPath(
                          pathSoFar,
                          Bytes.of(
                              org.hyperledger.besu.ethereum.trie.CompactEncoding.LEAF_TERMINATOR));
                  keyHash.ifPresent(
                      hash -> {
                        if (hash.compareTo(startKeyHash) >= 0) {
                          out.put(hash, value);
                        }
                      });
                });
        for (int i = 0; i < 16 && out.size() < limit; i++) {
          final ImmutableTreeNode child = branch.child(i);
          if (!(child instanceof EmptyTreeNode)) {
            collectEntries(
                child, Bytes.concatenate(pathSoFar, Bytes.of((byte) i)), startKeyHash, limit, out);
          }
        }
      }
      default -> {}
    }
  }

  private static Optional<Bytes32> keyHashFromPath(final Bytes prefix, final Bytes leafPath) {
    final Bytes fullPath = Bytes.concatenate(prefix, leafPath);
    try {
      return Optional.of(
          Bytes32.wrap(org.hyperledger.besu.ethereum.trie.CompactEncoding.pathToBytes(fullPath)));
    } catch (final RuntimeException e) {
      return Optional.empty();
    }
  }

  @Override
  public Map<Bytes32, Bytes> entriesFrom(final Function<Node<Bytes>, Map<Bytes32, Bytes>> handler) {
    throw new UnsupportedOperationException("custom entries collector not supported");
  }

  @Override
  public void visitAll(final Consumer<Node<Bytes>> nodeConsumer) {
    throw new UnsupportedOperationException("visitAll not supported on ImmutableMerkleTrie");
  }

  @Override
  public CompletableFuture<Void> visitAll(
      final Consumer<Node<Bytes>> nodeConsumer, final ExecutorService executorService) {
    throw new UnsupportedOperationException("visitAll not supported on ImmutableMerkleTrie");
  }

  @Override
  public void visitLeafs(final TrieIterator.LeafHandler<Bytes> handler) {
    throw new UnsupportedOperationException("visitLeafs not supported on ImmutableMerkleTrie");
  }
}
