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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PersistentImmutableTreeCacheTest {

  private Map<Bytes32, Bytes> disk;
  private PersistentImmutableTreeCache cache;

  @BeforeEach
  void setUp() {
    disk = new ConcurrentHashMap<>();
    cache =
        new PersistentImmutableTreeCache(
            (location, hash) -> Optional.ofNullable(disk.get(hash)), 2);
    cache.advanceBlock(10);
  }

  @Test
  void emptyRootIsStable() {
    assertThat(cache.head().rootHash()).isEqualTo(EmptyTreeNode.INSTANCE.hash());
    assertThat(cache.get(cache.head(), Bytes.fromHexString("0x01"))).isEmpty();
  }

  @Test
  void putIsCopyOnWrite_oldRootUnchanged() {
    final TreeRootHandle root0 = cache.head();
    final Bytes key = Bytes.fromHexString("0xabcd");
    final Bytes valueA = Bytes.fromHexString("0x11");
    final Bytes valueB = Bytes.fromHexString("0x22");

    final TreeRootHandle root1 = cache.put(root0, key, valueA, true);
    final TreeRootHandle root2 = cache.put(root1, key, valueB, true);

    assertThat(root0.rootHash()).isNotEqualTo(root1.rootHash());
    assertThat(root1.rootHash()).isNotEqualTo(root2.rootHash());

    assertThat(cache.get(root0, key)).isEmpty();
    assertThat(cache.get(root1, key)).contains(valueA);
    assertThat(cache.get(root2, key)).contains(valueB);
  }

  @Test
  void parallelRootsRemainIndependent() {
    final Bytes key = Bytes.fromHexString("0x10");
    final TreeRootHandle branchA =
        cache.put(cache.head(), key, Bytes.fromHexString("0xaa"), true);
    final TreeRootHandle branchB =
        cache.put(cache.head(), key, Bytes.fromHexString("0xbb"), true);

    assertThat(branchA.rootHash()).isNotEqualTo(branchB.rootHash());
    assertThat(cache.get(branchA, key)).contains(Bytes.fromHexString("0xaa"));
    assertThat(cache.get(branchB, key)).contains(Bytes.fromHexString("0xbb"));
    assertThat(cache.treeForRoot(branchA.rootHash(), RootKind.STATE).rootHash())
        .isEqualTo(branchA.rootHash());
    assertThat(cache.treeForRoot(branchB.rootHash(), RootKind.STATE).rootHash())
        .isEqualTo(branchB.rootHash());
  }

  @Test
  void headAndNewPayloadAreTrackedSeparately() {
    final Bytes key = Bytes.fromHexString("0x01");
    final TreeRootHandle headUpdate =
        cache.put(cache.head(), key, Bytes.fromHexString("0x01"), false);
    cache.setHead(headUpdate.rootHash(), RootKind.STATE);

    final TreeRootHandle payloadUpdate =
        cache.put(cache.head(), key, Bytes.fromHexString("0x02"), false);
    // Force role as new payload by registering explicitly
    final TreeRootHandle payload =
        cache.registerRoot(payloadUpdate.rootNode(), RootKind.STATE, TreeRole.NEW_PAYLOAD);
    cache.setNewPayload(payload.rootHash(), RootKind.STATE);

    assertThat(cache.head().rootHash()).isEqualTo(headUpdate.rootHash());
    assertThat(cache.newPayload().rootHash()).isEqualTo(payload.rootHash());
    assertThat(cache.get(cache.head(), key)).contains(Bytes.fromHexString("0x01"));
    assertThat(cache.get(cache.newPayload(), key)).contains(Bytes.fromHexString("0x02"));
  }

  @Test
  void missingNodeIsLoadedFromDiskAndCached() {
    final Bytes key = Bytes.fromHexString("0xabcd");
    final Bytes value = Bytes.fromHexString("0xdeadbeef");
    final TreeRootHandle written = cache.put(cache.head(), key, value, true);

    // Persist root RLP, drop in-memory nodes, reload via stored placeholder.
    disk.put(written.rootHash(), written.rootNode().rlp());
    final PersistentImmutableTreeCache cold =
        new PersistentImmutableTreeCache(
            (location, hash) -> Optional.ofNullable(disk.get(hash)), 2);
    cold.advanceBlock(10);

    final TreeRootHandle reloaded = cold.treeForRoot(written.rootHash(), RootKind.STATE);
    assertThat(cold.get(reloaded, key)).contains(value);
    assertThat(cold.cachedNodeCount()).isGreaterThan(0);
  }

  @Test
  void lockedNodesAreNotPruned() {
    final Bytes key = Bytes.fromHexString("0x22");
    final TreeRootHandle root =
        cache.put(cache.head(), key, Bytes.fromHexString("0x99"), true);
    cache.advanceBlock(20);

    try (TreeTraversalLock lock = cache.beginTraversal(root)) {
      assertThat(root.isInUse()).isTrue();
      assertThat(cache.prune()).isZero();
      assertThat(cache.get(root, key)).contains(Bytes.fromHexString("0x99"));
    }
  }

  @Test
  void staleUnlockedNodesCanBePruned() {
    final TreeRootHandle root =
        cache.put(cache.head(), Bytes.fromHexString("0x01"), Bytes.fromHexString("0x01"), true);
    final int before = cache.cachedNodeCount();
    assertThat(before).isGreaterThan(0);

    // Age out access, keep root pinned via head so only intermediate stale nodes may go;
    // advance far enough and clear pin by moving head to empty after dropping root from use.
    cache.setHead(EmptyTreeNode.INSTANCE.hash(), RootKind.STATE);
    cache.setNewPayload(EmptyTreeNode.INSTANCE.hash(), RootKind.STATE);
    cache.advanceBlock(100);

    final int pruned = cache.prune();
    assertThat(pruned).isGreaterThan(0);
    assertThat(cache.cachedNodeCount()).isLessThan(before);
    assertThat(cache.findRoot(root.rootHash(), RootKind.STATE)).isEmpty();
  }

  @Test
  void structuralSharing_unchangedBranchIdentityPreserved() {
    final Bytes key1 = Bytes.fromHexString("0x00aa");
    final Bytes key2 = Bytes.fromHexString("0x00bb");
    final TreeRootHandle r1 =
        cache.put(cache.head(), key1, Bytes.fromHexString("0x01"), true);
    final TreeRootHandle r2 =
        cache.put(r1, key2, Bytes.fromHexString("0x02"), true);

    // Reading key1 on both roots yields same value; old root unaffected by second put.
    assertThat(cache.get(r1, key1)).contains(Bytes.fromHexString("0x01"));
    assertThat(cache.get(r1, key2)).isEmpty();
    assertThat(cache.get(r2, key1)).contains(Bytes.fromHexString("0x01"));
    assertThat(cache.get(r2, key2)).contains(Bytes.fromHexString("0x02"));
  }

  @Test
  void storedNodeRlpAccessRequiresLoad() {
    final StoredTreeNode stored =
        new StoredTreeNode(Bytes.EMPTY, Bytes32.fromHexString("0x" + "11".repeat(32)));
    assertThat(stored.isStored()).isTrue();
    assertThatThrownBy(stored::rlp).isInstanceOf(IllegalStateException.class);
  }
}
