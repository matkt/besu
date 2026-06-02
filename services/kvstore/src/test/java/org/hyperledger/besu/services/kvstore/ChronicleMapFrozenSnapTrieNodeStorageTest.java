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
package org.hyperledger.besu.services.kvstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ChronicleMapFrozenSnapTrieNodeStorageTest {

  @TempDir Path tempDir;

  @Test
  void putGetAndFreeze() throws Exception {
    final Path dbPath = tempDir.resolve("frozen");
    final Bytes32 hash = Bytes32.random();
    final Bytes node = Bytes.fromHexString("0x820120");
    try (FrozenSnapTrieNodeStorage storage = FrozenSnapTrieNodeStorage.open(dbPath)) {
      storage.put(hash, node);
      assertThat(storage.get(hash)).contains(node);
      storage.freeze();
      assertThat(storage.isFrozen()).isTrue();
      assertThatThrownBy(() -> storage.put(hash, node))
          .isInstanceOf(IllegalStateException.class);
    }

    try (FrozenSnapTrieNodeStorage reopened = FrozenSnapTrieNodeStorage.open(dbPath)) {
      assertThat(reopened.isFrozen()).isTrue();
      assertThat(reopened.get(hash)).contains(node);
    }
    assertThat(Files.exists(dbPath.resolve("snap_trie_nodes.dat"))).isTrue();
  }

  @Test
  void releaseMappedMemoryAfterFlushReopensOnNextAccess() {
    final Bytes32 hash = Bytes32.random();
    final Bytes node = Bytes.fromHexString("0x820120");
    try (FrozenSnapTrieNodeStorage storage =
        FrozenSnapTrieNodeStorage.open(tempDir.resolve("unmap"))) {
      storage.put(hash, node);
      storage.releaseMappedMemoryAfterFlush();
      assertThat(storage.get(hash)).contains(node);
    }
  }

  @Test
  void putLargeBranchSizedNode() {
    final Bytes32 hash = Bytes32.random();
    final Bytes branchLikeNode = Bytes.repeat((byte) 0xab, 600);
    try (FrozenSnapTrieNodeStorage storage =
        FrozenSnapTrieNodeStorage.open(tempDir.resolve("large-branch"))) {
      storage.put(hash, branchLikeNode);
      assertThat(storage.get(hash)).contains(branchLikeNode);
    }
  }

  @Test
  void putAllBatch() {
    final Bytes32 hash1 = Bytes32.fromHexString(
        "0x1111111111111111111111111111111111111111111111111111111111111111");
    final Bytes32 hash2 = Bytes32.fromHexString(
        "0x2222222222222222222222222222222222222222222222222222222222222222");
    try (FrozenSnapTrieNodeStorage storage =
        FrozenSnapTrieNodeStorage.open(tempDir.resolve("batch"))) {
      storage.putAll(
          Map.of(hash1, Bytes.fromHexString("0x01"), hash2, Bytes.fromHexString("0x02")));
      assertThat(storage.get(hash1)).isPresent();
      assertThat(storage.get(hash2)).isPresent();
    }
  }
}
