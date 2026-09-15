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
package org.hyperledger.besu.ethereum.trie.pathbased.common.storage.cache.headmapdb;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class MapDbHeadStateCacheManagerTest {

  private MapDbHeadStateCacheManager manager;

  @AfterEach
  void tearDown() {
    if (manager != null) {
      manager.close();
    }
  }

  @Test
  void headReadUsesMapDbThenWriteBackFromKeyValueStorage() {
    manager = new MapDbHeadStateCacheManager(new NoOpMetricsSystem());
    manager.performForkChoiceUpdate(Hash.hash(Bytes.of(1)), 100);

    final Bytes accountKey = Bytes.of(0xab);
    assertThat(
            manager.readFlatAccount(
                HeadStateCacheAccessPolicy.CANONICAL_HEAD,
                accountKey,
                () -> Optional.of(Bytes.of(0x01))))
        .contains(Bytes.of(0x01));

    assertThat(
            manager.readFlatAccount(
                HeadStateCacheAccessPolicy.CANONICAL_HEAD, accountKey, Optional::empty))
        .contains(Bytes.of(0x01));
  }

  @Test
  void nonHeadPolicyBypassesMapDbEvenWhenCached() {
    manager = new MapDbHeadStateCacheManager(new NoOpMetricsSystem());
    manager.performForkChoiceUpdate(Hash.hash(Bytes.of(1)), 100);
    manager.preloadEntry(HeadMapDbCacheCategory.ACCOUNT_FLAT, Bytes.of(0x01), Bytes.of(0x02));

    assertThat(
            manager.readFlatAccount(
                HeadStateCacheAccessPolicy.KEY_VALUE_STORAGE_ONLY,
                Bytes.of(0x01),
                () -> Optional.of(Bytes.of(0x03))))
        .contains(Bytes.of(0x03));
  }

  @Test
  void trieParentResolvedFromKeyValueStorageSkipsMapDbForChild() {
    manager = new MapDbHeadStateCacheManager(new NoOpMetricsSystem());
    manager.performForkChoiceUpdate(Hash.hash(Bytes.of(2)), 50);

    final Bytes parentKey = Bytes.of(0x10);
    final Bytes childKey = Bytes.of(0x11);
    manager.writeHeadEntry(HeadMapDbCacheCategory.ACCOUNT_TRIE, parentKey, Bytes.of(0x20));

    assertThat(
            manager.readTrieNode(
                HeadMapDbCacheCategory.ACCOUNT_TRIE,
                childKey,
                Optional.of(ResolutionSource.RESOLVED_FROM_KEY_VALUE_STORAGE),
                HeadStateCacheAccessPolicy.CANONICAL_HEAD,
                () -> Optional.of(Bytes.of(0x99))))
        .contains(Bytes.of(0x99));
  }

  @Test
  void fcuReplacesSnapshotWithoutServingStaleHeadAssociation() {
    manager = new MapDbHeadStateCacheManager(new NoOpMetricsSystem());
    manager.performForkChoiceUpdate(Hash.hash(Bytes.of(1)), 10);
    manager.preloadEntry(HeadMapDbCacheCategory.ACCOUNT_FLAT, Bytes.of(0x01), Bytes.of(0x0a));

    manager.performForkChoiceUpdate(Hash.hash(Bytes.of(2)), 11);
    manager.writeHeadEntry(HeadMapDbCacheCategory.ACCOUNT_FLAT, Bytes.of(0x02), Bytes.of(0x0b));

    assertThat(
            manager.readFlatAccount(
                HeadStateCacheAccessPolicy.CANONICAL_HEAD, Bytes.of(0x02), Optional::empty))
        .contains(Bytes.of(0x0b));
  }
}
