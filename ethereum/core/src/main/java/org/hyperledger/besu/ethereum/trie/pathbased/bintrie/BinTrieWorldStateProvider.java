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

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.cache.BinTrieCachedWorldStorageManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage.BinTrieWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.worldview.BinTrieWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.Optional;

public class BinTrieWorldStateProvider extends PathBasedWorldStateProvider {

  public BinTrieWorldStateProvider(
      final BinTrieWorldStateKeyValueStorage worldStateKeyValueStorage,
      final Blockchain blockchain,
      final Optional<Long> maxLayersToLoad,
      final ServiceManager pluginContext,
      final EvmConfiguration evmConfiguration,
      final CodeCache codeCache) {
    super(
        DataStorageFormat.BINTRIE,
        worldStateKeyValueStorage,
        blockchain,
        maxLayersToLoad,
        pluginContext);
    provideCachedWorldStorageManager(
        new BinTrieCachedWorldStorageManager(
            this, worldStateKeyValueStorage, worldStateConfig, codeCache));
    loadHeadWorldState(
        new BinTrieWorldState(
            this, worldStateKeyValueStorage, evmConfiguration, worldStateConfig, codeCache));
  }
}
