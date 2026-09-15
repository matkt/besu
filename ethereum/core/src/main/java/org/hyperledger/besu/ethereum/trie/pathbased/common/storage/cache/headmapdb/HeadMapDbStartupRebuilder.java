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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedValue;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.BonsaiTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.plugin.data.BlockHeader;

import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Rebuilds the active head MapDB cache from the most recent trie logs at startup. */
public final class HeadMapDbStartupRebuilder {

  private static final Logger LOG = LoggerFactory.getLogger(HeadMapDbStartupRebuilder.class);

  private HeadMapDbStartupRebuilder() {}

  public static void rebuildHeadCacheFromTrieLogs(
      final MapDbHeadStateCacheManager headCacheManager,
      final BonsaiWorldStateKeyValueStorage storage,
      final Blockchain blockchain,
      final BlockHeader headHeader) {
    headCacheManager.performForkChoiceUpdate(headHeader.getBlockHash(), headHeader.getNumber());

    final long windowStart =
        Math.max(0, headHeader.getNumber() - MapDbHeadStateCacheManager.STARTUP_TRIE_LOG_WINDOW);
    int layersApplied = 0;
    for (long blockNumber = headHeader.getNumber(); blockNumber >= windowStart; blockNumber--) {
      final Optional<? extends BlockHeader> header = blockchain.getBlockHeader(blockNumber);
      if (header.isEmpty()) {
        continue;
      }
      final Optional<byte[]> trieLogBytes = storage.getTrieLog(header.get().getBlockHash());
      if (trieLogBytes.isEmpty()) {
        continue;
      }
      final TrieLogLayer layer =
          BonsaiTrieLogFactory.readFrom(
              new BytesValueRLPInput(Bytes.wrap(trieLogBytes.get()), false));
      preloadLayer(headCacheManager, storage, layer);
      layersApplied++;
    }
    LOG.info(
        "Head MapDB cache startup preload complete for block {} ({} trie log layers)",
        headHeader.getNumber(),
        layersApplied);
  }

  private static void preloadLayer(
      final MapDbHeadStateCacheManager headCacheManager,
      final BonsaiWorldStateKeyValueStorage storage,
      final TrieLogLayer layer) {
    for (final Map.Entry<Address, PathBasedValue<AccountValue>> accountEntry :
        layer.getAccountChanges().entrySet()) {
      final Hash accountHash = accountEntry.getKey().addressHash();
      storage
          .getAccountDirectFromKeyValueStorage(accountHash)
          .ifPresent(
              value ->
                  headCacheManager.preloadEntry(
                      HeadMapDbCacheCategory.ACCOUNT_FLAT, accountHash.getBytes(), value));
    }
    for (final Map.Entry<Address, Map<StorageSlotKey, PathBasedValue<UInt256>>> storageEntry :
        layer.getStorageChanges().entrySet()) {
      final Hash accountHash = storageEntry.getKey().addressHash();
      for (final StorageSlotKey slotKey : storageEntry.getValue().keySet()) {
        final Bytes storageKey =
            Bytes.concatenate(accountHash.getBytes(), slotKey.getSlotHash().getBytes());
        storage
            .getStorageValueDirectFromKeyValueStorage(accountHash, slotKey)
            .ifPresent(
                value ->
                    headCacheManager.preloadEntry(
                        HeadMapDbCacheCategory.STORAGE_FLAT, storageKey, value));
      }
    }
    for (final Address address : layer.getCodeChanges().keySet()) {
      layer
          .getCode(address)
          .filter(code -> !code.isEmpty())
          .ifPresent(
              code -> {
                final Hash accountHash = address.addressHash();
                final Hash codeHash = Hash.hash(code);
                storage
                    .getCodeDirectFromKeyValueStorage(codeHash, accountHash)
                    .ifPresent(
                        value ->
                            headCacheManager.preloadEntry(
                                HeadMapDbCacheCategory.CODE,
                                Bytes.concatenate(codeHash.getBytes(), accountHash.getBytes()),
                                value));
              });
    }
  }
}
