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
package org.hyperledger.besu.ethereum.trie.pathbased.transition;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.stateless.bintrie.adapter.TrieKeyUtils;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.plugin.services.trielogs.StateMigrationLog;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreImageDownloaderV2 {
  private static final Logger LOG = LoggerFactory.getLogger(PreImageDownloaderV2.class);

  private static final Bytes NEXT_ACCOUNT_PRE_IMAGE =
      Bytes.wrap("nextAccount".getBytes(StandardCharsets.UTF_8));

  private static final Bytes NEXT_SLOT_PRE_IMAGE =
      Bytes.wrap("nextSlot".getBytes(StandardCharsets.UTF_8));

  public static CompletableFuture<Void> generatePreloadedStateData(
      final BonsaiWorldState sourceWorldState, final int maxToConvert) {
    return CompletableFuture.runAsync(
        () -> {
          AtomicInteger entryCounter = new AtomicInteger(0);

          final BonsaiWorldStateKeyValueStorage worldStateStorage =
              sourceWorldState.getWorldStateStorage();

          StateMigrationLog localLog =
              new StateMigrationLog(
                  Hash.ZERO, // don't care during pre image download
                  Optional.empty(), // don't care during pre image download
                  worldStateStorage.getPreImage(NEXT_ACCOUNT_PRE_IMAGE),
                  worldStateStorage.getPreImage(NEXT_SLOT_PRE_IMAGE),
                  maxToConvert);

          final BonsaiWorldStateKeyValueStorage.Updater updater = worldStateStorage.updater();

          sourceWorldState
              .getWorldStateStorage()
              .streamFlatAccounts(
                  localLog.getNextAccountAndReset(),
                  accountEntry -> {
                    final Hash accountHash = Hash.wrap(accountEntry.getFirst());

                    final Address accountPreimage = Address.wrap(fetchPreimage(accountHash));

                    BonsaiAccount account =
                        BonsaiAccount.fromRLP(
                            sourceWorldState, accountPreimage, accountEntry.getSecond(), false);

                    updater.addPreImage(accountHash, accountPreimage);

                    if (!account.isStorageEmpty() && !localLog.isStorageAccountFullyMigrated()) {
                      sourceWorldState
                          .getWorldStateStorage()
                          .streamFlatStorages(
                              accountHash,
                              localLog.getNextStorageKeyAndReset(),
                              storageEntry -> {
                                if (entryCounter.get() >= localLog.getMaxToConvert()) {
                                  localLog.setNextStorageKey(storageEntry.getFirst());
                                  return false;
                                }

                                final Hash storageHash = Hash.wrap(storageEntry.getFirst());
                                final Bytes storagePreimage = fetchPreimage(storageHash);

                                updater.addPreImage(storageHash, storagePreimage);

                                entryCounter.incrementAndGet();
                                return true;
                              });

                      if (!localLog.hasNextStorage()) {
                        localLog.markStorageAccountFullyMigrated();
                      }
                    }

                    if (entryCounter.get() < localLog.getMaxToConvert()) {
                      localLog.clearNextStorageKey();
                      if (account.hasCode()) {
                        entryCounter.addAndGet(TrieKeyUtils.chunkifyCode(account.getCode()).size());
                      }
                      entryCounter.incrementAndGet();
                      return true;
                    } else {
                      localLog.setNextAccount(accountHash);
                      return false;
                    }
                  });

          if (!localLog.hasNextAccount()) {
            localLog.markAccountsFullyMigrated();
            LOG.atInfo().setMessage("All accounts have been fully migrated.").log();
          } else {
            LOG.atInfo()
                .setMessage("Reached migration limit, pausing at account: {}")
                .addArgument(localLog.getNextAccount())
                .log();
          }
          updater.addPreImage(NEXT_ACCOUNT_PRE_IMAGE, localLog.getNextAccount());
          updater.addPreImage(NEXT_SLOT_PRE_IMAGE, localLog.getNextStorageKey());
          updater.commit();
        });
  }

  private static Bytes fetchPreimage(final Hash hash) {
    try {
      return DebugPreImageClient.getPreImage(hash);
    } catch (Exception e) {
      throw new RuntimeException("Error fetching preimage for hash: " + hash, e);
    }
  }
}
