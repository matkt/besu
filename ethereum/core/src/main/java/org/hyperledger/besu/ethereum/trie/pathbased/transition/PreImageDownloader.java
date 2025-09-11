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
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreImageDownloader {

  private static final Logger LOG = LoggerFactory.getLogger(PreImageDownloader.class);

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  private static final Bytes NEXT_ACCOUNT_PRE_IMAGE =
      Bytes.wrap("nextAccount".getBytes(StandardCharsets.UTF_8));

  public PreImageDownloader() {}

  public CompletableFuture<Void> downloadLoop(
      final BonsaiWorldState sourceWorldState, final int maxAccount, final long delayInMillis) {

    final BonsaiWorldStateKeyValueStorage worldStateStorage =
        sourceWorldState.getWorldStateStorage();
    final Optional<Bytes32> nextAccount =
        worldStateStorage.getPreImage(NEXT_ACCOUNT_PRE_IMAGE).map(Bytes32::wrap);
    if (nextAccount.isEmpty() && worldStateStorage.getPreImage().stream().findFirst().isPresent()) {
      return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    download(sourceWorldState, nextAccount.orElse(Bytes32.ZERO), maxAccount)
        .thenRunAsync(
            () -> {
              scheduler.schedule(
                  () -> {
                    downloadLoop(sourceWorldState, maxAccount, delayInMillis);
                  },
                  delayInMillis,
                  TimeUnit.MILLISECONDS);
            })
        .exceptionally(
            ex -> {
              LOG.atError()
                  .setMessage("Error during download process {}")
                  .addArgument(ex.getMessage())
                  .log();
              future.completeExceptionally(ex);
              return null;
            });

    return future;
  }

  public CompletableFuture<Void> download(
      final BonsaiWorldState sourceWorldState, final Bytes32 nextAccount, final int maxAccount) {

    return CompletableFuture.runAsync(
        () -> {
          AtomicInteger index = new AtomicInteger(0);

          final BonsaiWorldStateKeyValueStorage worldStateStorage =
              sourceWorldState.getWorldStateStorage();
          final BonsaiWorldStateKeyValueStorage.Updater updater = worldStateStorage.updater();

          sourceWorldState
              .getWorldStateStorage()
              .streamFlatAccounts(
                  nextAccount,
                  accountEntry -> {
                    if (index.incrementAndGet() > maxAccount) {
                      return false;
                    }

                    updater.addPreImage(NEXT_ACCOUNT_PRE_IMAGE, nextAccount);

                    LOG.atInfo()
                        .setMessage("Reached migration for account: {}")
                        .addArgument(accountEntry.getFirst())
                        .log();

                    Hash accountHash = Hash.wrap(accountEntry.getFirst());
                    Bytes accountPreimage = fetchPreimage(accountHash);
                    Address accountAddress = Address.wrap(accountPreimage);

                    updater.addPreImage(accountHash, accountAddress);

                    BonsaiAccount account =
                        BonsaiAccount.fromRLP(
                            sourceWorldState, accountAddress, accountEntry.getSecond(), false);

                    if (!account.isStorageEmpty()) {
                      sourceWorldState
                          .getWorldStateStorage()
                          .streamFlatStorages(
                              accountHash,
                              Hash.ZERO,
                              storageEntry -> {
                                Hash storageHash = Hash.wrap(storageEntry.getFirst());
                                Bytes storagePreimage = fetchPreimage(storageHash);
                                updater.addPreImage(storageHash, storagePreimage);
                                return true;
                              });
                    }
                    return true;
                  });
          if (index.get() == 0) {
            updater.removePreImage(NEXT_ACCOUNT_PRE_IMAGE);
          }
          updater.commit();
        });
  }

  public void stop() {
    scheduler.shutdown();
  }

  private static Bytes fetchPreimage(final Hash hash) {
    try {
      return DebugPreImageClient.getPreImage(hash);
    } catch (Exception e) {
      throw new RuntimeException("Error fetching preimage for hash: " + hash, e);
    }
  }
}
