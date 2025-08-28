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
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.stateless.bintrie.adapter.TrieKeyUtils;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.BinTrieAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.worldview.BinTrieWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bintrie.worldview.BinTrieWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.preload.StorageConsumingMap;
import org.hyperledger.besu.evm.code.CodeV0;
import org.hyperledger.besu.plugin.services.trielogs.StateMigrationLog;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.rlp.RLP;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts accounts and storage from a Patricia Merkle Trie (Bonsai) WorldState to a BinTrie-based
 * WorldState.
 */
public class PatriciaToBinTrieConverter {

  private static final Logger LOG = LoggerFactory.getLogger(PatriciaToBinTrieConverter.class);

  private static final Cache<Long, CompletableFuture<PreloadedWorldStateData>> preloadedStateCache =
      CacheBuilder.newBuilder().recordStats().maximumSize(2).build();

  public static void preloadStateData(
      final BonsaiWorldState sourceWorldState, final StateMigrationLog migrationLog) {
    preloadedStateCache
        .asMap()
        .computeIfAbsent(
            migrationLog.getLastMigratedBlockNumber(),
            h ->
                CompletableFuture.supplyAsync(
                    () -> generatePreloadedStateData(sourceWorldState, migrationLog)));
  }

  private static PreloadedWorldStateData generatePreloadedStateData(
      final BonsaiWorldState sourceWorldState, final StateMigrationLog migrationLog) {

    PreloadedWorldStateData preloadedData = new PreloadedWorldStateData();
    AtomicInteger entryCounter = new AtomicInteger(0);

    StateMigrationLog localLog =
        new StateMigrationLog(
            migrationLog.getFirstBlockHash(),
            Optional.of(migrationLog.getLastMigratedBlockNumber() + 1),
            Optional.ofNullable(migrationLog.getNextAccount()),
            Optional.ofNullable(migrationLog.getNextStorageKey()),
            migrationLog.getMaxToConvert());
    sourceWorldState
        .getWorldStateStorage()
        .streamFlatAccounts(
            localLog.getNextAccountAndReset(),
            accountEntry -> {
              Hash accountHash = Hash.wrap(accountEntry.getFirst());

              Bytes accountPreimage = fetchPreimage(accountHash);
              Address accountAddress = Address.wrap(accountPreimage);

              BonsaiAccount account =
                  BonsaiAccount.fromRLP(
                      sourceWorldState, accountAddress, accountEntry.getSecond(), false);

              preloadedData.addPreimage(accountHash, accountPreimage);
              preloadedData.addAccount(accountAddress, account);

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

                          Hash storageHash = Hash.wrap(storageEntry.getFirst());
                          Bytes storagePreimage = fetchPreimage(storageHash);

                          StorageSlotKey slotKey =
                              new StorageSlotKey(
                                  storageHash, Optional.of(UInt256.fromBytes(storagePreimage)));

                          preloadedData.addPreimage(storageHash, storagePreimage);
                          preloadedData.addStorage(
                              accountAddress, slotKey, storageEntry.getSecond());

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

    preloadedData.setMigrationLog(localLog);

    return preloadedData;
  }

  private static Bytes fetchPreimage(final Hash hash) {
    try {
      return DebugPreImageClient.getPreImage(hash);
    } catch (Exception e) {
      throw new RuntimeException("Error fetching preimage for hash: " + hash, e);
    }
  }

  public static void migrateState(
      final BonsaiWorldState sourceWorldState,
      final BinTrieWorldState targetWorldState,
      final StateMigrationLog migrationLog) {

    PreloadedWorldStateData data;
    try {
      data =
          preloadedStateCache
              .get(
                  migrationLog.getLastMigratedBlockNumber(),
                  () -> {
                    return CompletableFuture.supplyAsync(
                        () -> generatePreloadedStateData(sourceWorldState, migrationLog));
                  })
              .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    BinTrieWorldStateUpdateAccumulator accumulator = targetWorldState.getAccumulator();

    LOG.atDebug().setMessage("Starting migration from Bonsai to BinTrie...").log();

    data.getAccounts()
        .forEach(
            (address, account) -> {
              final Map<StorageSlotKey, Bytes> storage = data.getStorage(address);
              LOG.atTrace()
                  .setMessage("Migrating account: {}({} slots)...")
                  .addArgument(address)
                  .addArgument(storage.size())
                  .log();

              storage.forEach(
                  (slotKey, value) -> {
                    StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>> storageMap =
                        accumulator
                            .getStorageToUpdate()
                            .computeIfAbsent(
                                address,
                                addr ->
                                    new StorageConsumingMap<>(
                                        addr, new ConcurrentHashMap<>(), (a, v) -> {}));

                    if (targetWorldState
                        .getStorageValueByStorageSlotKey(address, slotKey)
                        .isEmpty()) {
                      LOG.atTrace()
                          .setMessage("Migrating storage slot: {}")
                          .addArgument(slotKey)
                          .log();

                      storageMap.compute(
                          slotKey,
                          (key, existing) ->
                              existing != null
                                  ? new PathBasedValue<>(
                                      null, existing.getUpdated(), existing.isLastStepCleared())
                                  : new PathBasedValue<>(
                                      null, UInt256.fromBytes(RLP.decodeValue(value))));
                    }
                  });

              migrateAccount(account, targetWorldState, accumulator);
            });

    migrationLog.apply(data.getMigrationLog());
  }

  private static void migrateAccount(
      final BonsaiAccount source,
      final BinTrieWorldState target,
      final BinTrieWorldStateUpdateAccumulator accumulator) {

    LOG.atTrace().setMessage("Migrating account object: {}").addArgument(source.getAddress()).log();

    BinTrieAccount migrated =
        Optional.ofNullable((BinTrieAccount) target.get(source.getAddress()))
            .map(
                existing -> {
                  accumulator
                      .getAccountsToUpdate()
                      .putIfAbsent(existing.getAddress(), new PathBasedValue<>(existing, existing));
                  return existing;
                })
            .orElseGet(
                () -> {
                  BinTrieAccount newAccount =
                      new BinTrieAccount(
                          accumulator,
                          source.getAddress(),
                          source.getAddressHash(),
                          source.getNonce(),
                          source.getBalance(),
                          source.getCodeSize().orElse(0L),
                          new CodeV0(source.getCode()),
                          source.getCodeHash(),
                          false);

                  accumulator
                      .getAccountsToUpdate()
                      .compute(
                          newAccount.getAddress(),
                          (addr, existing) ->
                              existing != null
                                  ? new PathBasedValue<>(
                                      null, existing.getUpdated(), existing.isLastStepCleared())
                                  : new PathBasedValue<>(null, newAccount));

                  LOG.atTrace()
                      .setMessage("Created new BinTrie account: {}")
                      .addArgument(newAccount.getAddress())
                      .log();
                  return newAccount;
                });

    if (migrated.hasCode() && source.getCodeHash().equals(migrated.getCodeHash())) {
      accumulator
          .getCodeToUpdate()
          .putIfAbsent(migrated.getAddress(), new PathBasedValue<>(null, migrated.getCode()));
    }
  }
}
