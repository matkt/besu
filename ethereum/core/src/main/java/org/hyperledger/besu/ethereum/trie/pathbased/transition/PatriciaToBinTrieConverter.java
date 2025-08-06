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

  private static final Cache<Bytes, PreloadedWorldStateData> PRELOADED_TRANSITION_DATA =
      CacheBuilder.newBuilder().recordStats().maximumSize(2_000).build();

  public static void addPreImage(final Bytes hash, final Bytes key) {
    System.out.println("PREIMAGE " + key + " " + hash);
    // PRELOADED_TRANSITION_DATA.put(hash, key);
  }

  public static void preloadImage(
      final Hash blockHash,
      final BonsaiWorldState bonsaiWorldState,
      final StateMigrationLog migrationProgress) {
    CompletableFuture.runAsync(
        () -> {
          PRELOADED_TRANSITION_DATA.put(
              blockHash,
              generatePreloadedWorldStateData(blockHash, bonsaiWorldState, migrationProgress));
        });
  }

  private static PreloadedWorldStateData generatePreloadedWorldStateData(
      final Hash blockHash,
      final BonsaiWorldState bonsaiWorldState,
      final StateMigrationLog migrationProgress) {
    final PreloadedWorldStateData preloaded = new PreloadedWorldStateData();

    final AtomicInteger convertedEntriesCount = new AtomicInteger(0);

    final StateMigrationLog localLog =
        new StateMigrationLog(
            migrationProgress.getFirstBlockHash(),
            Optional.ofNullable(migrationProgress.getNextAccount()),
            Optional.ofNullable(migrationProgress.getNextStorageKey()),
            migrationProgress.getMaxToConvert());

    LOG.atInfo()
        .setMessage("Preloading preimages and data from Bonsai world state (block: {})...")
        .addArgument(blockHash)
        .log();

    bonsaiWorldState
        .getWorldStateStorage()
        .streamFlatAccounts(
            localLog.getNextAccountAndReset(),
            account -> {
              if (convertedEntriesCount.get() >= localLog.getMaxToConvert()) {
                return false;
              }

              final Hash accountHash = Hash.wrap(account.getFirst());
              final Bytes preimage;
              try {
                preimage = DebugPreImageClient.getPreImage(accountHash);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }

              final Address address = Address.wrap(preimage);
              final BonsaiAccount merkleAccount =
                  BonsaiAccount.fromRLP(bonsaiWorldState, address, account.getSecond(), false);

              preloaded.addPreimage(accountHash, preimage);
              preloaded.addAccount(address, merkleAccount);

              if (!merkleAccount.isStorageEmpty() && !localLog.isStorageAccountFullyMigrated()) {

                bonsaiWorldState
                    .getWorldStateStorage()
                    .streamFlatStorages(
                        accountHash,
                        localLog.getNextStorageKeyAndReset(),
                        storage -> {
                          if (convertedEntriesCount.get() >= localLog.getMaxToConvert()) {
                            localLog.setNextStorageKey(storage.getFirst());
                            return false;
                          }

                          final Hash slotHash = Hash.wrap(storage.getFirst());
                          final Bytes slotPreimage;
                          try {
                            slotPreimage = DebugPreImageClient.getPreImage(slotHash);
                          } catch (Exception e) {
                            throw new RuntimeException(e);
                          }

                          final StorageSlotKey slotKey =
                              new StorageSlotKey(
                                  slotHash, Optional.of(UInt256.fromBytes(slotPreimage)));

                          preloaded.addPreimage(slotHash, slotPreimage);
                          preloaded.addStorage(address, slotKey, storage.getSecond());

                          convertedEntriesCount.incrementAndGet();
                          return true;
                        });

                if (!localLog.hasNextStorage()) {
                  localLog.markStorageAccountFullyMigrated();
                }
              }

              if (convertedEntriesCount.get() < localLog.getMaxToConvert()) {
                localLog.clearNextStorageKey();
                if (merkleAccount.hasCode()) {
                  // Adjust conversion count based on the code chunkification process
                  convertedEntriesCount.addAndGet(
                      TrieKeyUtils.chunkifyCode(merkleAccount.getCode()).size());
                }
                convertedEntriesCount.incrementAndGet();
                return true;
              } else {
                localLog.setNextAccount(accountHash);
                return false;
              }
            });
    if (!localLog.hasNextAccount()) {
      localLog.markAccountsFullyMigrated();
      LOG.atInfo().setMessage("All accounts have been fully migrated.").log();
    }
    preloaded.setMigrationLog(localLog);

    LOG.atInfo()
        .setMessage("Preimage and data preloading completed for block {}. Total entries loaded: {}")
        .addArgument(blockHash)
        .addArgument(convertedEntriesCount.get())
        .log();
    return preloaded;
  }

  /**
   * Converts accounts and storage from a Bonsai (Patricia Merkle Trie) world state to a
   * BinTrie-based world state. The migration process respects a predefined limit.
   *
   * @param bonsaiWorldState The source Bonsai world state.
   * @param binTrieWorldState The target BinTrie world state.
   * @param migrationProgress Tracks migration progress to allow resumption.
   */
  public static void convert(
      final BonsaiWorldState bonsaiWorldState,
      final BinTrieWorldState binTrieWorldState,
      final StateMigrationLog migrationProgress) {

    final PreloadedWorldStateData preloadedWorldStateData;
    try {
      preloadedWorldStateData =
          PRELOADED_TRANSITION_DATA.get(
              binTrieWorldState.getWorldStateBlockHash(),
              () ->
                  generatePreloadedWorldStateData(
                      binTrieWorldState.getWorldStateBlockHash(),
                      bonsaiWorldState,
                      migrationProgress));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }

    final BinTrieWorldStateUpdateAccumulator binTrieUpdateAccumulator =
        binTrieWorldState.getAccumulator();

    LOG.atDebug().setMessage("Running migration from Bonsai to BinTrie...").log();

    preloadedWorldStateData
        .getAccounts()
        .forEach(
            (address, merkleAccount) -> {
              LOG.atInfo().setMessage("Migrating account: {}").addArgument(address).log();

              preloadedWorldStateData
                  .getStorage(address)
                  .forEach(
                      (storageSlotKey, value) -> {
                        LOG.atInfo()
                            .setMessage("Migrating storage for account: {}")
                            .addArgument(address)
                            .log();
                        final StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>>
                            storageMap =
                                binTrieUpdateAccumulator
                                    .getStorageToUpdate()
                                    .computeIfAbsent(
                                        merkleAccount.getAddress(),
                                        addr ->
                                            new StorageConsumingMap<>(
                                                addr, new ConcurrentHashMap<>(), (a, v) -> {}));

                        if (binTrieWorldState
                            .getStorageValueByStorageSlotKey(
                                merkleAccount.getAddress(), storageSlotKey)
                            .isEmpty()) {
                          LOG.atTrace()
                              .setMessage("Migrating storage slot: {}")
                              .addArgument(storageSlotKey)
                              .log();
                          storageMap.compute(
                              storageSlotKey,
                              (slotKey, existing) ->
                                  existing != null
                                      ? new PathBasedValue<>(
                                          null, existing.getUpdated(), existing.isLastStepCleared())
                                      : new PathBasedValue<>(
                                          null, UInt256.fromBytes(RLP.decodeValue(value))));
                        }
                      });

              migrateAccount(merkleAccount, binTrieWorldState, binTrieUpdateAccumulator);
            });
    migrationProgress.apply(preloadedWorldStateData.getMigrationLog());
  }

  private static void migrateAccount(
      final BonsaiAccount merkleAccount,
      final BinTrieWorldState binTrieWorldState,
      final BinTrieWorldStateUpdateAccumulator binTrieUpdateAccumulator) {

    LOG.atDebug()
        .setMessage("Processing account: {}")
        .addArgument(merkleAccount.getAddress())
        .log();

    final BinTrieAccount binTrieAccount =
        Optional.ofNullable((BinTrieAccount) binTrieWorldState.get(merkleAccount.getAddress()))
            .map(
                existingAccount -> {
                  binTrieUpdateAccumulator
                      .getAccountsToUpdate()
                      .putIfAbsent(
                          existingAccount.getAddress(),
                          new PathBasedValue<>(existingAccount, existingAccount));
                  return existingAccount;
                })
            .orElseGet(
                () -> {
                  BinTrieAccount toMigrateAccount =
                      new BinTrieAccount(
                          binTrieUpdateAccumulator,
                          merkleAccount.getAddress(),
                          merkleAccount.getAddressHash(),
                          merkleAccount.getNonce(),
                          merkleAccount.getBalance(),
                          merkleAccount.getCodeSize().orElse(0L),
                          new CodeV0(merkleAccount.getCode()),
                          merkleAccount.getCodeHash(),
                          false);

                  binTrieUpdateAccumulator
                      .getAccountsToUpdate()
                      .compute(
                          toMigrateAccount.getAddress(),
                          (address, existing) ->
                              existing != null
                                  ? new PathBasedValue<>(
                                      null, existing.getUpdated(), existing.isLastStepCleared())
                                  : new PathBasedValue<>(null, toMigrateAccount));
                  return toMigrateAccount;
                });

    if (binTrieAccount.hasCode()) {
      if (merkleAccount.getCodeHash().equals(binTrieAccount.getCodeHash())) {
        binTrieUpdateAccumulator
            .getCodeToUpdate()
            .putIfAbsent(
                binTrieAccount.getAddress(), new PathBasedValue<>(null, binTrieAccount.getCode()));
      }
    }
    LOG.atTrace().setMessage("Migrated account: {}").addArgument(merkleAccount.getAddress()).log();
  }
}
