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

import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
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

  private static final Cache<Bytes, Bytes> PRE_IMAGES =
      CacheBuilder.newBuilder().recordStats().maximumSize(2_000).build();

  public static void addPreImage(final Bytes hash, final Bytes key) {
    System.out.println("PREIMAGE " + key + " " + hash);
    PRE_IMAGES.put(hash, key);
  }

  public static void preloadImage(
      final BonsaiWorldState bonsaiWorldState, final StateMigrationLog migrationProgress) {
    CompletableFuture.runAsync(
        () -> {
          final AtomicInteger convertedEntriesCount = new AtomicInteger(0);

          final StateMigrationLog localLog =
              new StateMigrationLog(
                  migrationProgress.getFirstBlockHash(),
                  Optional.ofNullable(migrationProgress.getNextAccount()),
                  Optional.ofNullable(migrationProgress.getNextStorageKey()),
                  migrationProgress.getMaxToConvert());

          LOG.atInfo()
              .setMessage("Preloading preimages from Bonsai world state (without migration)...")
              .log();

          bonsaiWorldState
              .getWorldStateStorage()
              .streamFlatAccounts(
                  localLog.getNextAccountAndReset(),
                  account -> {
                    final Hash accountHash = Hash.wrap(account.getFirst());

                    if (convertedEntriesCount.get() >= localLog.getMaxToConvert()) {
                      return false;
                    }

                    final Address address = Address.wrap(getPreimage(accountHash));

                    final BonsaiAccount merkleAccount =
                        BonsaiAccount.fromRLP(
                            bonsaiWorldState, address, account.getSecond(), false);

                    convertedEntriesCount.incrementAndGet();

                    if (!merkleAccount.isStorageEmpty()
                        && !localLog.isStorageAccountFullyMigrated()) {

                      final NavigableMap<Bytes32, Bytes> storages =
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
                                    convertedEntriesCount.incrementAndGet();
                                    return true;
                                  });

                      storages.entrySet().parallelStream()
                          .forEach(
                              entry -> {
                                final Hash slotHash = Hash.wrap(entry.getKey());
                                getPreimage(slotHash);
                              });

                      if (!localLog.hasNextStorage()) {
                        localLog.markStorageAccountFullyMigrated();
                      }
                    }

                    return convertedEntriesCount.get() < localLog.getMaxToConvert();
                  });

          LOG.atInfo()
              .setMessage("Preimage preloading completed. Total entries loaded: {}")
              .addArgument(convertedEntriesCount.get())
              .log();
        });
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

    final BinTrieWorldStateUpdateAccumulator binTrieUpdateAccumulator =
        binTrieWorldState.getAccumulator();
    final AtomicInteger convertedEntriesCount = new AtomicInteger(0);

    LOG.atDebug().setMessage("Running migration from Bonsai to BinTrie...").log();

    bonsaiWorldState
        .getWorldStateStorage()
        .streamFlatAccounts(
            migrationProgress.getNextAccountAndReset(),
            account -> {
              final Hash accountHash = Hash.wrap(account.getFirst());
              final Address address;

              address = Address.wrap(getPreimage(accountHash));

              final BonsaiAccount merkleAccount =
                  BonsaiAccount.fromRLP(bonsaiWorldState, address, account.getSecond(), false);
              LOG.atTrace().setMessage("Migrating account: {}").addArgument(address).log();
              if (!merkleAccount.isStorageEmpty()
                  && !migrationProgress.isStorageAccountFullyMigrated()) {
                migrateStorage(
                    bonsaiWorldState,
                    binTrieWorldState,
                    binTrieUpdateAccumulator,
                    merkleAccount,
                    migrationProgress,
                    convertedEntriesCount,
                    accountHash);
              }

              return migrateAccount(
                  merkleAccount,
                  binTrieWorldState,
                  binTrieUpdateAccumulator,
                  migrationProgress,
                  convertedEntriesCount,
                  accountHash);
            });

    if (!migrationProgress.hasNextAccount()) {
      migrationProgress.markAccountsFullyMigrated();
      LOG.atInfo().setMessage("All accounts have been fully migrated.").log();
    }
  }

  /**
   * Migrates the storage slots of an account from Bonsai to BinTrie. This method ensures storage
   * migration does not exceed the predefined conversion limit.
   *
   * @param bonsaiWorldState The source Bonsai world state.
   * @param binTrieWorldState The target BinTrie world state.
   * @param binTrieUpdateAccumulator Accumulator for BinTrie state updates.
   * @param merkleAccount The Bonsai account being migrated.
   * @param migrationProgress Progress tracker for storage migration.
   * @param convertedEntriesCount Counter for converted storage entries.
   * @param accountHash The hash of the account being migrated.
   */
  private static void migrateStorage(
      final BonsaiWorldState bonsaiWorldState,
      final BinTrieWorldState binTrieWorldState,
      final BinTrieWorldStateUpdateAccumulator binTrieUpdateAccumulator,
      final BonsaiAccount merkleAccount,
      final StateMigrationLog migrationProgress,
      final AtomicInteger convertedEntriesCount,
      final Hash accountHash) {

    LOG.atTrace()
        .setMessage("Migrating storage for account: {}")
        .addArgument(merkleAccount.getAddress())
        .log();

    final NavigableMap<Bytes32, Bytes> storages =
        bonsaiWorldState
            .getWorldStateStorage()
            .streamFlatStorages(
                accountHash,
                migrationProgress.getNextStorageKeyAndReset(),
                storage -> {
                  if (convertedEntriesCount.get() >= migrationProgress.getMaxToConvert()) {
                    migrationProgress.setNextStorageKey(storage.getFirst());
                    return false;
                  }
                  convertedEntriesCount.incrementAndGet();
                  return true;
                });

    if (!migrationProgress.hasNextStorage()) {
      migrationProgress.markStorageAccountFullyMigrated();
      LOG.atInfo()
          .setMessage("Storage migration completed for account: {}")
          .addArgument(merkleAccount.getAddress())
          .log();
    } else {
      LOG.atInfo()
          .setMessage("Storage migration not completed for account: {} ({})")
          .addArgument(merkleAccount.getAddress())
          .addArgument(migrationProgress.getNextStorageKey())
          .log();
    }

    final StorageConsumingMap<StorageSlotKey, PathBasedValue<UInt256>> storageMap =
        binTrieUpdateAccumulator
            .getStorageToUpdate()
            .computeIfAbsent(
                merkleAccount.getAddress(),
                addr -> new StorageConsumingMap<>(addr, new ConcurrentHashMap<>(), (a, v) -> {}));

    storages.entrySet().parallelStream()
        .forEach(
            (entry) -> {
              final Hash slotHash = Hash.wrap(entry.getKey());
              final StorageSlotKey storageSlotKey;

              storageSlotKey =
                  new StorageSlotKey(
                      slotHash, Optional.of(UInt256.fromBytes(getPreimage(slotHash))));

              if (binTrieWorldState
                  .getStorageValueByStorageSlotKey(merkleAccount.getAddress(), storageSlotKey)
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
                                null, UInt256.fromBytes(RLP.decodeValue(entry.getValue()))));
              }
            });
  }

  /**
   * Migrates an individual account from Bonsai to BinTrie. If the migration limit is reached, the
   * migration is paused and the account is marked for resumption.
   *
   * @param merkleAccount The Bonsai account being migrated.
   * @param binTrieWorldState The target BinTrie world state.
   * @param binTrieUpdateAccumulator Accumulator for BinTrie updates.
   * @param migrationProgress Migration progress tracker.
   * @param convertedEntriesCount Counter for converted accounts.
   * @param accountHash Hash of the account being processed.
   * @return true if migration can continue, false if the limit is reached.
   */
  private static boolean migrateAccount(
      final BonsaiAccount merkleAccount,
      final BinTrieWorldState binTrieWorldState,
      final BinTrieWorldStateUpdateAccumulator binTrieUpdateAccumulator,
      final StateMigrationLog migrationProgress,
      final AtomicInteger convertedEntriesCount,
      final Hash accountHash) {

    if (convertedEntriesCount.get() < migrationProgress.getMaxToConvert()) {
      migrationProgress.clearNextStorageKey();

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
                  binTrieAccount.getAddress(),
                  new PathBasedValue<>(null, binTrieAccount.getCode()));
        }
        // Adjust conversion count based on the code chunkification process
        convertedEntriesCount.addAndGet(TrieKeyUtils.chunkifyCode(binTrieAccount.getCode()).size());
      }

      convertedEntriesCount.incrementAndGet();
      LOG.atTrace()
          .setMessage("Migrated account: {}")
          .addArgument(merkleAccount.getAddress())
          .log();

      return true;
    }

    migrationProgress.setNextAccount(accountHash);

    LOG.atInfo()
        .setMessage("Reached migration limit, pausing at account: {}")
        .addArgument(accountHash)
        .log();

    return false;
  }

  private static Bytes getPreimage(final Hash hash) {
    try {
      return PRE_IMAGES.get(
          hash,
          () -> {
            try {
              return DebugPreImageClient.getPreImage(hash);
            } catch (Exception e) {
              LOG.atError()
                  .setMessage("Error retrieving preimage for hash: {}")
                  .addArgument(hash)
                  .log();
              throw new RuntimeException(e);
            }
          });
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
