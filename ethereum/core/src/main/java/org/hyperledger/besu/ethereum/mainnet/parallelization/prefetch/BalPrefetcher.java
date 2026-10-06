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
package org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.parallelization.BlockProcessingExecutors;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads ahead the account and storage values a block access list touches, so that they are in the
 * cache when the block is executed.
 *
 * <p>The accounts are read by lots, each as soon as it is handed over: while the encoded list is
 * read (see {@link #prefetch(ProtocolContext, BlockHeader, Supplier, long)}), or as the decoded
 * list is split. A lot computes its own keys and reads them, so the first reads start right away
 * rather than once every key of the list is known.
 */
public class BalPrefetcher {

  private static final Logger LOG = LoggerFactory.getLogger(BalPrefetcher.class);

  private static final Comparator<byte[]> KEY_COMPARATOR = Arrays::compareUnsigned;
  private final boolean isSortingEnabled;
  private final int batchSize;

  /**
   * Creates a new prefetch mechanism.
   *
   * @param isSortingEnabled whether to sort the keys of a read (may improve DB locality)
   * @param batchSize the number of accounts of a lot and of keys of a read (0 or negative = all of
   *     them at once)
   */
  public BalPrefetcher(final boolean isSortingEnabled, final int batchSize) {
    this.isSortingEnabled = isSortingEnabled;
    this.batchSize = batchSize;
  }

  /**
   * Returns the prefetcher configured by {@code balConfiguration}, empty when BAL prefetch is
   * disabled or the block access list does not drive the parallel execution.
   *
   * @param balConfiguration the BAL configuration
   * @return the configured prefetcher, if enabled
   */
  public static Optional<BalPrefetcher> fromConfiguration(final BalConfiguration balConfiguration) {
    return balConfiguration.isPerfectParallelizationEnabled()
            && balConfiguration.isBalPreFetchReadingEnabled()
        ? Optional.of(
            new BalPrefetcher(
                balConfiguration.isBalPreFetchSortingEnabled(),
                balConfiguration.getBalPreFetchBatchSize()))
        : Optional.empty();
  }

  /**
   * Prefetches the state that an encoded block access list touches, as of {@code parentHeader},
   * while the list is read: each lot of accounts is read as soon as the list is read past it,
   * without waiting for the list to be decoded. Everything runs in the background, from getting the
   * encoding on.
   *
   * <p>The block access list is not validated: a malformed one stops the prefetch, and so does one
   * over {@code maxItems}, the EIP-7928 item budget of its block. Either belongs to an invalid
   * block.
   *
   * @param protocolContext the protocol context, for the world state archive
   * @param parentHeader the header of the parent of the block the access list belongs to
   * @param encodedBlockAccessList the RLP encoding of the block access list, called in the
   *     background (e.g. to decode it from hex)
   * @param maxItems the EIP-7928 item budget of the block: accounts plus storage keys
   * @return the prefetch, to cancel once its block is processed or rejected
   */
  public BalPrefetch prefetch(
      final ProtocolContext protocolContext,
      final BlockHeader parentHeader,
      final Supplier<Bytes> encodedBlockAccessList,
      final long maxItems) {
    final BalPrefetch prefetch = new BalPrefetch();
    final Executor executor = BlockProcessingExecutors.ioExecutor();
    CompletableFuture.runAsync(
            () ->
                openWorldState(protocolContext, parentHeader)
                    .ifPresent(
                        worldState ->
                            prefetchEncoded(
                                    worldState,
                                    encodedBlockAccessList,
                                    maxItems,
                                    Runnable::run,
                                    executor,
                                    prefetch)
                                .whenComplete((result, ex) -> worldState.close())),
            executor)
        .exceptionally(
            ex -> {
              LOG.error("Error during prefetch", ex);
              return null;
            });
    return prefetch;
  }

  /**
   * Prefetches the state that {@code blockAccessList} touches, as of {@code parentHeader}, on a
   * world state of its own that is closed once done.
   *
   * @param protocolContext the protocol context, for the world state archive
   * @param parentHeader the header of the parent of the block the access list belongs to
   * @param blockAccessList the block access list
   * @return the prefetch, to cancel once its block is processed or rejected
   */
  public BalPrefetch prefetch(
      final ProtocolContext protocolContext,
      final BlockHeader parentHeader,
      final BlockAccessList blockAccessList) {
    final BalPrefetch prefetch = new BalPrefetch();
    final Optional<BonsaiWorldState> maybeWorldState =
        openWorldState(protocolContext, parentHeader);
    if (maybeWorldState.isEmpty()) {
      return prefetch;
    }
    final BonsaiWorldState worldState = maybeWorldState.get();
    prefetch(
            worldState,
            blockAccessList,
            BlockProcessingExecutors.ioExecutor(),
            BlockProcessingExecutors.ioExecutor(),
            prefetch)
        .whenComplete((result, ex) -> worldState.close());
    return prefetch;
  }

  /**
   * Prefetch world state data based on the block access list.
   *
   * @param worldState the world state to prefetch data into
   * @param blockAccessList the block access list containing read operations
   * @param orchestrationExecutor the executor that splits the list into lots
   * @param fetchExecutor the executor that reads the lots
   * @return a completable future that completes when prefetching is done
   */
  public CompletableFuture<Void> prefetch(
      final BonsaiWorldState worldState,
      final BlockAccessList blockAccessList,
      final Executor orchestrationExecutor,
      final Executor fetchExecutor) {
    return prefetch(
        worldState, blockAccessList, orchestrationExecutor, fetchExecutor, new BalPrefetch());
  }

  CompletableFuture<Void> prefetch(
      final BonsaiWorldState worldState,
      final BlockAccessList blockAccessList,
      final Executor orchestrationExecutor,
      final Executor fetchExecutor,
      final BalPrefetch prefetch) {
    return readByLots(
        worldState,
        handOver -> {
          final List<BlockAccessList.AccountChanges> accounts = blockAccessList.accountChanges();
          final int lotSize = lotSize(accounts.size());
          for (int start = 0;
              start < accounts.size() && !prefetch.isCancelled();
              start += lotSize) {
            final List<BlockAccessList.AccountChanges> lot =
                accounts.subList(start, Math.min(start + lotSize, accounts.size()));
            handOver.accept(keys -> lot.forEach(keys::add));
          }
        },
        Long.MAX_VALUE,
        orchestrationExecutor,
        fetchExecutor,
        prefetch);
  }

  CompletableFuture<Void> prefetchEncoded(
      final BonsaiWorldState worldState,
      final Supplier<Bytes> encodedBlockAccessList,
      final long maxItems,
      final Executor orchestrationExecutor,
      final Executor fetchExecutor,
      final BalPrefetch prefetch) {
    return readByLots(
        worldState,
        handOver -> {
          final Bytes encoded = encodedBlockAccessList.get();
          prefetch.readsFrom(encoded);
          final RLPInput list = RLP.input(encoded);
          list.enterList();
          List<RLPInput> lot = new ArrayList<>();
          while (!list.isEndOfCurrentList() && !prefetch.isCancelled()) {
            // only delimited here: the lot reads its accounts itself
            lot.add(list.readAsRlp());
            if (lot.size() == batchSize) {
              final List<RLPInput> fullLot = lot;
              handOver.accept(keys -> fullLot.forEach(keys::add));
              lot = new ArrayList<>();
            }
          }
          if (!lot.isEmpty()) {
            final List<RLPInput> lastLot = lot;
            handOver.accept(keys -> lastLot.forEach(keys::add));
          }
        },
        maxItems,
        orchestrationExecutor,
        fetchExecutor,
        prefetch);
  }

  /** A lot of accounts of a block access list, which adds their keys to {@link LotKeys}. */
  @FunctionalInterface
  private interface Lot {
    void addKeysTo(LotKeys keys);
  }

  /** Splits a block access list into lots, handing each over as soon as it is delimited. */
  @FunctionalInterface
  private interface Splitter {
    void split(Consumer<Lot> handOver);
  }

  /**
   * Reads each lot {@code splitter} hands over on {@code fetchExecutor}, as soon as it is handed
   * over. Stops, and cancels {@code prefetch}, once the block access list turns out malformed or
   * over {@code maxItems}.
   */
  private CompletableFuture<Void> readByLots(
      final BonsaiWorldState worldState,
      final Splitter splitter,
      final long maxItems,
      final Executor orchestrationExecutor,
      final Executor fetchExecutor,
      final BalPrefetch prefetch) {
    final AtomicLong items = new AtomicLong();
    final AtomicLong accounts = new AtomicLong();
    final AtomicLong storageSlots = new AtomicLong();
    return CompletableFuture.supplyAsync(
            () -> {
              worldState.disableCacheMerkleTrieLoader();
              final List<CompletableFuture<Void>> lots = new ArrayList<>();
              try {
                splitter.split(
                    lot ->
                        lots.add(
                            CompletableFuture.runAsync(
                                () -> {
                                  if (prefetch.isCancelled()) {
                                    return;
                                  }
                                  final LotKeys keys = new LotKeys();
                                  try {
                                    lot.addKeysTo(keys);
                                  } catch (final RuntimeException e) {
                                    stop(prefetch, e);
                                    return;
                                  }
                                  if (items.addAndGet(keys.items) > maxItems) {
                                    // over the item budget: the block is invalid
                                    prefetch.cancel();
                                    return;
                                  }
                                  accounts.addAndGet(keys.accountKeys.size());
                                  storageSlots.addAndGet(keys.storageKeys.size());
                                  read(worldState, ACCOUNT_INFO_STATE, keys.accountKeys, prefetch);
                                  read(
                                      worldState,
                                      ACCOUNT_STORAGE_STORAGE,
                                      keys.storageKeys,
                                      prefetch);
                                },
                                fetchExecutor)));
              } catch (final RuntimeException e) {
                stop(prefetch, e);
              }
              return lots;
            },
            orchestrationExecutor)
        .thenCompose(lots -> CompletableFuture.allOf(lots.toArray(CompletableFuture[]::new)))
        .whenComplete(
            (result, ex) -> {
              if (ex != null) {
                LOG.error("Error during prefetch", ex);
              } else {
                LOG.info(
                    "Prefetch {}: {} accounts + {} storage slots{}",
                    prefetch.isCancelled() ? "cancelled" : "completed",
                    accounts.get(),
                    storageSlots.get(),
                    batchSize > 0 ? " in lots of " + batchSize : " in a single lot");
              }
            });
  }

  /** A malformed block access list belongs to an invalid block: nothing more is read for it. */
  private static void stop(final BalPrefetch prefetch, final RuntimeException e) {
    prefetch.cancel();
    LOG.debug("Prefetch stopped, block access list not readable", e);
  }

  private void read(
      final BonsaiWorldState worldState,
      final SegmentIdentifier segment,
      final List<byte[]> keys,
      final BalPrefetch prefetch) {
    if (isSortingEnabled) {
      keys.sort(KEY_COMPARATOR);
    }
    final int readSize = lotSize(keys.size());
    for (int start = 0; start < keys.size() && !prefetch.isCancelled(); start += readSize) {
      worldState
          .getWorldStateStorage()
          .getMultipleFlat(segment, keys.subList(start, Math.min(start + readSize, keys.size())));
    }
  }

  private int lotSize(final int count) {
    return batchSize > 0 ? batchSize : Math.max(count, 1);
  }

  private static Optional<BonsaiWorldState> openWorldState(
      final ProtocolContext protocolContext, final BlockHeader parentHeader) {
    final Optional<BonsaiWorldState> worldState =
        protocolContext
            .getWorldStateArchive()
            .getWorldState(
                WorldStateQueryParams.newBuilder()
                    .withBlockHeader(parentHeader)
                    .withShouldWorldStateUpdateHead(false)
                    .build())
            .map(BonsaiWorldState.class::cast);
    if (worldState.isEmpty()) {
      LOG.debug(
          "Prefetch skipped, world state of block {} not available", parentHeader.toLogString());
    }
    return worldState;
  }

  /**
   * The keys of a lot of accounts, and its EIP-7928 items (accounts plus storage keys). An address
   * is hashed here rather than through the shared address hash cache, which the addresses of a
   * block outnumber.
   */
  private static final class LotKeys {
    private final List<byte[]> accountKeys = new ArrayList<>();
    private final List<byte[]> storageKeys = new ArrayList<>();
    private long items;

    void add(final BlockAccessList.AccountChanges account) {
      final byte[] accountKey = addAccount(account.address());
      for (final BlockAccessList.SlotChanges slotChanges : account.storageChanges()) {
        addStorageKey(accountKey, slotChanges.slot().getSlotHash().getBytes());
      }
      for (final BlockAccessList.SlotRead slotRead : account.storageReads()) {
        addStorageKey(accountKey, slotRead.slot().getSlotHash().getBytes());
      }
    }

    /** Reads only what the keys need: the address and the slots, not the changes. */
    void add(final RLPInput account) {
      account.enterList();
      final byte[] accountKey = addAccount(Address.readFrom(account));
      account.enterList();
      while (!account.isEndOfCurrentList()) {
        account.enterList();
        addSlot(accountKey, account.readUInt256Scalar());
        account.skipNext();
        account.leaveList();
      }
      account.leaveList();
      account.enterList();
      while (!account.isEndOfCurrentList()) {
        addSlot(accountKey, account.readUInt256Scalar());
      }
      account.leaveList();
    }

    private byte[] addAccount(final Address address) {
      items++;
      final byte[] accountKey = Hash.hash(address.getBytes()).getBytes().toArrayUnsafe();
      accountKeys.add(accountKey);
      return accountKey;
    }

    private void addSlot(final byte[] accountKey, final UInt256 slot) {
      addStorageKey(accountKey, Hash.hash(slot).getBytes());
    }

    private void addStorageKey(final byte[] accountKey, final Bytes slotHash) {
      items++;
      final byte[] storageKey = new byte[accountKey.length + slotHash.size()];
      System.arraycopy(accountKey, 0, storageKey, 0, accountKey.length);
      System.arraycopy(slotHash.toArrayUnsafe(), 0, storageKey, accountKey.length, slotHash.size());
      storageKeys.add(storageKey);
    }
  }
}
