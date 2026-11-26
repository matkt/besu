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
package org.hyperledger.besu.ethereum.eth.manager.snap;

import static org.hyperledger.besu.ethereum.eth.manager.snap.ResponseLimits.ResponseSizePredicate;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.manager.EthMessages;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV1;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV2;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.StorageSubscriber;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.plugin.services.BesuEvents;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serves the requests of <a
 * href="https://github.com/ethereum/devp2p/blob/master/caps/snap.md">snap</a> once the node synced,
 * one responder per request.
 */
class SnapServer implements BesuEvents.InitialSyncCompletionListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(SnapServer.class);
  private static final StorageSubscriber STATE_READER = new StorageSubscriber() {};

  static final Hash HASH_LAST = Hash.wrap(Bytes32.leftPad(Bytes.fromHexString("FF"), (byte) 0xFF));

  /** Reads the cached world state of a state root, held open while it is read. */
  interface WorldStates {
    <T> Optional<T> read(Hash rootHash, Function<BonsaiWorldStateKeyValueStorage, T> reader);
  }

  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final EthMessages snapMessages;

  private final WorldStateStorageCoordinator worldStateStorageCoordinator;
  private final Optional<ProtocolContext> protocolContext;

  // whether snap server is enabled
  private final boolean snapServerEnabled;

  private final AccountRangeResponder accountRanges;
  private final StorageRangeResponder storageRanges;
  private final ByteCodesResponder byteCodes;
  private final TrieNodesResponder trieNodes;
  private final BlockAccessListsResponder blockAccessLists;

  // provide worldstate storage by root hash
  private Function<Hash, Optional<BonsaiWorldStateKeyValueStorage>> worldStateStorageProvider =
      __ -> Optional.empty();

  SnapServer(
      final SnapSyncConfiguration snapConfig,
      final EthMessages snapMessages,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final ProtocolContext protocolContext,
      final Synchronizer synchronizer) {
    this(
        snapConfig,
        snapMessages,
        worldStateStorageCoordinator,
        protocolContext,
        synchronizer,
        ResponseSizePredicate.DEFAULT_MAX_MILLIS_PER_REQUEST);
  }

  @VisibleForTesting
  SnapServer(
      final SnapSyncConfiguration snapConfig,
      final EthMessages snapMessages,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final ProtocolContext protocolContext,
      final Synchronizer synchronizer,
      final long maxMillisPerRequest) {
    this(
        Optional.ofNullable(snapConfig)
            .map(SnapSyncConfiguration::isSnapServerEnabled)
            .orElse(false),
        snapMessages,
        worldStateStorageCoordinator,
        Optional.of(protocolContext),
        maxMillisPerRequest);
    registerResponseConstructors();

    // subscribe to initial sync completed events to start/stop snap server,
    // not saving the listenerId since we never need to unsubscribe.
    synchronizer.subscribeInitialSync(this);
  }

  /**
   * Create a snap server without registering a listener for worldstate initial sync events. Used by
   * unit tests.
   */
  @VisibleForTesting
  SnapServer(
      final EthMessages snapMessages,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final Function<Hash, Optional<BonsaiWorldStateKeyValueStorage>> worldStateStorageProvider) {
    this(
        snapMessages,
        worldStateStorageCoordinator,
        worldStateStorageProvider,
        ResponseSizePredicate.DEFAULT_MAX_MILLIS_PER_REQUEST);
  }

  @VisibleForTesting
  SnapServer(
      final EthMessages snapMessages,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final Function<Hash, Optional<BonsaiWorldStateKeyValueStorage>> worldStateStorageProvider,
      final long maxMillisPerRequest) {
    this(true, snapMessages, worldStateStorageCoordinator, Optional.empty(), maxMillisPerRequest);
    this.worldStateStorageProvider = worldStateStorageProvider;
  }

  private SnapServer(
      final boolean snapServerEnabled,
      final EthMessages snapMessages,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final Optional<ProtocolContext> protocolContext,
      final long maxMillisPerRequest) {
    this.snapServerEnabled = snapServerEnabled;
    this.snapMessages = snapMessages;
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.protocolContext = protocolContext;
    this.accountRanges = new AccountRangeResponder(this::readState, maxMillisPerRequest);
    this.storageRanges = new StorageRangeResponder(this::readState, maxMillisPerRequest);
    this.byteCodes = new ByteCodesResponder(worldStateStorageCoordinator, maxMillisPerRequest);
    this.trieNodes = new TrieNodesResponder(this::readState, maxMillisPerRequest);
    this.blockAccessLists =
        new BlockAccessListsResponder(
            protocolContext.map(ProtocolContext::getBlockchain), maxMillisPerRequest);
  }

  @Override
  public void onInitialSyncCompleted() {
    start();
  }

  @Override
  public void onInitialSyncRestart() {
    stop();
  }

  public synchronized SnapServer start() {
    if (!isStarted.get() && snapServerEnabled) {
      // if we are bonsai and full flat, we can provide a worldstate storage:
      var worldStateKeyValueStorage = worldStateStorageCoordinator.worldStateKeyValueStorage();
      if (worldStateKeyValueStorage.getDataStorageFormat().isBonsaiFormat()
          && (worldStateStorageCoordinator.isMatchingFlatMode(FlatDbMode.FULL)
              || worldStateStorageCoordinator.isMatchingFlatMode(FlatDbMode.ARCHIVE))) {
        LOGGER.info("Starting SnapServer with Bonsai full flat db");
        var bonsaiArchive =
            protocolContext
                .map(ProtocolContext::getWorldStateArchive)
                .map(BonsaiWorldStateProvider.class::cast);
        var cachedStorageManagerOpt =
            bonsaiArchive.map(archive -> archive.getWorldStateCacheManager());

        if (cachedStorageManagerOpt.isPresent()) {
          // only the world states the node already caches are served: a peer never makes the
          // node roll trie logs back, nor waits on the lock the block import takes
          this.worldStateStorageProvider =
              cachedStorageManagerOpt.get()::getCachedStorageByRootHash;

          var flatDbStrategy =
              ((BonsaiWorldStateKeyValueStorage)
                      worldStateStorageCoordinator.worldStateKeyValueStorage())
                  .getFlatDbStrategy();
          if (!flatDbStrategy.isCodeByCodeHash()) {
            LOGGER.warn("SnapServer requires code stored by codehash, but it is not enabled");
          }
        } else {
          LOGGER.warn(
              "SnapServer started without cached storage manager, this should only happen in tests");
        }
        isStarted.set(true);
      }
    }
    return this;
  }

  public synchronized SnapServer stop() {
    isStarted.set(false);
    return this;
  }

  boolean isStarted() {
    return isStarted.get();
  }

  private void registerResponseConstructors() {
    snapMessages.registerResponseConstructor(
        SnapV1.GET_ACCOUNT_RANGE,
        (peer, messageData, capability) -> constructGetAccountRangeResponse(messageData));
    snapMessages.registerResponseConstructor(
        SnapV1.GET_STORAGE_RANGE,
        (peer, messageData, capability) -> constructGetStorageRangeResponse(messageData));
    snapMessages.registerResponseConstructor(
        SnapV1.GET_BYTECODES,
        (peer, messageData, capability) -> constructGetBytecodesResponse(messageData));
    snapMessages.registerResponseConstructor(
        SnapV1.GET_TRIE_NODES,
        (peer, messageData, capability) -> constructGetTrieNodesResponse(messageData));
    snapMessages.registerResponseConstructor(
        SnapV2.GET_BLOCK_ACCESS_LISTS,
        (peer, messageData, capability) -> constructGetBlockAccessListsResponse(messageData));
  }

  MessageData constructGetAccountRangeResponse(final MessageData message) {
    return isStarted.get() ? accountRanges.respond(message) : AccountRangeResponder.EMPTY;
  }

  MessageData constructGetStorageRangeResponse(final MessageData message) {
    return isStarted.get() ? storageRanges.respond(message) : StorageRangeResponder.EMPTY;
  }

  MessageData constructGetBytecodesResponse(final MessageData message) {
    return isStarted.get() ? byteCodes.respond(message) : ByteCodesResponder.EMPTY;
  }

  MessageData constructGetTrieNodesResponse(final MessageData message) {
    return isStarted.get() ? trieNodes.respond(message) : TrieNodesResponder.EMPTY;
  }

  MessageData constructGetBlockAccessListsResponse(final MessageData message) {
    return isStarted.get() ? blockAccessLists.respond(message) : BlockAccessListsResponder.EMPTY;
  }

  /** Reads a cached world state, subscribed to it so that the cache cannot close it meanwhile. */
  private <T> Optional<T> readState(
      final Hash rootHash, final Function<BonsaiWorldStateKeyValueStorage, T> read) {
    final Optional<BonsaiWorldStateKeyValueStorage> maybeStorage =
        worldStateStorageProvider.apply(rootHash);
    if (maybeStorage.isEmpty()) {
      return Optional.empty();
    }
    final BonsaiWorldStateKeyValueStorage storage = maybeStorage.get();
    final long subscription;
    try {
      subscription = storage.subscribe(STATE_READER);
    } catch (final RuntimeException closed) {
      // the cache closed it after the lookup
      return Optional.empty();
    }
    try {
      return Optional.of(read.apply(storage));
    } finally {
      storage.unSubscribe(subscription);
    }
  }

  static String asLogHash(final Bytes32 hash) {
    var str = hash.toHexString();
    return str.substring(0, 4) + ".." + str.substring(59, 63);
  }
}
