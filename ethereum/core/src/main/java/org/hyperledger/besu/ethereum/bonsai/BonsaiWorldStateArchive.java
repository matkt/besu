/*
 * Copyright ConsenSys AG.
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
 *
 */

package org.hyperledger.besu.ethereum.bonsai;

import static org.hyperledger.besu.datatypes.Hash.fromPlugin;
import static org.hyperledger.besu.util.Slf4jLambdaHelper.debugLambda;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.evm.worldstate.WorldState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BonsaiWorldStateArchive implements WorldStateArchive {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiWorldStateArchive.class);

  private static final long RETAINED_LAYERS = 512; // at least 256 + typical rollbacks

  private final Blockchain blockchain;

  private final BonsaiPersistedWorldState persistedState;
  private final Map<Bytes32, BonsaiLayeredWorldState> layeredWorldStatesByHash;
  private final BonsaiWorldStateKeyValueStorage worldStateStorage;
  private final long maxLayersToLoad;

  public BonsaiWorldStateArchive(final StorageProvider provider, final Blockchain blockchain) {
    this(provider, blockchain, RETAINED_LAYERS, new HashMap<>());
  }

  public BonsaiWorldStateArchive(
      final StorageProvider provider, final Blockchain blockchain, final long maxLayersToLoad) {
    this(provider, blockchain, maxLayersToLoad, new HashMap<>());
  }

  public BonsaiWorldStateArchive(
      final StorageProvider provider,
      final Blockchain blockchain,
      final long maxLayersToLoad,
      final Map<Bytes32, BonsaiLayeredWorldState> layeredWorldStatesByHash) {
    this.blockchain = blockchain;

    this.worldStateStorage = new BonsaiWorldStateKeyValueStorage(provider);
    this.persistedState = new BonsaiPersistedWorldState(this, worldStateStorage);
    this.layeredWorldStatesByHash = layeredWorldStatesByHash;
    this.maxLayersToLoad = maxLayersToLoad;
    blockchain.observeBlockAdded(this::blockAddedHandler);
  }

  private void blockAddedHandler(final BlockAddedEvent event) {
    LOG.debug("New block add event {}", event);
    if (event.isNewCanonicalHead()) {
      final BlockHeader eventBlockHeader = event.getBlock().getHeader();
      layeredWorldStatesByHash.computeIfPresent(
          eventBlockHeader.getParentHash(),
          (parentHash, bonsaiLayeredWorldState) -> {
            if (layeredWorldStatesByHash.containsKey(eventBlockHeader.getHash())) {
              bonsaiLayeredWorldState.setNextWorldView(
                  Optional.of(layeredWorldStatesByHash.get(eventBlockHeader.getHash())));
            }
            return bonsaiLayeredWorldState;
          });
    }
  }

  @Override
  public Optional<WorldState> get(final Hash rootHash, final Hash blockHash) {
    if (layeredWorldStatesByHash.containsKey(blockHash)) {
      return Optional.of(layeredWorldStatesByHash.get(blockHash));
    } else if (rootHash.equals(persistedState.blockHash())) {
      return Optional.of(persistedState);
    } else {
      return Optional.empty();
    }
  }

  public void addLayeredWorldState(
      final BonsaiWorldView persistedWorldState,
      final BlockHeader blockHeader,
      final Hash worldStateRootHash,
      final TrieLogLayer trieLog) {
    final BonsaiLayeredWorldState bonsaiLayeredWorldState =
        new BonsaiLayeredWorldState(
            blockchain,
            this,
            Optional.of(persistedWorldState),
            blockHeader.getNumber(),
            worldStateRootHash,
            trieLog);
    debugLambda(
        LOG,
        "adding layered world state for block {}, state root hash {}",
        blockHeader::toLogString,
        worldStateRootHash::toHexString);
    layeredWorldStatesByHash.put(blockHeader.getHash(), bonsaiLayeredWorldState);
  }

  public Optional<TrieLogLayer> getTrieLogLayer(final Hash blockHash) {
    return getTrieLogLayer(blockHash, true);
  }

  public Optional<TrieLogLayer> getTrieLogLayer(final Hash blockHash, final boolean b) {
    if (b) System.out.println("getTrieLogLayer for blockhash " + blockHash);
    if (layeredWorldStatesByHash.containsKey(blockHash)) {
      if (b) System.out.println("layeredWorldStatesByHash contains " + blockHash);
      return Optional.of(layeredWorldStatesByHash.get(blockHash).getTrieLog());
    } else {
      if (b)
        System.out.println(
            "trielog on the database " + worldStateStorage.getTrieLog(blockHash).isPresent());
      return worldStateStorage.getTrieLog(blockHash).map(TrieLogLayer::fromBytes);
    }
  }

  @Override
  public boolean isWorldStateAvailable(final Hash rootHash, final Hash blockHash) {
    return layeredWorldStatesByHash.containsKey(blockHash)
        || persistedState.blockHash().equals(blockHash)
        || worldStateStorage.isWorldStateAvailable(rootHash, blockHash);
  }

  @Override
  public Optional<MutableWorldState> getMutable(
      final long blockNumber, final boolean isPersistingState) {
    final Optional<Hash> blockHashByNumber = blockchain.getBlockHashByNumber(blockNumber);
    if (blockHashByNumber.isPresent()) {
      return getMutable(null, blockHashByNumber.get(), isPersistingState);
    }
    return Optional.empty();
  }

  @Override
  public Optional<MutableWorldState> getMutable(
      final Hash rootHash, final Hash blockHash, final boolean isPersistingState) {
    if (!isPersistingState) {
      if (layeredWorldStatesByHash.containsKey(blockHash)) {
        return Optional.of(layeredWorldStatesByHash.get(blockHash));
      } else {
        final BlockHeader header = blockchain.getBlockHeader(blockHash).get();
        final BlockHeader currentHeader = blockchain.getChainHeadHeader();
        if ((currentHeader.getNumber() - header.getNumber()) >= maxLayersToLoad) {
          LOG.warn("Exceeded the limit of back layers that can be loaded ({})", maxLayersToLoad);
          return Optional.empty();
        }
        final Optional<TrieLogLayer> trieLogLayer = getTrieLogLayer(blockHash, false);
        if (trieLogLayer.isPresent()) {
          return Optional.of(
              new BonsaiLayeredWorldState(
                  blockchain,
                  this,
                  Optional.empty(),
                  header.getNumber(),
                  fromPlugin(header.getStateRoot()),
                  trieLogLayer.get()));
        }
      }
    } else {
      return getMutable(rootHash, blockHash);
    }
    return Optional.empty();
  }

  @Override
  public Optional<MutableWorldState> getMutable(final Hash rootHash, final Hash blockHash) {
    System.out.println(
        "getMutable found block hash"
            + persistedState.blockHash()
            + " stateroot "
            + persistedState.worldStateRootHash);
    System.out.println("getMutable asked block hash " + blockHash + " stateroot " + rootHash);
    System.out.println(
        "get head asked block hash "
            + blockchain.getChainHeadHeader().getHash()
            + " stateroot "
            + blockchain.getChainHeadHeader().getStateRoot()
            + " "
            + blockchain.getChainHeadHeader().getNumber());
    System.out.println(
        "stateroot in trie log " + worldStateStorage.getStateTrieNode(Bytes.EMPTY).map(Hash::hash));
    if (blockHash.equals(persistedState.blockHash())) {
      return Optional.of(persistedState);
    } else {
      try {
        System.out.println("starting rollback");
        final Optional<BlockHeader> maybePersistedHeader =
            blockchain.getBlockHeader(persistedState.blockHash()).map(BlockHeader.class::cast);
        System.out.println("found persisted header " + maybePersistedHeader);
        final List<TrieLogLayer> rollBacks = new ArrayList<>();
        final List<TrieLogLayer> rollForwards = new ArrayList<>();
        if (maybePersistedHeader.isEmpty()) {
          getTrieLogLayer(persistedState.blockHash()).ifPresent(rollBacks::add);
        } else {
          BlockHeader targetHeader = blockchain.getBlockHeader(blockHash).get();
          System.out.println(
              "targetHeader header "
                  + targetHeader.getNumber()
                  + " "
                  + targetHeader.getHash()
                  + " "
                  + targetHeader.getParentHash()
                  + " "
                  + targetHeader.getStateRoot());
          BlockHeader persistedHeader = maybePersistedHeader.get();
          System.out.println(
              "persistedHeader header "
                  + persistedHeader.getNumber()
                  + " "
                  + persistedHeader.getHash()
                  + " "
                  + persistedHeader.getParentHash()
                  + " "
                  + persistedHeader.getStateRoot());
          // roll back from persisted to even with target
          Hash persistedBlockHash = persistedHeader.getBlockHash();
          while (persistedHeader.getNumber() > targetHeader.getNumber()) {
            LOG.info("Rollback {}", persistedBlockHash);
            rollBacks.add(getTrieLogLayer(persistedBlockHash).get());
            persistedHeader = blockchain.getBlockHeader(persistedHeader.getParentHash()).get();
            persistedBlockHash = persistedHeader.getBlockHash();
          }
          // roll forward to target
          Hash targetBlockHash = targetHeader.getBlockHash();
          while (persistedHeader.getNumber() < targetHeader.getNumber()) {
            LOG.info("Rollforward {}", targetBlockHash);
            rollForwards.add(getTrieLogLayer(targetBlockHash).get());
            targetHeader = blockchain.getBlockHeader(targetHeader.getParentHash()).get();
            targetBlockHash = targetHeader.getBlockHash();
          }

          // roll back in tandem until we hit a shared state
          while (!persistedBlockHash.equals(targetBlockHash)) {
            LOG.info("Paired Rollback {}", persistedBlockHash);
            LOG.info("Paired Rollforward {}", targetBlockHash);
            rollForwards.add(getTrieLogLayer(targetBlockHash).get());
            targetHeader = blockchain.getBlockHeader(targetHeader.getParentHash()).get();

            rollBacks.add(getTrieLogLayer(persistedBlockHash).get());
            persistedHeader = blockchain.getBlockHeader(persistedHeader.getParentHash()).get();

            targetBlockHash = targetHeader.getBlockHash();
            persistedBlockHash = persistedHeader.getBlockHash();
          }
        }

        // attempt the state rolling
        final BonsaiWorldStateUpdater bonsaiUpdater = getUpdater();
        try {
          for (final TrieLogLayer rollBack : rollBacks) {
            LOG.info("Attempting Rollback of {}", rollBack.getBlockHash());
            System.out.println("**********************************************");
            System.out.println("trie log block hash " + rollBack.dump());
            System.out.println("**********************************************");
            bonsaiUpdater.rollBack(rollBack);
          }
          for (int i = rollForwards.size() - 1; i >= 0; i--) {
            LOG.info("Attempting Rollforward of {}", rollForwards.get(i).getBlockHash());
            System.out.println("**********************************************");
            System.out.println("trie log block hash " + rollForwards.get(i).dump());
            System.out.println("**********************************************");
            bonsaiUpdater.rollForward(rollForwards.get(i));
          }
          bonsaiUpdater.commit();

          persistedState.persist(blockchain.getBlockHeader(blockHash).get());

          LOG.debug("Archive rolling finished, now at {}", blockHash);
          return Optional.of(persistedState);
        } catch (final Exception e) {
          // if we fail we must clean up the updater
          bonsaiUpdater.reset();
          throw new RuntimeException(e);
        }
      } catch (final RuntimeException re) {
        re.printStackTrace(System.out);
        return Optional.empty();
      }
    }
  }

  BonsaiWorldStateUpdater getUpdater() {
    return (BonsaiWorldStateUpdater) persistedState.updater();
  }

  @Override
  public MutableWorldState getMutable() {
    return persistedState;
  }

  @Override
  public void setArchiveStateUnSafe(final BlockHeader blockHeader) {
    persistedState.setArchiveStateUnSafe(blockHeader);
  }

  @Override
  public Optional<Bytes> getNodeData(final Hash hash) {
    return Optional.empty();
  }

  @Override
  public Optional<WorldStateProof> getAccountProof(
      final Hash worldStateRoot,
      final Address accountAddress,
      final List<UInt256> accountStorageKeys) {
    // FIXME we can do proofs for layered tries and the persisted trie
    return Optional.empty();
  }

  void scrubLayeredCache(final long newMaxHeight) {
    final long waterline = newMaxHeight - RETAINED_LAYERS;
    layeredWorldStatesByHash.entrySet().removeIf(entry -> entry.getValue().getHeight() < waterline);
  }
}
