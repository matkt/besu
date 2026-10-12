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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.messages.BlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.messages.BlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockBodiesMessage;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockHeadersMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.chain.ChainHeadSource;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * A post-Amsterdam chain of empty blocks whose state changes through block access lists, served
 * over the fake network: headers, bodies, block access lists, and the recent states.
 */
public final class TestChain implements ChainHeadSource {

  private final Random random;
  private final FakeSnapNetwork network;
  private final int servedStates;
  private final int changesPerBlock;
  private final List<BlockHeader> headers = new ArrayList<>();
  private final List<TestWorld> worlds = new ArrayList<>();
  private final Map<Hash, BlockHeader> byHash = new ConcurrentHashMap<>();
  private volatile Hash head;

  public TestChain(
      final Random random,
      final TestWorld genesisWorld,
      final FakeSnapNetwork network,
      final int servedStates,
      final int changesPerBlock) {
    this.random = random;
    this.network = network;
    this.servedStates = servedStates;
    this.changesPerBlock = changesPerBlock;
    final BlockHeader genesis =
        new BlockHeaderTestFixture()
            .number(0)
            .stateRoot(Hash.wrap(genesisWorld.root()))
            .buildHeader();
    add(genesis, genesisWorld);
    network.serve(genesisWorld);
    network.handle(Protocol.ETH, EthProtocolMessages.GET_BLOCK_HEADERS, this::headers);
    network.handle(Protocol.ETH, EthProtocolMessages.GET_BLOCK_BODIES, this::bodies);
  }

  /** Returns the genesis block, to create the local blockchain from. */
  public Block genesisBlock() {
    return new Block(headers.getFirst(), BlockBody.empty());
  }

  /** Appends blocks, making the last one the head. */
  public synchronized void extend(final int blocks) {
    for (int i = 0; i < blocks; i++) {
      final BlockHeader parent = headers.getLast();
      final TestWorld world = worlds.getLast().copy();
      final BlockAccessList bal = world.mutate(random, changesPerBlock);
      final Bytes balRlp = bal.encode();
      final long number = parent.getNumber() + 1;
      final BlockHeader header =
          new BlockHeaderTestFixture()
              .number(number)
              .parentHash(parent.getHash())
              .stateRoot(Hash.wrap(world.root()))
              .timestamp(number * 12)
              .gasLimit(30_000_000)
              .baseFeePerGas(Wei.of(7))
              .withdrawalsRoot(Hash.EMPTY_TRIE_HASH)
              .blobGasUsed(0L)
              .excessBlobGas(BlobGas.ZERO)
              .parentBeaconBlockRoot(Optional.of(Bytes32.ZERO))
              .requestsHash(Hash.EMPTY_REQUESTS_HASH)
              .balHash(Hash.hash(balRlp))
              .buildHeader();
      add(header, world);
      network.serveBlockAccessList(header.getHash(), balRlp);
      network.serve(world);
      if (worlds.size() > servedStates) {
        network.forget(worlds.get(worlds.size() - 1 - servedStates).root());
      }
    }
  }

  /**
   * Replaces the blocks after {@code ancestor} with a new fork of {@code blocks} blocks. The
   * orphaned headers and block access lists stay served by hash.
   */
  public synchronized void reorg(final long ancestor, final int blocks) {
    reorg(ancestor, blocks, true);
  }

  /** Reorganises the chain; without retention, peers forget the orphaned block access lists. */
  public synchronized void reorg(final long ancestor, final int blocks, final boolean retain) {
    while (headers.size() > ancestor + 1) {
      final BlockHeader orphaned = headers.removeLast();
      worlds.removeLast();
      if (!retain) {
        network.forgetBlockAccessList(orphaned.getHash());
      }
    }
    extend(blocks);
  }

  public synchronized BlockHeader header(final long number) {
    return headers.get((int) number);
  }

  public synchronized TestWorld world(final long number) {
    return worlds.get((int) number);
  }

  public synchronized long headNumber() {
    return headers.getLast().getNumber();
  }

  @Override
  public Optional<Hash> headHash() {
    return Optional.ofNullable(head);
  }

  @Override
  public Optional<BlockHeader> knownHeader(final Hash hash) {
    return Optional.ofNullable(byHash.get(hash));
  }

  private void add(final BlockHeader header, final TestWorld world) {
    headers.add(header);
    worlds.add(world);
    byHash.put(header.getHash(), header);
    head = header.getHash();
  }

  // by hash, a reverse request walks parent links, so it also serves orphaned branches
  private synchronized MessageData headers(final MessageData message) {
    final GetBlockHeadersMessage request = GetBlockHeadersMessage.readFrom(message);
    final List<BlockHeader> out = new ArrayList<>();
    if (request.hash().isPresent()) {
      BlockHeader header = byHash.get(request.hash().get());
      while (header != null && out.size() < request.maxHeaders()) {
        out.add(header);
        if (!request.reverse() || request.skip() != 0) {
          break;
        }
        header = byHash.get(header.getParentHash());
      }
      return BlockHeadersMessage.create(out);
    }
    final long start = request.blockNumber().orElse(-1);
    if (start >= 0 && start < headers.size()) {
      final int step = (request.skip() + 1) * (request.reverse() ? -1 : 1);
      for (long n = start;
          n >= 0 && n < headers.size() && out.size() < request.maxHeaders();
          n += step) {
        out.add(headers.get((int) n));
      }
    }
    return BlockHeadersMessage.create(out);
  }

  private MessageData bodies(final MessageData message) {
    final List<BlockBody> out = new ArrayList<>();
    for (final Hash hash : GetBlockBodiesMessage.readFrom(message).hashes()) {
      if (byHash.containsKey(hash)) {
        out.add(new BlockBody(List.of(), List.of(), Optional.of(List.of())));
      }
    }
    return BlockBodiesMessage.create(out);
  }
}
