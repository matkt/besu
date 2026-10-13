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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolVersion;
import org.hyperledger.besu.ethereum.eth.SnapProtocol;
import org.hyperledger.besu.ethereum.eth.manager.ChainState;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.EthProtocolMessages;
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.BlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.ByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetAccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetBlockAccessListsMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetByteCodesMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.GetStorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.SnapV2;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.PeerResponse;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest.Protocol;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Serves snap requests from in-memory world states, with real range proofs, and answers other
 * requests through pluggable handlers. Stands in for the {@link
 * org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestScheduler}.
 */
public final class FakeSnapNetwork implements RequestSender {

  private final Map<Bytes32, ServedState> states = new ConcurrentHashMap<>();
  private final Map<Bytes32, Bytes> codes = new ConcurrentHashMap<>();
  private final ExecutorService workers = Executors.newFixedThreadPool(8);
  // snap/2 peers, and a snap/1 peer serving block access lists over eth/71
  private final List<EthPeer> peers =
      List.of(
          peer(SnapProtocol.SNAP2, EthProtocolVersion.V71.getCapability()),
          peer(SnapProtocol.SNAP1, EthProtocolVersion.V71.getCapability()),
          peer(SnapProtocol.SNAP2, EthProtocolVersion.V68.getCapability()));
  private final AtomicInteger nextPeer = new AtomicInteger();
  private final AtomicLong requests = new AtomicLong();
  private final List<String> unagreedRequests = new CopyOnWriteArrayList<>();
  private final Map<String, Function<MessageData, MessageData>> handlers =
      new ConcurrentHashMap<>();
  private final Map<Hash, Bytes> blockAccessLists = new ConcurrentHashMap<>();
  private final Set<EthPeer> withoutState = ConcurrentHashMap.newKeySet();
  private final Set<EthPeer> withoutBlockAccessLists = ConcurrentHashMap.newKeySet();
  private final Map<EthPeer, AtomicLong> stateRequests = new ConcurrentHashMap<>();
  private volatile int maxResponseBytes = Integer.MAX_VALUE;
  private volatile long latencyMillis;

  /** Starts serving a world state. */
  public void serve(final TestWorld world) {
    states.put(world.root(), new ServedState(world));
    codes.putAll(world.codes());
  }

  /** Stops serving the state with this root, as peers do once it is too old. */
  public void forget(final Bytes32 root) {
    states.remove(root);
  }

  /** Caps every snap response, to force many requests and split storages. */
  public void maxResponseBytes(final int bytes) {
    this.maxResponseBytes = bytes;
  }

  /** Delays every response, so that the pivot can move while requests are in flight. */
  public void latency(final long millis) {
    this.latencyMillis = millis;
  }

  /** Answers requests of a protocol with the given code through a handler. */
  public void handle(
      final Protocol protocol, final int code, final Function<MessageData, MessageData> handler) {
    handlers.put(protocol + ":" + code, handler);
  }

  /** Serves the block access list of a block. */
  public void serveBlockAccessList(final Hash blockHash, final Bytes rlp) {
    blockAccessLists.put(blockHash, rlp);
  }

  /** Stops serving the block access list of a block, as if every peer pruned it. */
  public void forgetBlockAccessList(final Hash blockHash) {
    blockAccessLists.remove(blockHash);
  }

  public long requestCount() {
    return requests.get();
  }

  /** Makes a peer answer every account and storage range request with nothing. */
  public void serveNoState(final int peer) {
    withoutState.add(peers.get(peer));
  }

  /** Makes a peer answer every block access list request with nothing. */
  public void serveNoBlockAccessLists(final int peer) {
    withoutBlockAccessLists.add(peers.get(peer));
  }

  /** Returns the account and storage range requests a peer received. */
  public long stateRequestsTo(final EthPeer peer) {
    return stateRequests.getOrDefault(peer, new AtomicLong()).get();
  }

  /** Returns the account and storage range requests all peers received. */
  public long stateRequestCount() {
    return stateRequests.values().stream().mapToLong(AtomicLong::get).sum();
  }

  public List<EthPeer> peers() {
    return peers;
  }

  /** Returns the requests sent over a protocol version their peer did not agree. */
  public List<String> unagreedRequests() {
    return unagreedRequests;
  }

  @Override
  public <T> CompletableFuture<PeerResponse<T>> submit(
      final SyncRequest<T> request, final Set<EthPeer> excluded) {
    final EthPeer peer = nextPeer(request, excluded);
    if (peer == null) {
      return CompletableFuture.failedFuture(
          new RequestFailedException(RequestFailedException.Reason.NO_PEER, null, "no peer"));
    }
    requests.incrementAndGet();
    return CompletableFuture.supplyAsync(
        () -> {
          if (latencyMillis > 0) {
            try {
              Thread.sleep(latencyMillis);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          final Protocol protocol = request.protocolFor(peer);
          final MessageData message = request.message(peer);
          if (!agreed(peer, protocol, message)) {
            unagreedRequests.add(request.name() + " over " + protocol);
          }
          final MessageData raw = response(peer, protocol, message);
          // eth responses reach requests already unwrapped, snap ones keep their request id
          final MessageData response =
              protocol == Protocol.SNAP ? raw.wrapMessageData(BigInteger.ONE) : raw;
          try {
            return new PeerResponse<>(request.decode(response, peer), peer);
          } catch (final SyncRequest.EmptyResponseException e) {
            throw new RequestFailedException(
                RequestFailedException.Reason.EMPTY, peer, e.getMessage());
          } catch (final RuntimeException e) {
            throw new RequestFailedException(
                RequestFailedException.Reason.INVALID, peer, e.toString());
          }
        },
        workers);
  }

  @Override
  public <T> CompletableFuture<PeerResponse<T>> submitWithRetries(
      final SyncRequest<T> request, final int attempts) {
    return submit(request);
  }

  @Override
  public int peerCount(final Protocol protocol) {
    return peers.size();
  }

  @Override
  public int capacity(final Protocol protocol) {
    return peers.size() * 4;
  }

  @Override
  public ExecutorService workers() {
    return workers;
  }

  @Override
  public void stop() {
    workers.shutdownNow();
  }

  private MessageData response(
      final EthPeer peer, final Protocol protocol, final MessageData message) {
    if (protocol == Protocol.SNAP && isStateRequest(message)) {
      return stateResponse(peer, message);
    }
    if (withoutBlockAccessLists.contains(peer)) {
      if (protocol == Protocol.SNAP && message.getCode() == SnapV2.GET_BLOCK_ACCESS_LISTS) {
        return BlockAccessListsMessage.create(List.of());
      }
      if (protocol == Protocol.ETH
          && message.getCode() == EthProtocolMessages.GET_BLOCK_ACCESS_LISTS) {
        return org.hyperledger.besu.ethereum.eth.messages.BlockAccessListsMessage.create(List.of());
      }
    }
    return respond(protocol, message);
  }

  private MessageData respond(final Protocol protocol, final MessageData message) {
    final Function<MessageData, MessageData> handler =
        handlers.get(protocol + ":" + message.getCode());
    if (handler != null) {
      return handler.apply(message);
    }
    if (protocol != Protocol.SNAP) {
      if (message.getCode() == EthProtocolMessages.GET_BLOCK_ACCESS_LISTS) {
        return ethBlockAccessListsResponse(
            org.hyperledger.besu.ethereum.eth.messages.GetBlockAccessListsMessage.readFrom(
                message));
      }
      throw new IllegalArgumentException("no handler for eth code " + message.getCode());
    }
    return switch (message.getCode()) {
      case SnapV2.GET_ACCOUNT_RANGE -> accountRange(GetAccountRangeMessage.readFrom(message));
      case SnapV2.GET_STORAGE_RANGE -> storageRanges(GetStorageRangeMessage.readFrom(message));
      case SnapV2.GET_BYTECODES -> byteCodes(GetByteCodesMessage.readFrom(message));
      case SnapV2.GET_BLOCK_ACCESS_LISTS ->
          blockAccessListsResponse(GetBlockAccessListsMessage.readFrom(message));
      default ->
          throw new IllegalArgumentException("no handler for snap code " + message.getCode());
    };
  }

  // the next peer, in turn, that can serve the request; eth task filters need real peer state
  private EthPeer nextPeer(final SyncRequest<?> request, final Set<EthPeer> excluded) {
    for (int i = 0; i < peers.size(); i++) {
      final EthPeer peer = peers.get(Math.floorMod(nextPeer.getAndIncrement(), peers.size()));
      if (!excluded.contains(peer)
          && (request.protocol() == Protocol.ETH || request.canServe(peer))) {
        return peer;
      }
    }
    return null;
  }

  // block access lists need snap/2 or eth/71, the rest any snap or eth version
  private static boolean agreed(
      final EthPeer peer, final Protocol protocol, final MessageData message) {
    final Set<Capability> capabilities = peer.getAgreedCapabilities();
    if (protocol == Protocol.SNAP) {
      return message.getCode() == SnapV2.GET_BLOCK_ACCESS_LISTS
          ? capabilities.contains(SnapProtocol.SNAP2)
          : SnapProtocol.isAgreed(capabilities);
    }
    return message.getCode() != EthProtocolMessages.GET_BLOCK_ACCESS_LISTS
        || capabilities.stream().anyMatch(EthProtocol::isEth71Compatible);
  }

  private static boolean isStateRequest(final MessageData message) {
    return message.getCode() == SnapV2.GET_ACCOUNT_RANGE
        || message.getCode() == SnapV2.GET_STORAGE_RANGE;
  }

  private MessageData stateResponse(final EthPeer peer, final MessageData message) {
    stateRequests.computeIfAbsent(peer, p -> new AtomicLong()).incrementAndGet();
    if (!withoutState.contains(peer)) {
      return respond(Protocol.SNAP, message);
    }
    return message.getCode() == SnapV2.GET_ACCOUNT_RANGE
        ? AccountRangeMessage.create(new TreeMap<>(), List.of())
        : StorageRangeMessage.create(new kotlin.collections.ArrayDeque<>(), List.of());
  }

  /** A mock peer that agreed the given snap and eth versions. */
  public static EthPeer peer(final Capability snap, final Capability eth) {
    final EthPeer peer = mock(EthPeer.class);
    when(peer.getAgreedCapabilities()).thenReturn(Set.of(snap, eth));
    // header tasks report the heights they see to the peer
    when(peer.chainState()).thenReturn(new ChainState());
    return peer;
  }

  private MessageData ethBlockAccessListsResponse(
      final org.hyperledger.besu.ethereum.eth.messages.GetBlockAccessListsMessage message) {
    final List<Optional<BlockAccessList>> out = new ArrayList<>();
    for (final Hash hash : message.blockHashes()) {
      out.add(Optional.ofNullable(blockAccessLists.get(hash)).map(BlockAccessList::fromBytes));
    }
    return org.hyperledger.besu.ethereum.eth.messages.BlockAccessListsMessage.create(out);
  }

  private MessageData blockAccessListsResponse(final GetBlockAccessListsMessage message) {
    final List<Optional<BlockAccessList>> out = new ArrayList<>();
    for (final Hash hash : message.blockHashes(false)) {
      out.add(Optional.ofNullable(blockAccessLists.get(hash)).map(BlockAccessList::fromBytes));
    }
    return BlockAccessListsMessage.create(out);
  }

  private MessageData accountRange(final GetAccountRangeMessage message) {
    final GetAccountRangeMessage.Range range = message.range(false);
    final ServedState state = states.get(Bytes32.wrap(range.worldStateRootHash().getBytes()));
    if (state == null) {
      return AccountRangeMessage.create(new TreeMap<>(), List.of());
    }
    final Bytes32 origin = Bytes32.wrap(range.startKeyHash().getBytes());
    final Bytes32 limit = Bytes32.wrap(range.endKeyHash().getBytes());
    final long bytes = Math.min(range.responseBytes().longValue(), maxResponseBytes);
    final TreeMap<Bytes32, Bytes> out = new TreeMap<>();
    long size = 0;
    for (final Map.Entry<Bytes32, Bytes> entry : state.accounts.tailMap(origin, true).entrySet()) {
      out.put(entry.getKey(), entry.getValue());
      size += 32 + entry.getValue().size();
      if (entry.getKey().compareTo(limit) >= 0 || size >= bytes) {
        break;
      }
    }
    final boolean whole = origin.isZero() && out.size() == state.accounts.size();
    final List<Bytes> proof =
        whole ? List.of() : proof(state.accountTrie, origin, out.isEmpty() ? null : out.lastKey());
    return AccountRangeMessage.create(out, proof);
  }

  private MessageData storageRanges(final GetStorageRangeMessage message) {
    final GetStorageRangeMessage.StorageRange range = message.range(false);
    final ServedState state = states.get(Bytes32.wrap(range.worldStateRootHash().getBytes()));
    final kotlin.collections.ArrayDeque<NavigableMap<Bytes32, Bytes>> slots =
        new kotlin.collections.ArrayDeque<>();
    if (state == null) {
      return StorageRangeMessage.create(slots, List.of());
    }
    final Bytes32 origin = Bytes32.wrap(range.startKeyHash().getBytes());
    final Bytes32 limit = Bytes32.wrap(range.endKeyHash().getBytes());
    final long bytes = Math.min(range.responseBytes().longValue(), maxResponseBytes);
    long size = 0;
    for (final Bytes32 account : range.accountHashes()) {
      final NavigableMap<Bytes32, Bytes> storage = state.storage.get(account);
      if (storage == null) {
        return StorageRangeMessage.create(new kotlin.collections.ArrayDeque<>(), List.of());
      }
      final TreeMap<Bytes32, Bytes> out = new TreeMap<>();
      boolean truncated = false;
      for (final Map.Entry<Bytes32, Bytes> entry : storage.tailMap(origin, true).entrySet()) {
        if (size >= bytes) {
          truncated = true;
          break;
        }
        out.put(entry.getKey(), entry.getValue());
        size += 32 + entry.getValue().size();
        if (entry.getKey().compareTo(limit) >= 0) {
          truncated = entry.getKey().compareTo(storage.lastKey()) < 0;
          break;
        }
      }
      // like geth, an account without slots in the range gets no slot list
      if (!out.isEmpty()) {
        slots.add(out);
      }
      if (truncated || !origin.isZero()) {
        return StorageRangeMessage.create(
            slots,
            proof(state.storageTries.get(account), origin, out.isEmpty() ? null : out.lastKey()));
      }
      if (size >= bytes) {
        break;
      }
    }
    return StorageRangeMessage.create(slots, List.of());
  }

  private MessageData byteCodes(final GetByteCodesMessage message) {
    final List<Bytes> out = new ArrayList<>();
    final long bytes = Math.min(message.responseBytes(false).longValue(), maxResponseBytes);
    long size = 0;
    for (final Bytes32 hash : message.codeHashes(false)) {
      final Bytes code = codes.get(hash);
      if (code != null) {
        out.add(code);
        size += code.size();
        if (size >= bytes) {
          break;
        }
      }
    }
    return ByteCodesMessage.create(out);
  }

  private static List<Bytes> proof(
      final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie,
      final Bytes32 origin,
      final Bytes32 last) {
    final Set<Bytes> nodes =
        new LinkedHashSet<>(trie.getValueWithProof(origin).getProofRelatedNodes());
    if (last != null) {
      nodes.addAll(trie.getValueWithProof(last).getProofRelatedNodes());
    }
    return new ArrayList<>(nodes);
  }

  private static final class ServedState {
    private final NavigableMap<Bytes32, Bytes> accounts;
    private final Map<Bytes32, NavigableMap<Bytes32, Bytes>> storage;
    private final SimpleMerklePatriciaTrie<Bytes32, Bytes> accountTrie =
        new SimpleMerklePatriciaTrie<>(Function.identity());
    private final Map<Bytes32, SimpleMerklePatriciaTrie<Bytes32, Bytes>> storageTries =
        new HashMap<>();

    private ServedState(final TestWorld world) {
      this.accounts = world.accountLeaves();
      this.storage = world.storageLeaves();
      accounts.forEach(accountTrie::put);
      storage.forEach(
          (account, slots) -> {
            final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie =
                new SimpleMerklePatriciaTrie<>(Function.identity());
            slots.forEach(trie::put);
            storageTries.put(account, trie);
          });
    }
  }
}
