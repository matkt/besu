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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestFailedException;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.RequestSender;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SnapMessages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.NodeEncoding;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads block access lists over snap/2 and checks each one against the block access list hash
 * of its header. Blocks a peer could not serve are asked to other peers until every list is
 * received or the attempts run out.
 */
public final class BalFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(BalFetcher.class);

  private final RequestSender sender;
  private final int batchSize;
  private final long responseBytes;
  private final int maxRounds;
  private final ScheduledExecutorService timer =
      Executors.newSingleThreadScheduledExecutor(
          Thread.ofPlatform().name("snap2-bal-retry").daemon().factory());

  public BalFetcher(
      final RequestSender sender,
      final int batchSize,
      final long responseBytes,
      final int maxRounds) {
    this.sender = sender;
    this.batchSize = batchSize;
    this.responseBytes = responseBytes;
    this.maxRounds = maxRounds;
  }

  /**
   * Fetches the block access lists of the given headers.
   *
   * @return the raw lists, in the order of the headers
   */
  public CompletableFuture<List<Bytes>> fetch(final List<BlockHeader> headers) {
    return fetch(headers, maxRounds);
  }

  /**
   * Fetches the block access lists of the given headers, giving up after {@code rounds} rounds.
   *
   * @return the raw lists, in the order of the headers
   */
  public CompletableFuture<List<Bytes>> fetch(final List<BlockHeader> headers, final int rounds) {
    for (final BlockHeader header : headers) {
      if (header.getBalHash().isEmpty()) {
        return CompletableFuture.failedFuture(
            new IllegalStateException(
                "block "
                    + header.getNumber()
                    + " has no block access list, snap/2 cannot sync it"));
      }
    }
    final Map<Hash, Bytes> received = new HashMap<>();
    final CompletableFuture<List<Bytes>> result = new CompletableFuture<>();
    round(headers, received, new HashMap<>(), 0, rounds, result);
    return result;
  }

  public void stop() {
    timer.shutdownNow();
  }

  private void round(
      final List<BlockHeader> headers,
      final Map<Hash, Bytes> received,
      final Map<Hash, Set<EthPeer>> lacking,
      final int round,
      final int rounds,
      final CompletableFuture<List<Bytes>> result) {
    final List<BlockHeader> missing =
        headers.stream().filter(header -> !received.containsKey(header.getHash())).toList();
    if (missing.isEmpty()) {
      result.complete(headers.stream().map(header -> received.get(header.getHash())).toList());
      return;
    }
    if (round >= rounds) {
      result.completeExceptionally(
          new IllegalStateException(
              "no peer served the block access list of block " + missing.getFirst().getNumber()));
      return;
    }
    final List<CompletableFuture<Void>> requests = new ArrayList<>();
    for (int i = 0; i < missing.size(); i += batchSize) {
      final List<BlockHeader> batch = missing.subList(i, Math.min(missing.size(), i + batchSize));
      final Set<EthPeer> excluded = new HashSet<>();
      batch.forEach(header -> excluded.addAll(lacking.getOrDefault(header.getHash(), Set.of())));
      requests.add(
          sender
              .submit(new BalRequest(batch, responseBytes), excluded)
              .handle(
                  (response, error) -> {
                    synchronized (received) {
                      if (error == null) {
                        received.putAll(response.value());
                        batch.stream()
                            .filter(header -> !response.value().containsKey(header.getHash()))
                            .forEach(
                                header ->
                                    lacking
                                        .computeIfAbsent(header.getHash(), h -> new HashSet<>())
                                        .add(response.peer()));
                      } else {
                        RequestFailedException.from(error)
                            .flatMap(RequestFailedException::peer)
                            .ifPresent(
                                peer ->
                                    batch.forEach(
                                        header ->
                                            lacking
                                                .computeIfAbsent(
                                                    header.getHash(), h -> new HashSet<>())
                                                .add(peer)));
                      }
                    }
                    return null;
                  }));
    }
    CompletableFuture.allOf(requests.toArray(CompletableFuture[]::new))
        .whenComplete(
            (unused, error) -> {
              final boolean progressed;
              synchronized (received) {
                progressed =
                    missing.stream().anyMatch(header -> received.containsKey(header.getHash()));
              }
              if (progressed) {
                round(headers, received, lacking, round + 1, rounds, result);
              } else {
                // nobody had them yet, peers that lacked them may have caught up since
                LOG.debug("No block access list received in round {}, retrying", round);
                if (round % 5 == 4) {
                  lacking.clear();
                }
                try {
                  timer.schedule(
                      () -> round(headers, received, lacking, round + 1, rounds, result),
                      Duration.ofSeconds(Math.min(10, 1 + round)).toMillis(),
                      TimeUnit.MILLISECONDS);
                } catch (final RuntimeException e) {
                  result.completeExceptionally(e);
                }
              }
            });
  }

  /** Requests block access lists by block hash; each returned list is checked. */
  static final class BalRequest implements SyncRequest<Map<Hash, Bytes>> {
    private final List<BlockHeader> headers;
    private final long bytes;

    BalRequest(final List<BlockHeader> headers, final long bytes) {
      this.headers = headers;
      this.bytes = bytes;
    }

    @Override
    public Protocol protocol() {
      return Protocol.SNAP;
    }

    @Override
    public Priority priority() {
      return Priority.PIVOT;
    }

    @Override
    public String name() {
      return "block_access_lists";
    }

    @Override
    public MessageData message(final EthPeer peer) {
      return SnapMessages.getBlockAccessLists(
          headers.stream().map(BlockHeader::getHash).toList(), bytes);
    }

    @Override
    public Map<Hash, Bytes> decode(final MessageData response, final EthPeer peer) {
      final List<Bytes> lists = SnapMessages.blockAccessLists(response);
      if (lists.size() > headers.size()) {
        throw new InvalidResponseException("more block access lists than requested", true);
      }
      final Map<Hash, Bytes> out = new HashMap<>();
      for (int i = 0; i < lists.size(); i++) {
        final Bytes list = lists.get(i);
        if (list.isEmpty()) {
          continue;
        }
        final BlockHeader header = headers.get(i);
        if (!NodeEncoding.keccak(list).equals(header.getBalHash().orElseThrow().getBytes())) {
          throw new InvalidResponseException(
              "block access list does not match the header of block " + header.getNumber(), true);
        }
        // keep a compact copy, the list slices the whole message
        out.put(header.getHash(), list.copy());
      }
      if (out.isEmpty()) {
        throw new EmptyResponseException("no block access list");
      }
      return out;
    }
  }
}
