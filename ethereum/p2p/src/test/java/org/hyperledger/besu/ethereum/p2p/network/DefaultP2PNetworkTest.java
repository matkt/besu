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
 */
package org.hyperledger.besu.ethereum.p2p.network;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.SECP256K1.KeyPair;
import org.hyperledger.besu.ethereum.p2p.config.DiscoveryConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.NetworkingConfiguration;
import org.hyperledger.besu.ethereum.p2p.config.RlpxConfiguration;
import org.hyperledger.besu.ethereum.p2p.discovery.DiscoveryPeer;
import org.hyperledger.besu.ethereum.p2p.discovery.Endpoint;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerBondedObserver;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryAgent;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryEvent;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryStatus;
import org.hyperledger.besu.ethereum.p2p.peers.MaintainedPeers;
import org.hyperledger.besu.ethereum.p2p.peers.Peer;
import org.hyperledger.besu.ethereum.p2p.peers.PeerTestHelper;
import org.hyperledger.besu.ethereum.p2p.rlpx.RlpxAgent;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.MockPeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MockSubProtocol;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages.DisconnectMessage.DisconnectReason;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.nat.NatMethod;
import org.hyperledger.besu.nat.NatService;
import org.hyperledger.besu.nat.core.domain.NetworkProtocol;
import org.hyperledger.besu.nat.upnp.UpnpNatManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public final class DefaultP2PNetworkTest {

  final MaintainedPeers maintainedPeers = new MaintainedPeers();
  @Mock PeerDiscoveryAgent discoveryAgent;
  @Mock RlpxAgent rlpxAgent;
  @Mock NatService natService;

  private final ArgumentCaptor<PeerBondedObserver> discoverySubscriberCaptor =
      ArgumentCaptor.forClass(PeerBondedObserver.class);

  @Captor private ArgumentCaptor<Stream<? extends Peer>> peerStreamCaptor;

  private final NetworkingConfiguration config =
      NetworkingConfiguration.create()
          .setDiscovery(DiscoveryConfiguration.create().setActive(false))
          .setRlpx(
              RlpxConfiguration.create()
                  .setBindPort(0)
                  .setSupportedProtocols(MockSubProtocol.create()));

  @Before
  public void before() {
    lenient().when(rlpxAgent.start()).thenReturn(CompletableFuture.completedFuture(30303));
    lenient().when(rlpxAgent.stop()).thenReturn(CompletableFuture.completedFuture(null));
    lenient()
        .when(discoveryAgent.start(anyInt()))
        .thenAnswer(
            invocation ->
                CompletableFuture.completedFuture(invocation.getArgument(0, Integer.class)));
    lenient().when(discoveryAgent.stop()).thenReturn(CompletableFuture.completedFuture(null));
    lenient()
        .when(discoveryAgent.observePeerBondedEvents(discoverySubscriberCaptor.capture()))
        .thenReturn(1L);
  }

  @Test
  public void addMaintainConnectionPeer_newPeer() {
    final DefaultP2PNetwork network = network();
    network.start();
    final Peer peer = PeerTestHelper.createPeer();

    assertThat(network.addMaintainConnectionPeer(peer)).isTrue();

    assertThat(maintainedPeers.contains(peer)).isTrue();
    verify(rlpxAgent, times(1)).connect(peer);
  }

  @Test
  public void addMaintainConnectionPeer_existingPeer() {
    final DefaultP2PNetwork network = network();
    network.start();
    final Peer peer = PeerTestHelper.createPeer();

    assertThat(network.addMaintainConnectionPeer(peer)).isTrue();
    assertThat(network.addMaintainConnectionPeer(peer)).isFalse();
    verify(rlpxAgent, times(2)).connect(peer);
    assertThat(maintainedPeers.contains(peer)).isTrue();
  }

  @Test
  public void removeMaintainedConnectionPeer_alreadyMaintainedPeer() {
    final DefaultP2PNetwork network = network();
    network.start();
    final Peer peer = PeerTestHelper.createPeer();

    assertThat(network.addMaintainConnectionPeer(peer)).isTrue();
    assertThat(network.removeMaintainedConnectionPeer(peer)).isTrue();

    assertThat(maintainedPeers.contains(peer)).isFalse();
    verify(rlpxAgent, times(1)).connect(peer);
    verify(rlpxAgent, times(1)).disconnect(peer.getId(), DisconnectReason.REQUESTED);
    verify(discoveryAgent, times(1)).dropPeer(peer);
  }

  @Test
  public void removeMaintainedConnectionPeer_nonMaintainedPeer() {
    final DefaultP2PNetwork network = network();
    network.start();
    final Peer peer = PeerTestHelper.createPeer();

    assertThat(network.removeMaintainedConnectionPeer(peer)).isFalse();

    assertThat(maintainedPeers.contains(peer)).isFalse();
    verify(rlpxAgent, times(1)).disconnect(peer.getId(), DisconnectReason.REQUESTED);
    verify(discoveryAgent, times(1)).dropPeer(peer);
  }

  @Test
  public void checkMaintainedConnectionPeers_unconnectedPeer() {
    final DefaultP2PNetwork network = network();
    final Peer peer = PeerTestHelper.createPeer();
    maintainedPeers.add(peer);

    network.start();

    verify(rlpxAgent, times(0)).connect(peer);

    network.checkMaintainedConnectionPeers();
    verify(rlpxAgent, times(1)).connect(peer);
  }

  @Test
  public void checkMaintainedConnectionPeers_connectedPeer() {
    final DefaultP2PNetwork network = network();
    final Peer peer = PeerTestHelper.createPeer();
    maintainedPeers.add(peer);

    network.start();

    // Don't connect to an already connected peer
    final CompletableFuture<PeerConnection> connectionFuture =
        CompletableFuture.completedFuture(MockPeerConnection.create(peer));
    when(rlpxAgent.getPeerConnection(peer)).thenReturn(Optional.of(connectionFuture));
    network.checkMaintainedConnectionPeers();
    verify(rlpxAgent, times(0)).connect(peer);
  }

  @Test
  public void checkMaintainedConnectionPeers_connectingPeer() {
    final DefaultP2PNetwork network = network();
    final Peer peer = PeerTestHelper.createPeer();
    maintainedPeers.add(peer);

    network.start();

    // Don't connect when connection is already pending.
    final CompletableFuture<PeerConnection> connectionFuture = new CompletableFuture<>();
    when(rlpxAgent.getPeerConnection(peer)).thenReturn(Optional.of(connectionFuture));
    network.checkMaintainedConnectionPeers();
    verify(rlpxAgent, times(0)).connect(peer);
  }

  @Test
  public void beforeStartingNetworkEnodeURLShouldNotBePresent() {
    final P2PNetwork network = network();

    Assertions.assertThat(network.getLocalEnode()).isNotPresent();
  }

  @Test
  public void afterStartingNetworkEnodeURLShouldBePresent() {
    final P2PNetwork network = network();
    network.start();

    Assertions.assertThat(network.getLocalEnode()).isPresent();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void start_withNatManager() {
    final String externalIp = "127.0.0.3";
    config.getRlpx().setBindPort(30303);
    config.getDiscovery().setBindPort(30301);

    UpnpNatManager upnpNatManager = mock(UpnpNatManager.class);

    when(natService.getNatManager()).thenReturn(Optional.of(upnpNatManager));

    when(natService.getNatMethod()).thenReturn(NatMethod.UPNP);

    when(natService.queryExternalIPAddress()).thenReturn(Optional.of(externalIp));

    doAnswer(
            answer -> {
              var action = (Consumer<NatService>) answer.getArgument(0);
              action.accept(natService);
              return null;
            })
        .when(natService)
        .ifNatEnvironment(any(Consumer.class));

    final P2PNetwork network = builder().natService(natService).build();

    network.start();
    verify(upnpNatManager)
        .requestPortForward(eq(config.getRlpx().getBindPort()), eq(NetworkProtocol.TCP), any());
    verify(upnpNatManager)
        .requestPortForward(
            eq(config.getDiscovery().getBindPort()), eq(NetworkProtocol.UDP), any());

    Assertions.assertThat(network.getLocalEnode().get().getIpAsString()).isEqualTo(externalIp);
  }

  @Test
  public void handlePeerBondedEvent_forListeningPeer() {
    final DefaultP2PNetwork network = network();
    network.start();
    final DiscoveryPeer peer = DiscoveryPeer.fromEnode(PeerTestHelper.enode());
    final PeerDiscoveryEvent.PeerBondedEvent peerBondedEvent =
        new PeerDiscoveryEvent.PeerBondedEvent(peer, System.currentTimeMillis());

    discoverySubscriberCaptor.getValue().onPeerBonded(peerBondedEvent);
    verify(rlpxAgent, times(1)).connect(peer);
  }

  @Test
  public void handlePeerBondedEvent_forPeerWithNoTcpPort() {
    final DefaultP2PNetwork network = network();
    network.start();
    final DiscoveryPeer peer =
        DiscoveryPeer.fromIdAndEndpoint(
            Peer.randomId(), new Endpoint("127.0.0.1", 999, OptionalInt.empty()));
    final PeerDiscoveryEvent.PeerBondedEvent peerBondedEvent =
        new PeerDiscoveryEvent.PeerBondedEvent(peer, System.currentTimeMillis());

    discoverySubscriberCaptor.getValue().onPeerBonded(peerBondedEvent);
    verify(rlpxAgent, times(1)).connect(peer);
  }

  @Test
  public void attemptPeerConnections_bondedPeers() {
    final DiscoveryPeer discoPeer = DiscoveryPeer.fromEnode(PeerTestHelper.enode());
    discoPeer.setStatus(PeerDiscoveryStatus.BONDED);
    final Stream<DiscoveryPeer> peerStream = Stream.of(discoPeer);
    when(discoveryAgent.streamDiscoveredPeers()).thenReturn(peerStream);

    final DefaultP2PNetwork network = network();
    network.attemptPeerConnections();
    verify(rlpxAgent, times(1)).connect(peerStreamCaptor.capture());

    List<? extends Peer> capturedPeers = peerStreamCaptor.getValue().collect(Collectors.toList());
    assertThat(capturedPeers.contains(discoPeer)).isTrue();
    assertThat(capturedPeers.size()).isEqualTo(1);
  }

  @Test
  public void attemptPeerConnections_unbondedPeers() {
    final DiscoveryPeer discoPeer = DiscoveryPeer.fromEnode(PeerTestHelper.enode());
    discoPeer.setStatus(PeerDiscoveryStatus.KNOWN);
    final Stream<DiscoveryPeer> peerStream = Stream.of(discoPeer);
    when(discoveryAgent.streamDiscoveredPeers()).thenReturn(peerStream);

    final DefaultP2PNetwork network = network();
    network.attemptPeerConnections();
    verify(rlpxAgent, times(1)).connect(peerStreamCaptor.capture());

    List<? extends Peer> capturedPeers = peerStreamCaptor.getValue().collect(Collectors.toList());
    assertThat(capturedPeers.contains(discoPeer)).isFalse();
    assertThat(capturedPeers.size()).isEqualTo(0);
  }

  @Test
  public void attemptPeerConnections_sortsPeersByLastContacted() {
    final List<DiscoveryPeer> discoPeers = new ArrayList<>();
    discoPeers.add(DiscoveryPeer.fromEnode(PeerTestHelper.enode()));
    discoPeers.add(DiscoveryPeer.fromEnode(PeerTestHelper.enode()));
    discoPeers.add(DiscoveryPeer.fromEnode(PeerTestHelper.enode()));
    discoPeers.forEach(p -> p.setStatus(PeerDiscoveryStatus.BONDED));
    discoPeers.get(0).setLastAttemptedConnection(10);
    discoPeers.get(2).setLastAttemptedConnection(15);
    when(discoveryAgent.streamDiscoveredPeers()).thenReturn(discoPeers.stream());

    final DefaultP2PNetwork network = network();
    network.attemptPeerConnections();
    verify(rlpxAgent, times(1)).connect(peerStreamCaptor.capture());

    List<? extends Peer> capturedPeers = peerStreamCaptor.getValue().collect(Collectors.toList());
    assertThat(capturedPeers.size()).isEqualTo(3);
    assertThat(capturedPeers.get(0)).isEqualTo(discoPeers.get(1));
    assertThat(capturedPeers.get(1)).isEqualTo(discoPeers.get(0));
    assertThat(capturedPeers.get(2)).isEqualTo(discoPeers.get(2));
  }

  private DefaultP2PNetwork network() {
    return (DefaultP2PNetwork) builder().build();
  }

  private DefaultP2PNetwork.Builder builder() {

    final KeyPair keyPair = KeyPair.generate();

    return DefaultP2PNetwork.builder()
        .config(config)
        .peerDiscoveryAgent(discoveryAgent)
        .rlpxAgent(rlpxAgent)
        .keyPair(keyPair)
        .maintainedPeers(maintainedPeers)
        .metricsSystem(new NoOpMetricsSystem())
        .supportedCapabilities(Capability.create("eth", 63));
  }
}
