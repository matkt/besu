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

package org.hyperledger.besu.nat.docker;

import org.hyperledger.besu.nat.NatMethod;
import org.hyperledger.besu.nat.core.AbstractNatManager;
import org.hyperledger.besu.nat.core.domain.NatPortMapping;
import org.hyperledger.besu.nat.core.domain.NatServiceType;
import org.hyperledger.besu.nat.core.domain.NetworkProtocol;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DockerNatManager extends AbstractNatManager {
  protected static final Logger LOG = LogManager.getLogger();

  private static final String HOSTNAME = "HOST_IP";

  private static final String PORT_MAPPING_TAG = "HOST_PORT_";

  private final int internalP2pPort;
  private final int internalRpcHttpPort;
  private final List<NatPortMapping> forwardedPorts;

  public DockerNatManager(final int p2pPort, final int rpcHttpPort) {
    super(NatMethod.DOCKER);
    this.internalP2pPort = p2pPort;
    this.internalRpcHttpPort = rpcHttpPort;
    forwardedPorts = buildForwardedPorts();
  }

  private List<NatPortMapping> buildForwardedPorts() {
    try {
      final String internalHost = queryLocalIPAddress().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      final String advertisedHost =
          retrieveExternalIPAddress().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      return Arrays.asList(
          new NatPortMapping(
              NatServiceType.DISCOVERY,
              NetworkProtocol.UDP,
              internalHost,
              advertisedHost,
              internalP2pPort,
              getExternalPort(internalP2pPort)),
          new NatPortMapping(
              NatServiceType.RLPX,
              NetworkProtocol.TCP,
              internalHost,
              advertisedHost,
              internalP2pPort,
              getExternalPort(internalP2pPort)),
          new NatPortMapping(
              NatServiceType.JSON_RPC,
              NetworkProtocol.TCP,
              internalHost,
              advertisedHost,
              internalRpcHttpPort,
              getExternalPort(internalRpcHttpPort)));
    } catch (Exception e) {
      LOG.warn("Failed to create forwarded port list", e);
    }
    return Collections.emptyList();
  }

  @Override
  protected void doStart() {
    LOG.info("Starting docker NAT manager.");
  }

  @Override
  protected void doStop() {
    LOG.info("Stopping docker NAT manager.");
  }

  @Override
  protected CompletableFuture<String> retrieveExternalIPAddress() {
    try {
      return CompletableFuture.completedFuture(InetAddress.getByName(HOSTNAME).getHostAddress());
    } catch (UnknownHostException e) {
      return CompletableFuture.failedFuture(new RuntimeException("Cannot detect IP."));
    }
  }

  @Override
  public CompletableFuture<List<NatPortMapping>> getPortMappings() {
    return CompletableFuture.completedFuture(forwardedPorts);
  }

  private int getExternalPort(final int defaultValue) {
    return Optional.ofNullable(System.getenv(PORT_MAPPING_TAG + defaultValue))
        .map(Integer::valueOf)
        .orElse(defaultValue);
  }
}
