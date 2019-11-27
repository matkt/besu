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
package org.hyperledger.besu.nat.core;

import static com.google.common.base.Preconditions.checkState;

import org.hyperledger.besu.nat.NatMethod;
import org.hyperledger.besu.nat.core.domain.NatPortMapping;
import org.hyperledger.besu.nat.core.domain.NatServiceType;
import org.hyperledger.besu.nat.core.domain.NetworkProtocol;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * This class describes the behaviour of any supported NAT manager. Internal API to support Network
 * Address Translation (NAT) technologies in Besu.
 */
public interface NatManager {

  int TIMEOUT_SECONDS = 60;

  /**
   * Returns the NAT method associated to this manager.
   *
   * @return the {@link NatMethod}
   */
  NatMethod getNatMethod();

  /** Starts the manager or service. */
  void start();

  /** Stops the manager or service. */
  void stop();

  /**
   * Returns whether or not the manager is started.
   *
   * @return true if started, false otherwise.
   */
  boolean isStarted();

  /**
   * Checks if the manager is started and throws an {@link IllegalStateException} in case it is not
   * started. Convenient method to perform actions only if service is started.
   */
  default void requireManagerStarted() {
    checkState(isStarted(), "NAT manager must be started.");
  }

  /**
   * Returns a {@link java.util.concurrent.Future} wrapping the local IP address.
   *
   * @return The local IP address wrapped in a {@link java.util.concurrent.Future}.
   */
  CompletableFuture<String> queryLocalIPAddress();

  /**
   * Returns a {@link java.util.concurrent.Future} wrapping the external IP address.
   *
   * @return The external IP address wrapped in a {@link java.util.concurrent.Future}.
   */
  CompletableFuture<String> queryExternalIPAddress();

  /**
   * Returns all known port mappings.
   *
   * @return The known port mappings wrapped in a {@link java.util.concurrent.Future}.
   */
  CompletableFuture<List<NatPortMapping>> getPortMappings();

  /**
   * Returns the port mapping associated to the passed service type.
   *
   * @param serviceType The service type {@link NatServiceType}.
   * @param networkProtocol The network protocol {@link NetworkProtocol}.
   * @return The port mapping {@link NatPortMapping}
   */
  NatPortMapping getPortMapping(
      final NatServiceType serviceType, final NetworkProtocol networkProtocol);
}
