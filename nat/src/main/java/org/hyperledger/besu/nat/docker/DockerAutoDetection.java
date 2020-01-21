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
import org.hyperledger.besu.nat.core.AutoDetectionResult;
import org.hyperledger.besu.nat.core.NatMethodAutoDetection;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class DockerAutoDetection implements NatMethodAutoDetection {

  @Override
  public AutoDetectionResult shouldBeThisNatMethod() {
    try (Stream<String> stream = Files.lines(Paths.get("/proc/1/cgroup"))) {
      return new AutoDetectionResult(
          NatMethod.DOCKER, stream.anyMatch(line -> line.contains("/docker")));
    } catch (IOException e) {
      return new AutoDetectionResult(NatMethod.DOCKER, false);
    }
  }
}
