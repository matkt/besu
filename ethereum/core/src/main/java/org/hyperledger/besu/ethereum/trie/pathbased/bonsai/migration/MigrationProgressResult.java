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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Wire shape of {@code debug_migrationProgress}, matching geth's PascalCase encoding of {@code
 * core.MigrationProgress}.
 */
@JsonPropertyOrder({"Phase", "Binary", "Merkle"})
@JsonInclude(JsonInclude.Include.ALWAYS)
public class MigrationProgressResult {

  private final String phase;
  private final DirectionProgressResult binary;
  private final DirectionProgressResult merkle;

  public MigrationProgressResult(
      final String phase,
      final DirectionProgressResult binary,
      final DirectionProgressResult merkle) {
    this.phase = phase;
    this.binary = binary;
    this.merkle = merkle;
  }

  public static MigrationProgressResult inactive() {
    return new MigrationProgressResult("inactive", null, null);
  }

  public static MigrationProgressResult done() {
    return new MigrationProgressResult("done", null, null);
  }

  public static MigrationProgressResult running(final DirectionProgressResult binary) {
    return new MigrationProgressResult("running", binary, null);
  }

  @JsonProperty("Phase")
  public String getPhase() {
    return phase;
  }

  @JsonProperty("Binary")
  public DirectionProgressResult getBinary() {
    return binary;
  }

  @JsonProperty("Merkle")
  public DirectionProgressResult getMerkle() {
    return merkle;
  }
}
