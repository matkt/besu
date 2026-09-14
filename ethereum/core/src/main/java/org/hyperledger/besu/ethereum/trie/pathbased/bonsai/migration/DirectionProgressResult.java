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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Wire shape of one migration direction, matching geth's PascalCase encoding of {@code
 * core.DirectionProgress}.
 */
@JsonPropertyOrder({"Phase", "Cursor", "CursorHash", "ShadowRoot", "Error"})
public class DirectionProgressResult {

  private final String phase;
  private final long cursor;
  private final String cursorHash;
  private final String shadowRoot;
  private final String error;

  public DirectionProgressResult(
      final String phase,
      final long cursor,
      final String cursorHash,
      final String shadowRoot,
      final String error) {
    this.phase = phase;
    this.cursor = cursor;
    this.cursorHash = cursorHash;
    this.shadowRoot = shadowRoot;
    this.error = error;
  }

  public static DirectionProgressResult idle() {
    return new DirectionProgressResult("idle", 0L, "", "", "");
  }

  @JsonProperty("Phase")
  public String getPhase() {
    return phase;
  }

  @JsonProperty("Cursor")
  public long getCursor() {
    return cursor;
  }

  @JsonProperty("CursorHash")
  public String getCursorHash() {
    return cursorHash;
  }

  @JsonProperty("ShadowRoot")
  public String getShadowRoot() {
    return shadowRoot;
  }

  @JsonProperty("Error")
  public String getError() {
    return error;
  }
}
