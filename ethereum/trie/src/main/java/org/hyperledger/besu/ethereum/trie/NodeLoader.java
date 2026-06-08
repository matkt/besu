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
package org.hyperledger.besu.ethereum.trie;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public interface NodeLoader {
  Optional<Bytes> getNode(Bytes location, Bytes32 hash);

  default Optional<LoadedNode> getNodeWithSource(
      final Bytes location, final Bytes32 hash, final NodeSource preferredSource) {
    return getNode(location, hash).map(bytes -> new LoadedNode(bytes, NodeSource.UNKNOWN));
  }

  enum NodeSource {
    UNKNOWN,
    HOT,
    COLD
  }

  class LoadedNode {
    private final Bytes bytes;
    private final NodeSource source;

    public LoadedNode(final Bytes bytes, final NodeSource source) {
      this.bytes = bytes;
      this.source = source;
    }

    public Bytes getBytes() {
      return bytes;
    }

    public NodeSource getSource() {
      return source;
    }
  }
}
