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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public interface NodeLoader {
  Optional<Bytes> getNode(Bytes location, Bytes32 hash);

  /**
   * Batch variant of {@link #getNode}. The default loops over {@link #getNode}; loaders backed by a
   * store supporting batched reads (e.g. a RocksDB MultiGet) should override it.
   *
   * @param locations the node locations
   * @param hashes the expected node hashes, aligned with {@code locations}
   * @return the encoded nodes, in request order
   */
  default List<Optional<Bytes>> getNodes(final List<Bytes> locations, final List<Bytes32> hashes) {
    final List<Optional<Bytes>> nodes = new ArrayList<>(locations.size());
    for (int i = 0; i < locations.size(); i++) {
      nodes.add(getNode(locations.get(i), hashes.get(i)));
    }
    return nodes;
  }
}
