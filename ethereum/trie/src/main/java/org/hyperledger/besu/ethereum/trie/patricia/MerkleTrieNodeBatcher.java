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
package org.hyperledger.besu.ethereum.trie.patricia;

import org.hyperledger.besu.ethereum.trie.Node;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;

/**
 * Processes batches of trie nodes for efficient hashing.
 *
 * <p>This class manages the batching and hashing of trie nodes to optimize performance.
 */
@SuppressWarnings("unused")
public class MerkleTrieNodeBatcher<V> {

  private final Map<Bytes, Node<V>> updatedNodes = new HashMap<>();

  /**
   * Adds a node for future batching.
   *
   * @param location location of the node
   * @param node The node to add.
   */
  public void addNodeToBatch(final Bytes location, final Node<V> node) {
    updatedNodes.put(location, node);
  }

  /**
   * Processes the nodes in batches. Sorts the nodes by their location and hashes them in batches.
   * Clears the batch after processing.
   */
  public void runAsyncComputeStateRoot() {
    if (updatedNodes.isEmpty()) {
      return;
    }

    final Map<Bytes, Node<V>> sortedMap =
        updatedNodes.entrySet().stream()
            .sorted(
                (entry1, entry2) -> Integer.compare(entry2.getKey().size(), entry1.getKey().size()))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

    while (!sortedMap.isEmpty()) {
      final Map<Bytes, Node<V>> independentNodes = new HashMap<>();
      for (Map.Entry<Bytes, Node<V>> entry : sortedMap.entrySet()) {
        final Bytes location = entry.getKey();
        final Node<V> node = entry.getValue();
        final boolean matchFound =
            independentNodes.entrySet().stream()
                .anyMatch(
                    e ->
                        e.getKey().size() > location.size()
                            && e.getKey().commonPrefixLength(location) > 0);
        if (!matchFound) {
          independentNodes.put(location, node);
        }
      }
      if (!independentNodes.isEmpty()) {
        processBatch(independentNodes.values());
      }
      independentNodes.forEach((bytes, vNode) -> sortedMap.remove(bytes));
    }
  }

  private void processBatch(final Collection<Node<V>> nodes) {
    if (nodes.size() > 25) {
      nodes.parallelStream()
          .forEach(
              vNode -> {
                vNode.getEncodedBytes();
                vNode.getHash();
              });
    } else {
      nodes.forEach(
          vNode -> {
            vNode.getEncodedBytes();
            vNode.getHash();
          });
    }
  }

  private void calculateRootInternalNodeHash(final Node<V> root) {
    root.getEncodedBytesRef();
  }
}
