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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    final List<Map.Entry<Bytes, Node<V>>> sortedNodesByLocation =
        new ArrayList<>(updatedNodes.entrySet());
    sortedNodesByLocation.sort(
        (entry1, entry2) -> Integer.compare(entry2.getKey().size(), entry1.getKey().size()));

    int currentDepth = -1; // Tracks the depth of the current batch

    final List<Node<V>> nodesInSameLevel = new ArrayList<>();
    for (Map.Entry<Bytes, Node<V>> entry : sortedNodesByLocation) {
      final Bytes location = entry.getKey();
      final Node<V> node = entry.getValue();
      if (location.size() != currentDepth) {
        if (!nodesInSameLevel.isEmpty()) {
          processBatch(nodesInSameLevel);
          nodesInSameLevel.clear();
        }
        if (location.isEmpty()) {
          // We will end up updating the root node. Once all the batching is finished,
          // we will update the previous states of the nodes by setting them to the new ones.
          calculateRootInternalNodeHash(node);
          updatedNodes.clear();
          return;
        }
        currentDepth = location.size();
      }
      if (node.isDirty()) {
        nodesInSameLevel.add(node);
      }
    }
    throw new IllegalStateException("root node not found");
  }

  private void processBatch(final List<Node<V>> nodes) {
    nodes.parallelStream()
        .forEach(
            vNode -> {
              vNode.getEncodedBytes();
              vNode.getHash();
            });
  }

  private void calculateRootInternalNodeHash(final Node<V> root) {
    root.getEncodedBytesRef();
  }
}
