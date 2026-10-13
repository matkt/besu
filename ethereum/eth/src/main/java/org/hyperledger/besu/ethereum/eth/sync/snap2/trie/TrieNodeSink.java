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
package org.hyperledger.besu.ethereum.eth.sync.snap2.trie;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Receives the hashed trie nodes produced while building a trie. */
@FunctionalInterface
interface TrieNodeSink {

  /**
   * Called once per node referenced by hash, children before parents.
   *
   * @param location the nibble path of the node, one nibble per byte
   * @param hash the keccak hash of the node
   * @param rlp the node encoding
   */
  void onNode(Bytes location, Bytes32 hash, Bytes rlp);
}
