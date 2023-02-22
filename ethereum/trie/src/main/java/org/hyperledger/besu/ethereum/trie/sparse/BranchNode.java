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
package org.hyperledger.besu.ethereum.trie.sparse;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.ethereum.trie.Node;
import org.hyperledger.besu.ethereum.trie.NodeFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class BranchNode<V> extends org.hyperledger.besu.ethereum.trie.patricia.BranchNode<V> implements Node<V> {


  public BranchNode(final Bytes location, final ArrayList<Node<V>> children, final Optional<V> value, final NodeFactory<V> nodeFactory, final Function<V, Bytes> valueSerializer) {
    super(location, children, value, nodeFactory, valueSerializer);
  }

  public BranchNode(final List<Node<V>> children, final Optional<V> value, final NodeFactory<V> nodeFactory, final Function<V, Bytes> valueSerializer) {
    super(children, value, nodeFactory, valueSerializer);
  }

  @Override
  public int maxChild(){
    return 2;
  }
}
