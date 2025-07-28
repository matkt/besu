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
package org.hyperledger.besu.ethereum.core.witness;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;

public class ExecutionWitness {

  private final StateDiff stateDiff;
  private final BinTrieProof binTrieProof;
  private final Hash parentStateRoot;

  @VisibleForTesting
  public ExecutionWitness() {
    this.stateDiff = null;
    this.binTrieProof = null;
    this.parentStateRoot = null;
  }

  public ExecutionWitness(
      final StateDiff stateDiff, final BinTrieProof binTrieProof, final Hash parentStateRoot) {
    this.stateDiff = stateDiff;
    this.binTrieProof = binTrieProof;
    this.parentStateRoot = parentStateRoot;
  }

  @SuppressWarnings("unused")
  public static ExecutionWitness readFrom(final RLPInput input) {
    return new ExecutionWitness();
  }

  @Override
  public String toString() {
    return "ExecutionWitness{"
        + "stateDiff="
        + stateDiff
        + ", binTrieProof="
        + binTrieProof
        + ", parentStateRoot="
        + parentStateRoot
        + '}';
  }

  public StateDiff getStateDiff() {
    return stateDiff;
  }

  public BinTrieProof getBinTrieProof() {
    return binTrieProof;
  }

  public Hash getParentStateRoot() {
    return parentStateRoot;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExecutionWitness that = (ExecutionWitness) o;
    return Objects.equals(stateDiff, that.stateDiff)
        && Objects.equals(binTrieProof, that.binTrieProof)
        && Objects.equals(parentStateRoot, that.parentStateRoot);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stateDiff, binTrieProof, parentStateRoot);
  }
}
