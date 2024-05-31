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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.verkle.ExecutionWitness;
import org.hyperledger.besu.ethereum.trie.verkle.proof.ProofVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ExecutionWitnessValidator {

  boolean validateExecutionWitness(
      final long blockNumber,
      final Optional<BlockHeader> maybeParentHeader,
      Optional<ExecutionWitness> executionWitness);

  class ProhibitedExecutionWitness implements ExecutionWitnessValidator {

    private static final Logger LOG = LoggerFactory.getLogger(ProhibitedExecutionWitness.class);

    @Override
    public boolean validateExecutionWitness(
        final long blockNumber,
        final Optional<BlockHeader> maybeParentHeader,
        final Optional<ExecutionWitness> executionWitness) {
      final boolean isValid = executionWitness.isEmpty();
      if (!isValid) {
        LOG.warn(
            "ExecutionWitness must be empty when ExecutionWitness are prohibited but were: {}",
            executionWitness);
      }
      return isValid;
    }
  }

  class AllowedExecutionWitness implements ExecutionWitnessValidator {

    private static final Logger LOG = LoggerFactory.getLogger(AllowedExecutionWitness.class);

    private static final ProofVerifier VERKLE_PROOF_VERIFIER = new ProofVerifier();

    @Override
    public boolean validateExecutionWitness(
        final long blockNumber,
        final Optional<BlockHeader> maybeParentHeader,
        final Optional<ExecutionWitness> maybeExecutionWitness) {
      if (blockNumber == 0) {
        LOG.warn("Ignore execution witness verification for genesis block");
        return true;
      } else {
        if (maybeParentHeader.isEmpty()) {
          LOG.warn("Failed to verify execution witness because parent header is not available");
          return false;
        }
      }
      if (maybeExecutionWitness.isEmpty()) {
        LOG.warn(
            "ExecutionWitness must not be empty when execution witness verification is activated");
        return false;
      }

      final ExecutionWitness executionWitness = maybeExecutionWitness.get();
      final List<Bytes> keys = new ArrayList<>();
      final List<Bytes> values = new ArrayList<>();
      executionWitness
          .getStateDiff()
          .stemStateDiff()
          .forEach(
              stateDiff ->
                  stateDiff
                      .suffixDiffs()
                      .forEach(
                          suffixDiff -> {
                            keys.add(
                                Bytes.concatenate(stateDiff.stem(), Bytes.of(suffixDiff.suffix())));
                            values.add(suffixDiff.currentValue());
                          }));
      System.out.println();
      return VERKLE_PROOF_VERIFIER.verifyVerkleProof(
          keys,
          values,
          executionWitness.getVerkleProof().commitmentsByPath(),
          executionWitness.getVerkleProof().ipaProof().cl(),
          executionWitness.getVerkleProof().ipaProof().cr(),
          executionWitness.getVerkleProof().otherStems(),
          executionWitness.getVerkleProof().d(),
          executionWitness.getVerkleProof().depthExtensionPresent(),
          executionWitness.getVerkleProof().ipaProof().finalEvaluation(),
          maybeParentHeader.get().getStateRoot());
    }
  }
}
