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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact;

import org.hyperledger.besu.ethereum.partitionedbinarytrie.params.EmbeddingParameters;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * One PBT leaf derived from an EIP-8347 typed snapshot: full tree key and left-padded 32-byte
 * value.
 *
 * <p>The on-disk snapshot packs leaves into typed, stem-grouped records (EIP PR 12379); readers
 * expand those records into {@code Eip8347SnapshotLeaf} instances before hashing.
 */
@SuppressWarnings("MethodInputParametersMustBeFinal") // compact record constructor
public record Eip8347SnapshotLeaf(Bytes key, Bytes32 value) {

  public Eip8347SnapshotLeaf {
    if (key == null || key.isEmpty()) {
      throw new Eip8347ArtifactVerificationException("snapshot leaf key must be non-empty");
    }
    if (value == null) {
      throw new Eip8347ArtifactVerificationException("snapshot leaf value must be present");
    }
    validateKeyLength(key);
    if (value.isZero()) {
      throw new Eip8347ArtifactVerificationException(
          "snapshot must not contain a zero-valued leaf (EIP-8297 absence rule)");
    }
  }

  static void validateKeyLength(final Bytes key) {
    final int zone = key.get(0) & 0xFF;
    final int expected =
        switch (zone) {
          case EmbeddingParameters.ACCOUNT_ZONE -> EmbeddingParameters.ACCOUNT_KEY_LENGTH;
          case EmbeddingParameters.CODE_ZONE -> EmbeddingParameters.CODE_KEY_LENGTH;
          case EmbeddingParameters.STORAGE_ZONE -> EmbeddingParameters.STORAGE_KEY_LENGTH;
          default -> -1;
        };
    if (expected < 0) {
      throw new Eip8347ArtifactVerificationException(
          "snapshot leaf has reserved zone byte 0x" + Integer.toHexString(zone));
    }
    if (key.size() != expected) {
      throw new Eip8347ArtifactVerificationException(
          "snapshot leaf key length "
              + key.size()
              + " disagrees with zone 0x"
              + Integer.toHexString(zone)
              + " (expected "
              + expected
              + ")");
    }
  }
}
