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
package org.hyperledger.besu.crypto;

import org.apache.tuweni.bytes.Bytes;

/**
 * Verifies leanSPHINCS signatures, the stateless hash-based scheme over Keccak-256 specified by the
 * Cryptography/Sphincs module of ethereum/EthLib.
 */
public interface LeanSphincsVerifier {

  /** Size of a public key: PK.seed then PK.root, 16 bytes each. */
  int PUBLIC_KEY_SIZE = 32;

  /** Size of a signed message. */
  int MESSAGE_SIZE = 32;

  /** Size of a signature. */
  int SIGNATURE_SIZE = 7740;

  /**
   * Checks a signature of a message under a public key.
   *
   * @param publicKey the public key
   * @param message the signed message
   * @param signature the signature
   * @return true if the signature verifies; false otherwise, including for an input of another size
   */
  boolean verify(Bytes publicKey, Bytes message, Bytes signature);
}
