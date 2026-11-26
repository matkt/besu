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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.ethereum.leanpq.SphincsSecretKey;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Checks the lean-pq wrapper with a signature that lean-pq makes; leanVM tests lean-pq itself. */
class NativeLeanSphincsVerifierTest {

  private static final Bytes32 SEED = Bytes32.fromHexString("0x" + "07".repeat(32));
  private static final Bytes32 MESSAGE = Bytes32.fromHexString("0x" + "42".repeat(32));

  private static Bytes publicKey;
  private static Bytes signature;

  private final LeanSphincsVerifier verifier = new NativeLeanSphincsVerifier();

  // Signing is deterministic, so the same seed and message give the same signature on every run.
  @BeforeAll
  static void sign() {
    try (SphincsSecretKey key = SphincsSecretKey.fromSeed(SEED.toArrayUnsafe())) {
      publicKey = Bytes.wrap(key.publicKey());
      signature = Bytes.wrap(key.sign(MESSAGE.toArrayUnsafe()));
    }
  }

  @Test
  void signatureVerifies() {
    assertThat(verifier.verify(publicKey, MESSAGE, signature)).isTrue();
  }

  // The first byte is the randomizer and the last one ends the top layer's path.
  @ParameterizedTest
  @ValueSource(ints = {0, LeanSphincsVerifier.SIGNATURE_SIZE - 1})
  void flippedSignatureBitIsRejected(final int offset) {
    assertThat(verifier.verify(publicKey, MESSAGE, flipLowBit(signature, offset))).isFalse();
  }

  @Test
  void otherMessageIsRejected() {
    assertThat(verifier.verify(publicKey, flipLowBit(MESSAGE, 0), signature)).isFalse();
  }

  // The first half of the key is PK.seed, the second half PK.root.
  @ParameterizedTest
  @ValueSource(ints = {0, LeanSphincsVerifier.PUBLIC_KEY_SIZE / 2})
  void otherPublicKeyIsRejected(final int offset) {
    assertThat(verifier.verify(flipLowBit(publicKey, offset), MESSAGE, signature)).isFalse();
  }

  @Test
  void inputOfAnotherSizeIsRejected() {
    final Bytes extraByte = Bytes.of(0);

    assertThat(verifier.verify(publicKey.slice(1), MESSAGE, signature)).isFalse();
    assertThat(verifier.verify(Bytes.concatenate(publicKey, extraByte), MESSAGE, signature))
        .isFalse();
    assertThat(verifier.verify(publicKey, MESSAGE.slice(1), signature)).isFalse();
    assertThat(verifier.verify(publicKey, Bytes.concatenate(MESSAGE, extraByte), signature))
        .isFalse();
    assertThat(verifier.verify(publicKey, MESSAGE, signature.slice(1))).isFalse();
    assertThat(verifier.verify(publicKey, MESSAGE, Bytes.concatenate(signature, extraByte)))
        .isFalse();
  }

  private static Bytes flipLowBit(final Bytes bytes, final int offset) {
    final byte[] flipped = bytes.toArray();
    flipped[offset] ^= 0x01;
    return Bytes.wrap(flipped);
  }
}
