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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Checks the lean-pq wrapper against EthLib's known answer, described in the README next to it. */
class NativeLeanSphincsVerifierTest {

  private static Bytes publicKey;
  private static Bytes message;
  private static Bytes signature;

  private final LeanSphincsVerifier verifier = new NativeLeanSphincsVerifier();

  @BeforeAll
  static void readKnownAnswer() throws IOException, URISyntaxException {
    final Map<String, String> fields = readFields("leansphincs/verify_valid_known_answer.yaml");
    assertThat(fields).containsEntry("output", "true");
    publicKey = Bytes.fromHexString(fields.get("public_key"));
    message = Bytes.fromHexString(fields.get("message"));
    signature = Bytes.fromHexString(fields.get("signature"));
  }

  @Test
  void knownAnswerVerifies() {
    assertThat(verifier.verify(publicKey, message, signature)).isTrue();
  }

  // The first byte is the randomizer and the last one ends the top layer's path.
  @ParameterizedTest
  @ValueSource(ints = {0, LeanSphincsVerifier.SIGNATURE_SIZE - 1})
  void flippedSignatureBitIsRejected(final int offset) {
    assertThat(verifier.verify(publicKey, message, flipLowBit(signature, offset))).isFalse();
  }

  @Test
  void otherMessageIsRejected() {
    assertThat(verifier.verify(publicKey, flipLowBit(message, 0), signature)).isFalse();
  }

  // The first half of the key is PK.seed, the second half PK.root.
  @ParameterizedTest
  @ValueSource(ints = {0, LeanSphincsVerifier.PUBLIC_KEY_SIZE / 2})
  void otherPublicKeyIsRejected(final int offset) {
    assertThat(verifier.verify(flipLowBit(publicKey, offset), message, signature)).isFalse();
  }

  @Test
  void inputOfAnotherSizeIsRejected() {
    final Bytes extraByte = Bytes.of(0);

    assertThat(verifier.verify(publicKey.slice(1), message, signature)).isFalse();
    assertThat(verifier.verify(Bytes.concatenate(publicKey, extraByte), message, signature))
        .isFalse();
    assertThat(verifier.verify(publicKey, message.slice(1), signature)).isFalse();
    assertThat(verifier.verify(publicKey, Bytes.concatenate(message, extraByte), signature))
        .isFalse();
    assertThat(verifier.verify(publicKey, message, signature.slice(1))).isFalse();
    assertThat(verifier.verify(publicKey, message, Bytes.concatenate(signature, extraByte)))
        .isFalse();
  }

  private static Bytes flipLowBit(final Bytes bytes, final int offset) {
    final byte[] flipped = bytes.toArray();
    flipped[offset] ^= 0x01;
    return Bytes.wrap(flipped);
  }

  // Each line of the vector is "key: value", with hex values in single quotes.
  private static Map<String, String> readFields(final String resource)
      throws IOException, URISyntaxException {
    final Path file = Path.of(NativeLeanSphincsVerifierTest.class.getResource(resource).toURI());
    final Map<String, String> fields = new HashMap<>();
    for (final String line : Files.readAllLines(file)) {
      final int colon = line.indexOf(':');
      final String value = line.substring(colon + 1).trim();
      if (colon > 0 && !value.isEmpty()) {
        fields.put(line.substring(0, colon).trim(), value.replace("'", ""));
      }
    }
    return fields;
  }
}
