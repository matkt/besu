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
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.ethereum.leanpq.SphincsSecretKey;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Runs the EthLib leanSPHINCS vectors, described in the README next to them, against lean-pq. */
class NativeLeanSphincsVerifierTest {

  private final LeanSphincsVerifier verifier = new NativeLeanSphincsVerifier();

  /** One case: its directory name and the fields of its data.yaml. */
  record Vector(String name, Map<String, String> fields) {

    Bytes bytes(final String field) {
      return Bytes.fromHexString(fields.get(field));
    }

    String output() {
      return fields.get("output");
    }

    Arguments arguments() {
      return Arguments.of(name, this);
    }
  }

  // A null output marks an input of the wrong size, which verify rejects by returning false.
  static Stream<Arguments> verifyCases() throws IOException, URISyntaxException {
    return vectors("verify").map(Vector::arguments);
  }

  // Rejected seeds and messages only exercise lean-pq's argument checks, so they are skipped.
  static Stream<Arguments> keyGenCases() throws IOException, URISyntaxException {
    return vectors("key_gen").filter(v -> !v.output().equals("null")).map(Vector::arguments);
  }

  static Stream<Arguments> signCases() throws IOException, URISyntaxException {
    return vectors("sign").filter(v -> !v.output().equals("null")).map(Vector::arguments);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("verifyCases")
  void verifyMatchesEthLib(final String name, final Vector vector) {
    final boolean verified =
        verifier.verify(
            vector.bytes("public_key"), vector.bytes("message"), vector.bytes("signature"));

    assertThat(verified).isEqualTo(vector.output().equals("true"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("keyGenCases")
  void keyGenMatchesEthLib(final String name, final Vector vector) {
    try (SphincsSecretKey key = SphincsSecretKey.fromSeed(vector.bytes("seed").toArrayUnsafe())) {
      assertThat(Bytes.wrap(key.publicKey())).isEqualTo(vector.bytes("output"));
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("signCases")
  void signMatchesEthLib(final String name, final Vector vector) {
    try (SphincsSecretKey key = SphincsSecretKey.fromSeed(vector.bytes("seed").toArrayUnsafe())) {
      final byte[] signature = key.sign(vector.bytes("message").toArrayUnsafe());

      assertThat(Bytes.wrap(signature)).isEqualTo(vector.bytes("output"));
    }
  }

  private static Stream<Vector> vectors(final String operation)
      throws IOException, URISyntaxException {
    final Path directory =
        Path.of(
            NativeLeanSphincsVerifierTest.class.getResource("leansphincs/" + operation).toURI());
    final List<Path> caseDirectories;
    try (Stream<Path> listing = Files.list(directory)) {
      caseDirectories = listing.sorted().toList();
    }
    return caseDirectories.stream()
        .map(dir -> new Vector(dir.getFileName().toString(), readFields(dir.resolve("data.yaml"))));
  }

  // Each line of a vector is "key: value", with hex values in single quotes.
  private static Map<String, String> readFields(final Path file) {
    final Map<String, String> fields = new HashMap<>();
    try {
      for (final String line : Files.readAllLines(file)) {
        final int colon = line.indexOf(':');
        final String value = line.substring(colon + 1).trim();
        if (colon > 0 && !value.isEmpty()) {
          fields.put(line.substring(0, colon).trim(), value.replace("'", ""));
        }
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return fields;
  }
}
