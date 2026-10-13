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
package org.hyperledger.besu.ethereum.eth.sync.snap2.storage;

import java.math.BigInteger;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;

/** Arithmetic on 32-byte trie keys; the key space bounds are in {@code RangeManager}. */
public final class Keys {

  private static final BigInteger KEY_SPACE = BigInteger.ONE.shiftLeft(256);

  private Keys() {}

  /** Returns the key right after {@code key}, or empty after the last one. */
  public static Optional<Bytes32> next(final Bytes32 key) {
    final byte[] out = key.toArray();
    for (int i = 31; i >= 0; i--) {
      if (out[i] != (byte) 0xff) {
        out[i]++;
        return Optional.of(Bytes32.wrap(out));
      }
      out[i] = 0;
    }
    return Optional.empty();
  }

  /** Returns the share of the key space covered by {@code [from, to]}. */
  public static double fraction(final Bytes32 from, final Bytes32 to) {
    final BigInteger size =
        to.toUnsignedBigInteger().subtract(from.toUnsignedBigInteger()).add(BigInteger.ONE);
    return size.doubleValue() / KEY_SPACE.doubleValue();
  }
}
