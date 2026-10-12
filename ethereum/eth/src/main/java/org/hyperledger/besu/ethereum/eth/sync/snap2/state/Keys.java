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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Arithmetic on 32-byte trie keys. */
public final class Keys {

  /** Smallest key. */
  public static final Bytes32 MIN = Bytes32.ZERO;

  /** Largest key. */
  public static final Bytes32 MAX = Bytes32.fromHexString("ff".repeat(32));

  private static final BigInteger KEY_SPACE = BigInteger.ONE.shiftLeft(256);

  private Keys() {}

  /** Returns the key right after {@code key}, or empty for {@link #MAX}. */
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

  /** Splits {@code [from, to]} into at most {@code parts} contiguous ranges of similar size. */
  public static List<Bytes32[]> split(final Bytes32 from, final Bytes32 to, final int parts) {
    final BigInteger start = from.toUnsignedBigInteger();
    final BigInteger size = to.toUnsignedBigInteger().subtract(start).add(BigInteger.ONE);
    final BigInteger count = BigInteger.valueOf(Math.max(1, parts)).min(size);
    final BigInteger step = size.divide(count);
    final List<Bytes32[]> ranges = new ArrayList<>();
    BigInteger current = start;
    for (int i = 0; i < count.intValue(); i++) {
      final BigInteger end =
          i == count.intValue() - 1
              ? to.toUnsignedBigInteger()
              : current.add(step).subtract(BigInteger.ONE);
      ranges.add(new Bytes32[] {toKey(current), toKey(end)});
      current = end.add(BigInteger.ONE);
    }
    return ranges;
  }

  /** Returns the share of the key space covered by {@code [from, to]}. */
  public static double fraction(final Bytes32 from, final Bytes32 to) {
    final BigInteger size =
        to.toUnsignedBigInteger().subtract(from.toUnsignedBigInteger()).add(BigInteger.ONE);
    return size.doubleValue() / KEY_SPACE.doubleValue();
  }

  /** Converts a non-negative integer below 2^256 to a key. */
  public static Bytes32 toKey(final BigInteger value) {
    return Bytes32.leftPad(Bytes.wrap(value.toByteArray()).trimLeadingZeros());
  }
}
