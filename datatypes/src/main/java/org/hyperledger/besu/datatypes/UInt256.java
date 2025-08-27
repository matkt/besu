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
package org.hyperledger.besu.datatypes;

import java.util.Arrays;

public final class UInt256 {
  private final int[] limbs; // little-endian limbs, length = 8
  private final int length; // number of significant limbs

  // --- Preallocating small integers 0..nSmallInts ---
  private static final int nSmallInts = 256;
  private static final UInt256[] smallInts = new UInt256[nSmallInts];

  static {
    for (int i = 0; i < nSmallInts; i++) {
      smallInts[i] = new UInt256(new int[] {i, 0, 0, 0, 0, 0, 0, 0});
    }
  }

  /** The constant 0. */
  public static final UInt256 ZERO = smallInts[0];

  /** The constant 1. */
  public static final UInt256 ONE = smallInts[1];

  /** The constant 2. */
  public static final UInt256 TWO = smallInts[2];

  /** The constant 10. */
  public static final UInt256 TEN = smallInts[10];

  /** The constant 16. */
  public static final UInt256 SIXTEEN = smallInts[16];

  // --- Constructors ---

  /**
   * Instantiates a new UInt256 from byte[].
   *
   * @param bytes raw bytes in BigEndian order.
   */
  public UInt256(final byte[] bytes) {
    if (bytes.length > 32) {
      throw new IllegalArgumentException("UInt256 can only hold up to 32 bytes");
    }

    // Left pad with zeros to 32 bytes
    byte[] padded = new byte[32];
    int offset = 32 - bytes.length;
    System.arraycopy(bytes, 0, padded, offset, bytes.length);

    // Convert to 8 little-endian 32-bit limbs
    this.limbs = new int[8];
    for (int i = 0; i < 8; i++) {
      int base = 28 - (i * 4); // big-endian bytes into little-endian ints
      this.limbs[i] =
          ((padded[base] & 0xFF) << 24)
              | ((padded[base + 1] & 0xFF) << 16)
              | ((padded[base + 2] & 0xFF) << 8)
              | ((padded[base + 3] & 0xFF));
    }
    length = computeLength(limbs);
  }

  public UInt256(final int[] limbs) {
    this.limbs = limbs;
    this.length = computeLength(limbs);
  }

  public UInt256(final int[] limbs, final int length) {
    this.limbs = limbs;
    this.length = length;
  }
  /**
   * Instantiates a new UInt256 from int[].
   *
   * @param limbs int limbs in LittleEndian order.
   */
  public static UInt256 fromLimbs(final int[] limbs) {
    if (limbs.length > 8) throw new IllegalArgumentException();
    return new UInt256(Arrays.copyOf(limbs, 8));
  }

  /**
   * Instantiates a new UInt256 from an int.
   *
   * @param value int value to convert to UInt256.
   */
  public static UInt256 fromInt(final int value) {
    if (0 <= value && value < nSmallInts) return smallInts[value];
    return new UInt256(new int[] {value, 0, 0, 0, 0, 0, 0, 0});
  }

  /**
   * Instantiates a new UInt256 from a long.
   *
   * @param value long value to convert to UInt256.
   */
  public static UInt256 fromLong(final long value) {
    if (0 <= value && value < nSmallInts) return smallInts[(int) value];
    return new UInt256(new int[] {(int) value, (int) (value >>> 32), 0, 0, 0, 0, 0, 0});
  }

  private static int computeLength(final int[] limbs) {
    for (int i = 7; i >= 0; i--) {
      if (limbs[i] != 0) return i + 1;
    }
    return 0;
  }

  // ---- conversion ----

  /**
   * Convert to int.
   *
   * @return Value truncated to an int, possibly lossy.
   */
  public int intValue() {
    return limbs[0];
  }

  /**
   * Convert to int.
   *
   * @return Value truncated to a long, possibly lossy.
   */
  public long longValue() {
    return (limbs[0] & 0xFFFFFFFFL) | ((limbs[1] & 0xFFFFFFFFL) << 32);
  }

  /** Convert to BigEndian byte array. */
  public byte[] toBytesBE() {
    byte[] out = new byte[32];
    encodeInt(out, 0, limbs[7]);
    encodeInt(out, 4, limbs[6]);
    encodeInt(out, 8, limbs[5]);
    encodeInt(out, 12, limbs[4]);
    encodeInt(out, 16, limbs[3]);
    encodeInt(out, 20, limbs[2]);
    encodeInt(out, 24, limbs[1]);
    encodeInt(out, 28, limbs[0]);
    return out;
  }

  private static void encodeInt(final byte[] out, final int offset, final int v) {
    out[offset] = (byte) (v >>> 24);
    out[offset + 1] = (byte) (v >>> 16);
    out[offset + 2] = (byte) (v >>> 8);
    out[offset + 3] = (byte) v;
  }

  /** Number of active int limbs. */
  public int length() {
    return length;
  }

  // ---- comparison ----

  /** Is the value 0 ? */
  public boolean isZero() {
    return length == 0;
  }

  /**
   * Compares two UInt256.
   *
   * @param a left UInt256
   * @param b right UInt256
   * @return 0 if a == b, negative if a &lt b and positive if a &gt b.
   */
  public static int compare(final UInt256 a, final UInt256 b) {
    int comp = Integer.compare(a.length(), b.length());
    if (comp != 0) return comp;
    long[] aa = a.asLongs();
    long[] bb = b.asLongs();
    for (int i = a.length - 1; i >= 0; i--) {
      comp = Long.compare(aa[i], bb[i]);
      if (comp != 0) return comp;
    }
    return 0;
  }

  private long[] asLongs() {
    return new long[] {
      limbs[0] & 0xFFFFFFFFL, limbs[1] & 0xFFFFFFFFL,
      limbs[2] & 0xFFFFFFFFL, limbs[3] & 0xFFFFFFFFL,
      limbs[4] & 0xFFFFFFFFL, limbs[5] & 0xFFFFFFFFL,
      limbs[6] & 0xFFFFFFFFL, limbs[7] & 0xFFFFFFFFL
    };
  }

  /**
   * Shifts value to the left.
   *
   * @param s number of places to shift. If negative, shift right instead.
   */
  public UInt256 shiftLeft(final int s) {
    if (s == 0 || isZero()) return this;
    if (s < 0) return shiftRight(-s);
    int[] res = new int[8];
    long carry = 0;
    for (int i = 0; i < 8; i++) {
      long v = (limbs[i] & 0xFFFFFFFFL);
      long r = (v << s) | carry;
      res[i] = (int) r;
      carry = r >>> 32;
    }
    return fromLimbs(res);
  }

  /**
   * Shifts value to the right.
   *
   * @param s number of places to shift. If negative, shift left instead.
   */
  public UInt256 shiftRight(final int s) {
    if (s == 0 || isZero()) return this;
    if (s < 0) return shiftLeft(-s);
    int[] res = new int[8];
    long carry = 0;
    for (int i = 7; i >= 0; i--) {
      long v = limbs[i] & 0xFFFFFFFFL;
      long r = (v >>> s) | (carry << (32 - s));
      res[i] = (int) r;
      carry = v & ((1L << s) - 1);
    }
    return fromLimbs(res);
  }

  /**
   * Reduce modulo divisor.
   *
   * @param divisor The modulus of the reduction
   */
  public UInt256 mod(final UInt256 divisor) {
    if (divisor.isZero()) throw new ArithmeticException("divide by zero");
    int cmp = compare(this, divisor);
    if (cmp < 0) return this;
    if (cmp == 0) return ZERO;

    int n = divisor.length();
    if (n == 1) {
      long d = divisor.limbs[0] & 0xFFFFFFFFL;
      if (d == 0) throw new ArithmeticException("divide by zero");

      long rem = 0;
      // Process from most significant limb downwards
      for (int i = this.length - 1; i >= 0; i--) {
        long cur = (rem << 32) | (limbs[i] & 0xFFFFFFFFL);
        rem = cur % d;
      }
      return fromLimbs(new int[] {(int) rem});
    }

    // --- Shortcut: divisor fits in 64 bits (2 limbs) ---
    if (n == 2) {
      long d = ((long) divisor.limbs[1] << 32) | (divisor.limbs[0] & 0xFFFFFFFFL);
      if (d == 0) throw new ArithmeticException("divide by zero");

      long rem = 0;
      // Process from most significant limb downwards
      for (int i = this.length - 1; i >= 0; i--) {
        long cur = (rem << 32) | (limbs[i] & 0xFFFFFFFFL);
        rem = cur % d;
      }
      int lo = (int) rem;
      int hi = (int) (rem >>> 32);
      return fromLimbs(new int[] {lo, hi});
    }

    // --- Knuth Division ---
    int m = this.length() - n;

    // Normalize
    int shift = Integer.numberOfLeadingZeros(divisor.limbs[n - 1]);
    UInt256 v = divisor.shiftLeft(shift);
    UInt256 u = this.shiftLeft(shift);

    int[] uLimbs = Arrays.copyOf(u.limbs, 9); // 1 extra limb
    int[] vLimbs = Arrays.copyOf(v.limbs, n);

    // Main division loop
    for (int j = m; j >= 0; j--) {
      long ujn = (uLimbs[j + n] & 0xFFFFFFFFL);
      long ujn1 = (uLimbs[j + n - 1] & 0xFFFFFFFFL);
      long ujn2 = (uLimbs[j + n - 2] & 0xFFFFFFFFL);
      long vn1 = vLimbs[n - 1] & 0xFFFFFFFFL;
      long vn2 = (n > 1) ? vLimbs[n - 2] & 0xFFFFFFFFL : 0;

      long dividendPart = (ujn << 32) | ujn1;
      long qhat = dividendPart / vn1;
      long rhat = dividendPart % vn1;

      while (qhat == 0x1_0000_0000L || (qhat * vn2) > ((rhat << 32) | ujn2)) {
        qhat--;
        rhat += vn1;
        if (rhat >= 0x1_0000_0000L) break;
      }

      // Multiply-subtract qhat*v from u slice
      long borrow = 0;
      for (int i = 0; i < n; i++) {
        long prod = (vLimbs[i] & 0xFFFFFFFFL) * qhat;
        long sub = (uLimbs[i + j] & 0xFFFFFFFFL) - (prod & 0xFFFFFFFFL) - borrow;
        uLimbs[i + j] = (int) sub;
        borrow = (prod >>> 32) - (sub >> 63);
      }
      long sub = (uLimbs[j + n] & 0xFFFFFFFFL) - borrow;
      uLimbs[j + n] = (int) sub;

      if (sub < 0) {
        // Add back
        long carry = 0;
        for (int i = 0; i < n; i++) {
          long sum = (uLimbs[i + j] & 0xFFFFFFFFL) + (vLimbs[i] & 0xFFFFFFFFL) + carry;
          uLimbs[i + j] = (int) sum;
          carry = sum >>> 32;
        }
        uLimbs[j + n] = (int) (uLimbs[j + n] + carry);
      }
    }

    // Unnormalize remainder
    int[] remLimbs = Arrays.copyOf(uLimbs, n);
      return fromLimbs(remLimbs).shiftRight(shift);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("0x");
    for (byte b : toBytesBE()) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof UInt256)) return false;
    UInt256 other = (UInt256) obj;

    // Compare lengths after trimming leading zero limbs
    int cmp = UInt256.compare(this, other);
    return cmp == 0;
  }

  @Override
  public int hashCode() {
    int h = 1;
    int len = this.length();
    for (int i = 0; i < len; i++) {
      h = 31 * h + limbs[i];
    }
    return h;
  }
}
