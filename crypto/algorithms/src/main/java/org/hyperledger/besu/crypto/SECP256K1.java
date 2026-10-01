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
package org.hyperledger.besu.crypto;

import static org.hyperledger.besu.nativelib.secp256k1.LibSecp256k1.SECP256K1_EC_UNCOMPRESSED;

import org.hyperledger.besu.nativelib.secp256k1.LibSecp256k1;
import org.hyperledger.besu.nativelib.secp256k1.LibSecp256k1.secp256k1_ecdsa_recoverable_signature;
import org.hyperledger.besu.nativelib.secp256k1.LibSecp256k1.secp256k1_ecdsa_signature;
import org.hyperledger.besu.nativelib.secp256k1.LibSecp256k1.secp256k1_pubkey;
import org.hyperledger.besu.nativelib.secp256k1.LibSecp256k1JNI;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Optional;

import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.LongByReference;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.signers.DSAKCalculator;
import org.bouncycastle.crypto.signers.HMacDSAKCalculator;
import org.bouncycastle.math.ec.custom.sec.SecP256K1Curve;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The SECP256K1 implementation. */
/*
 * Adapted from the BitcoinJ ECKey (Apache 2 License) implementation:
 * https://github.com/bitcoinj/bitcoinj/blob/master/core/src/main/java/org/bitcoinj/core/ECKey.java
 *
 *
 * Adapted from the web3j (Apache 2 License) implementations:
 * https://github.com/web3j/web3j/crypto/src/main/java/org/web3j/crypto/*.java
 */
public class SECP256K1 extends AbstractSECP256 {

  private static final Logger LOG = LoggerFactory.getLogger(SECP256K1.class);

  private boolean useNative;

  /** The constant CURVE_NAME. */
  public static final String CURVE_NAME = "secp256k1";

  /** Instantiates a new SECP256K1. */
  public SECP256K1() {
    super(CURVE_NAME, SecP256K1Curve.q);

    // use the native library implementation, if it is available
    maybeEnableNative();
  }

  @Override
  public void disableNative() {
    useNative = false;
  }

  /**
   * Attempt to enable the native library for secp256k1
   *
   * @return true if the native library was enabled.
   */
  @Override
  public boolean maybeEnableNative() {
    try {
      useNative = LibSecp256k1.CONTEXT != null;
    } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
      LOG.info("Native secp256k1 not available - {}", e.getMessage());
      useNative = false;
    }
    return useNative;
  }

  @Override
  public boolean isNative() {
    return useNative;
  }

  /**
   * SECP256K1 is using the deterministic implementation of K calculation (defined on RFC 6979
   * section 3.2)
   *
   * @return an instance of HMacDSAKCalculator
   * @see <a href="https://datatracker.ietf.org/doc/html/rfc6979#section-3.2">RFC 6979 Section
   *     3.2</a>
   */
  @Override
  public DSAKCalculator getKCalculator() {
    return new HMacDSAKCalculator(new SHA256Digest());
  }

  @Override
  public SECPSignature sign(final Bytes32 dataHash, final KeyPair keyPair) {
    if (useNative) {
      return signNative(dataHash, keyPair);
    } else {
      return super.sign(dataHash, keyPair);
    }
  }

  /**
   * Verifies the given ECDSA signature against the message bytes using the public key bytes.
   *
   * <p>When using native ECDSA verification, data must be 32 bytes, and no element may be larger
   * than 520 bytes.
   *
   * @param data Hash of the data to verify.
   * @param signature ASN.1 encoded signature.
   * @param pub The public key bytes to use.
   * @return True if the verification is successful.
   */
  @Override
  public boolean verify(final Bytes data, final SECPSignature signature, final SECPPublicKey pub) {
    if (useNative) {
      return verifyNative(data, signature, pub);
    } else {
      return super.verify(data, signature, pub);
    }
  }

  @Override
  public Optional<SECPPublicKey> recoverPublicKeyFromSignature(
      final Bytes32 dataHash, final SECPSignature signature) {
    if (useNative) {
      // The BouncyCastle path gets this same check from super; only the native path needs it
      // here. Without it the two backends disagree on out-of-range r/s: native rejects r >= n in
      // its compact parser while BouncyCastle bounds r only by the field prime, so an EIP-7702
      // authorization tuple carrying n < r < p would recover different authorities on different
      // nodes. See AbstractSECP256#isRecoverable.
      if (!isRecoverable(signature)) {
        return Optional.empty();
      }
      try {
        return recoverFromSignatureNative(dataHash, signature);
      } catch (final IllegalArgumentException e) {
        return Optional.empty();
      }
    } else {
      return super.recoverPublicKeyFromSignature(dataHash, signature);
    }
  }

  @Override
  public String getCurveName() {
    return CURVE_NAME;
  }

  private SECPSignature signNative(final Bytes32 dataHash, final KeyPair keyPair) {
    final LibSecp256k1.secp256k1_ecdsa_recoverable_signature signature =
        new secp256k1_ecdsa_recoverable_signature();
    // sign in internal form
    if (LibSecp256k1.secp256k1_ecdsa_sign_recoverable(
            LibSecp256k1.CONTEXT,
            signature,
            dataHash.toArrayUnsafe(),
            keyPair.getPrivateKey().getEncoded(),
            null,
            null)
        == 0) {
      throw new RuntimeException(
          "Could not natively sign. Private Key is invalid or default nonce generation failed.");
    }

    // encode to compact form
    final ByteBuffer compactSig = ByteBuffer.allocate(64);
    final IntByReference recId = new IntByReference(0);
    LibSecp256k1.secp256k1_ecdsa_recoverable_signature_serialize_compact(
        LibSecp256k1.CONTEXT, compactSig, recId, signature);
    compactSig.flip();
    final byte[] sig = compactSig.array();

    // wrap in signature object
    final Bytes32 r = Bytes32.wrap(sig, 0);
    final Bytes32 s = Bytes32.wrap(sig, 32);
    return SECPSignature.create(
        r.toUnsignedBigInteger(), s.toUnsignedBigInteger(), (byte) recId.getValue(), curveOrder);
  }

  private boolean verifyNative(
      final Bytes data, final SECPSignature signature, final SECPPublicKey pub) {

    // translate signature
    final LibSecp256k1.secp256k1_ecdsa_signature _signature = new secp256k1_ecdsa_signature();
    if (LibSecp256k1.secp256k1_ecdsa_signature_parse_compact(
            LibSecp256k1.CONTEXT, _signature, signature.encodedBytes().toArrayUnsafe())
        == 0) {
      throw new IllegalArgumentException("Could not parse signature");
    }

    // translate key
    final LibSecp256k1.secp256k1_pubkey _pub = new secp256k1_pubkey();
    final Bytes encodedPubKey = Bytes.concatenate(Bytes.of(0x04), pub.getEncodedBytes());
    if (LibSecp256k1.secp256k1_ec_pubkey_parse(
            LibSecp256k1.CONTEXT, _pub, encodedPubKey.toArrayUnsafe(), encodedPubKey.size())
        == 0) {
      throw new IllegalArgumentException("Could not parse public key");
    }

    return LibSecp256k1.secp256k1_ecdsa_verify(
            LibSecp256k1.CONTEXT, _signature, data.toArrayUnsafe(), _pub)
        != 0;
  }

  @Override
  protected BigInteger recoverFromSignature(
      final int recId, final BigInteger r, final BigInteger s, final Bytes32 dataHash) {
    if (useNative) {
      return recoverFromSignatureNative(dataHash, new SECPSignature(r, s, (byte) recId))
          .map(key -> new BigInteger(1, key.getEncoded()))
          .orElse(null);
    } else {
      return super.recoverFromSignature(recId, r, s, dataHash);
    }
  }

  /*
   * Native structures reused by recoverFromSignatureNative, one per thread. They only carry data
   * from one native call to the next (parse -> recover -> serialize) and are never read from Java,
   * so automatic Java <-> native copies are turned off: JNA passes the pointer as is. Each call fully
   * overwrites a structure before the next call reads it, and a failed call aborts the recovery,
   * so a previous recovery's content is never used. Reusing them also avoids building a JNA
   * structure (reflection on its layout, native allocation) twice per recovery.
   */
  private static final ThreadLocal<LibSecp256k1.secp256k1_ecdsa_recoverable_signature>
      RECOVERABLE_SIGNATURE =
          ThreadLocal.withInitial(
              () -> {
                final LibSecp256k1.secp256k1_ecdsa_recoverable_signature signature =
                    new LibSecp256k1.secp256k1_ecdsa_recoverable_signature();
                signature.setAutoSynch(false);
                // Native memory is otherwise allocated lazily on the first Java -> native copy,
                // which no longer happens: allocate it now so the native calls get a valid pointer.
                signature.getPointer();
                return signature;
              });

  private static final ThreadLocal<LibSecp256k1.secp256k1_pubkey> PUBLIC_KEY =
      ThreadLocal.withInitial(
          () -> {
            final LibSecp256k1.secp256k1_pubkey publicKey = new LibSecp256k1.secp256k1_pubkey();
            publicKey.setAutoSynch(false);
            publicKey.getPointer();
            return publicKey;
          });

  /*
   * The JNI ecrecover entry point of besu-native, much cheaper to call than the JNA one: no JNA
   * structures, argument marshalling or by-reference values. LibSecp256k1 must be loaded first.
   */
  private static final boolean JNI_RECOVERY_AVAILABLE = isJniRecoveryAvailable();

  private static boolean isJniRecoveryAvailable() {
    try {
      return LibSecp256k1.CONTEXT != null && LibSecp256k1JNI.ENABLED;
    } catch (final UnsatisfiedLinkError | NoClassDefFoundError e) {
      LOG.info("secp256k1 JNI ecrecover not available: {}", e.getMessage());
      return false;
    }
  }

  private Optional<SECPPublicKey> recoverFromSignatureNative(
      final Bytes32 dataHash, final SECPSignature signature) {
    // The JNI entry point only takes recovery ids 0 and 1; 2 and 3 (r >= n, practically never
    // used) go through JNA.
    if (JNI_RECOVERY_AVAILABLE && (signature.getRecId() == 0 || signature.getRecId() == 1)) {
      return recoverFromSignatureJni(dataHash, signature);
    }
    return recoverFromSignatureJna(dataHash, signature);
  }

  /**
   * Recovers the public key through the JNI entry point. libsecp256k1 rejects r or s out of range
   * (when parsing) and zero (when recovering), which is reported as a failure, as with JNA.
   */
  private Optional<SECPPublicKey> recoverFromSignatureJni(
      final Bytes32 dataHash, final SECPSignature signature) {
    final byte[] compactSignature = signature.encodedBytes().slice(0, 64).toArrayUnsafe();
    final byte[] recovered = new byte[65];
    if (LibSecp256k1JNI.secp256k1_ecrecover_jni(
            dataHash.toArrayUnsafe(), compactSignature, signature.getRecId(), recovered)
        != LibSecp256k1JNI.STATUS_SUCCESS) {
      return Optional.empty();
    }
    // Uncompressed encoding: 0x04 prefix followed by the 64-byte key.
    return Optional.of(SECPPublicKey.create(Bytes.wrap(recovered, 1, 64), ALGORITHM));
  }

  /** Recovers the public key through the JNA bindings. Visible for testing. */
  Optional<SECPPublicKey> recoverFromSignatureJna(
      final Bytes32 dataHash, final SECPSignature signature) {

    // parse the sig
    final LibSecp256k1.secp256k1_ecdsa_recoverable_signature parsedSignature =
        RECOVERABLE_SIGNATURE.get();
    final Bytes encodedSig = signature.encodedBytes();
    if (LibSecp256k1.secp256k1_ecdsa_recoverable_signature_parse_compact(
            LibSecp256k1.CONTEXT,
            parsedSignature,
            encodedSig.slice(0, 64).toArrayUnsafe(),
            encodedSig.get(64))
        == 0) {
      throw new IllegalArgumentException("Could not parse signature");
    }

    // recover the key
    final LibSecp256k1.secp256k1_pubkey newPubKey = PUBLIC_KEY.get();
    if (LibSecp256k1.secp256k1_ecdsa_recover(
            LibSecp256k1.CONTEXT, newPubKey, parsedSignature, dataHash.toArrayUnsafe())
        == 0) {
      return Optional.empty();
    }

    // parse the key
    final ByteBuffer recoveredKey = ByteBuffer.allocate(65);
    final LongByReference keySize = new LongByReference(recoveredKey.limit());
    LibSecp256k1.secp256k1_ec_pubkey_serialize(
        LibSecp256k1.CONTEXT, recoveredKey, keySize, newPubKey, SECP256K1_EC_UNCOMPRESSED);

    return Optional.of(
        SECPPublicKey.create(Bytes.wrapByteBuffer(recoveredKey).slice(1), ALGORITHM));
  }
}
