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
import org.ethereum.leanpq.LeanPq;

/**
 * {@link LeanSphincsVerifier} backed by the native library of lean-pq, the Java binding of leanVM.
 * The library loads on the first verification.
 */
public final class NativeLeanSphincsVerifier implements LeanSphincsVerifier {

  /** Creates the verifier. */
  public NativeLeanSphincsVerifier() {}

  @Override
  public boolean verify(final Bytes publicKey, final Bytes message, final Bytes signature) {
    // lean-pq throws on a key or message of another size; the interface returns false instead.
    if (publicKey.size() != PUBLIC_KEY_SIZE
        || message.size() != MESSAGE_SIZE
        || signature.size() != SIGNATURE_SIZE) {
      return false;
    }
    return LeanPq.sphincsVerify(
        publicKey.toArrayUnsafe(), message.toArrayUnsafe(), signature.toArrayUnsafe());
  }
}
