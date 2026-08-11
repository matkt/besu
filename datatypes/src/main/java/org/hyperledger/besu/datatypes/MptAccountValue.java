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
package org.hyperledger.besu.datatypes;

/**
 * An {@link AccountValue} backed by a Merkle Patricia trie (MPT) account, which carries a
 * per-account storage root.
 *
 * <p>The partitioned binary trie has no per-account storage root, so binary account values do not
 * implement this interface. Callers that need the storage root should hold or check for an {@code
 * MptAccountValue} rather than calling {@code getStorageRoot()} on a generic {@link AccountValue}.
 */
public interface MptAccountValue extends AccountValue {

  /**
   * The hash of the root of the storage trie associated with this account.
   *
   * @return the hash of the root node of the storage trie.
   */
  Hash getStorageRoot();
}
