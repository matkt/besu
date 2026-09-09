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
package org.hyperledger.besu.ethereum.trie.common;

import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.account.StorageRootStrategy;

/**
 * An {@link AccountValue} that knows the strategy reproducing its own RLP shape: a 4-field MPT
 * value yields an MPT strategy, a 3-field binary value the binary one.
 *
 * <p>{@link AccountValue} lives in the {@code datatypes} module, which cannot see {@link
 * StorageRootStrategy}; this interface carries the mapping on the core side instead.
 */
public interface TrieAccountValue extends AccountValue {

  /**
   * The strategy matching this value's encoded shape, safe to hand to a new account: MPT strategies
   * own a mutable storage root and are never shared between accounts.
   *
   * @return a strategy preserving this value's RLP field count.
   */
  StorageRootStrategy storageRootStrategy();
}
