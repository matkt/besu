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
package org.hyperledger.besu.ethereum.trie.pathbased.transition;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.plugin.services.trielogs.StateMigrationLog;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tuweni.bytes.Bytes;

public class PreloadedWorldStateData {
  private final Map<Hash, Bytes> preimages = new ConcurrentHashMap<>();
  private final Map<Address, BonsaiAccount> accounts = new ConcurrentHashMap<>();
  private final Map<Address, Map<StorageSlotKey, Bytes>> storage = new ConcurrentHashMap<>();
  private StateMigrationLog migrationLog;

  public StateMigrationLog getMigrationLog() {
    return migrationLog;
  }

  public void setMigrationLog(final StateMigrationLog migrationLog) {
    this.migrationLog = migrationLog;
  }

  public void addPreimage(final Hash hash, final Bytes key) {
    preimages.put(hash, key);
  }

  public Bytes getPreimage(final Hash hash) {
    return preimages.get(hash);
  }

  public void addAccount(final Address address, final BonsaiAccount account) {
    accounts.put(address, account);
  }

  public void addStorage(final Address address, final StorageSlotKey key, final Bytes value) {
    storage.computeIfAbsent(address, k -> new ConcurrentHashMap<>()).put(key, value);
  }

  public Map<Address, BonsaiAccount> getAccounts() {
    return accounts;
  }

  public Map<StorageSlotKey, Bytes> getStorage(final Address address) {
    return storage.getOrDefault(address, Collections.emptyMap());
  }
}
