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
package org.hyperledger.besu.ethereum.storage.keyvalue;

import static org.hyperledger.besu.plugin.services.storage.DataStorageFormat.BONSAI;
import static org.hyperledger.besu.plugin.services.storage.DataStorageFormat.FOREST;
import static org.hyperledger.besu.plugin.services.storage.DataStorageFormat.X_BONSAI_ARCHIVE;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public enum KeyValueSegmentIdentifier implements SegmentIdentifier {
  DEFAULT("default".getBytes(StandardCharsets.UTF_8)),
  BLOCKCHAIN(new byte[] {1}, EnumSet.allOf(DataStorageFormat.class), true, true, false),
  WORLD_STATE(new byte[] {2}, EnumSet.of(FOREST), false, true, false),

  // No longer used but retained for DB backwards compatibility
  PRIVATE_TRANSACTIONS(new byte[] {3}),
  PRIVATE_STATE(new byte[] {4}),

  PRUNING_STATE(new byte[] {5}, EnumSet.of(FOREST)),
  ACCOUNT_INFO_STATE(new byte[] {6}, EnumSet.of(BONSAI, X_BONSAI_ARCHIVE), false, true, false),
  CODE_STORAGE(new byte[] {7}, EnumSet.of(BONSAI, X_BONSAI_ARCHIVE)),
  ACCOUNT_STORAGE_STORAGE(new byte[] {8}, EnumSet.of(BONSAI, X_BONSAI_ARCHIVE), false, true, false),
  TRIE_BRANCH_STORAGE(new byte[] {9}, EnumSet.of(BONSAI, X_BONSAI_ARCHIVE), false, true, false),
  TRIE_LOG_STORAGE(new byte[] {10}, EnumSet.of(BONSAI, X_BONSAI_ARCHIVE), true, false, true),
  ACCOUNT_INFO_STATE_ARCHIVE(
      "ACCOUNT_INFO_STATE_ARCHIVE".getBytes(StandardCharsets.UTF_8),
      EnumSet.of(X_BONSAI_ARCHIVE),
      true,
      false,
      true),
  ACCOUNT_STORAGE_ARCHIVE(
      "ACCOUNT_STORAGE_ARCHIVE".getBytes(StandardCharsets.UTF_8),
      EnumSet.of(X_BONSAI_ARCHIVE),
      true,
      false,
      true),
  VARIABLES(new byte[] {11}), // formerly GOQUORUM_PRIVATE_WORLD_STATE

  // previously supported GoQuorum private states
  // no longer used but need to be retained for db backward compatibility
  GOQUORUM_PRIVATE_STORAGE(new byte[] {12}),

  BACKWARD_SYNC_HEADERS(new byte[] {13}),
  BACKWARD_SYNC_BLOCKS(new byte[] {14}),
  BACKWARD_SYNC_CHAIN(new byte[] {15}),
  SNAPSYNC_MISSING_ACCOUNT_RANGE(new byte[] {16}),
  SNAPSYNC_ACCOUNT_TO_FIX(new byte[] {17}),
  CHAIN_PRUNER_STATE(new byte[] {18}),
  ACCOUNT_HOT_STORAGE_STORAGE(
      new byte[] {19}, EnumSet.of(BONSAI, X_BONSAI_ARCHIVE), false, true, false);

  private static final List<Hash> HOT_CONTRACTS = new ArrayList<>();
  static {
    HOT_CONTRACTS.add(Address.fromHexString("0x06450dEe7FD2Fb8E39061434BAbCFC05599a6Fb8").addressHash());//XEN
    HOT_CONTRACTS.add(Address.fromHexString("0x06450dEe7FD2Fb8E39061434BAbCFC05599a6Fb8").addressHash());//USDT
    HOT_CONTRACTS.add(Address.fromHexString("0xdac17f958d2ee523a2206206994597c13d831ec7").addressHash());//AAVE
    HOT_CONTRACTS.add(Address.fromHexString("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").addressHash());//USDC
    HOT_CONTRACTS.add(Address.fromHexString("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").addressHash());//WETH

    HOT_CONTRACTS.add(Address.fromHexString("0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72").addressHash());//ENS 2
    HOT_CONTRACTS.add(Address.fromHexString("0x00000000006c3852cbEf3e08E8dF289169EdE581").addressHash());//OPENSEA SEAPORT
    HOT_CONTRACTS.add(Address.fromHexString("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b").addressHash());//OPENSEA WYVERN
    HOT_CONTRACTS.add(Address.fromHexString("0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85").addressHash());//ENS


    HOT_CONTRACTS.add(Address.fromHexString("0x8853B05833029e3Cf8d3Cbb592f9784FA43d2a79").addressHash());//CR
    HOT_CONTRACTS.add(Address.fromHexString("0xB705268213D593B8FD88d3FDEFF93AFF5CbDcfAE").addressHash());//IDEX
    HOT_CONTRACTS.add(Address.fromHexString("0xF5b0A3eFB8e8E4c201e2A935F110eAaF3FFEcb8d").addressHash());//AXIE

    HOT_CONTRACTS.add(Address.fromHexString("0x5283D291DBCF85356A21bA090E6db59121208b44").addressHash());//BLUR
    HOT_CONTRACTS.add(Address.fromHexString("0xB3319f5D18Bc0D84dD1b4825Dcde5d5f7266d407").addressHash());//0X
  }
  private final byte[] id;
  private final EnumSet<DataStorageFormat> formats;
  private final boolean containsStaticData;
  private final boolean eligibleToHighSpecFlag;
  private final boolean staticDataGarbageCollectionEnabled;

  KeyValueSegmentIdentifier(final byte[] id) {
    this(id, EnumSet.allOf(DataStorageFormat.class));
  }

  KeyValueSegmentIdentifier(final byte[] id, final EnumSet<DataStorageFormat> formats) {
    this(id, formats, false, false, false);
  }

  KeyValueSegmentIdentifier(
      final byte[] id,
      final EnumSet<DataStorageFormat> formats,
      final boolean containsStaticData,
      final boolean eligibleToHighSpecFlag,
      final boolean staticDataGarbageCollectionEnabled) {
    this.id = id;
    this.formats = formats;
    this.containsStaticData = containsStaticData;
    this.eligibleToHighSpecFlag = eligibleToHighSpecFlag;
    this.staticDataGarbageCollectionEnabled = staticDataGarbageCollectionEnabled;
  }

  @Override
  public String getName() {
    return name();
  }

  @Override
  public byte[] getId() {
    return id;
  }

  @Override
  public boolean containsStaticData() {
    return containsStaticData;
  }

  @Override
  public boolean isEligibleToHighSpecFlag() {
    return eligibleToHighSpecFlag;
  }

  @Override
  public boolean isStaticDataGarbageCollectionEnabled() {
    return staticDataGarbageCollectionEnabled;
  }

  @Override
  public boolean includeInDatabaseFormat(final DataStorageFormat format) {
    return formats.contains(format);
  }

  public static KeyValueSegmentIdentifier getContractSlotColumn(final Hash accountHash) {
    return HOT_CONTRACTS.contains(accountHash)
        ? ACCOUNT_HOT_STORAGE_STORAGE
        : ACCOUNT_STORAGE_STORAGE;
  }
}
