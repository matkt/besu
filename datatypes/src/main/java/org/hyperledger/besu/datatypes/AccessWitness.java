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

import java.util.List;

import org.apache.tuweni.units.bigints.UInt256;

public interface AccessWitness {

  List<Address> keys();

  long touchAddressOnWriteAndComputeGas(Address address, UInt256 treeIndex, UInt256 subIndex);

  long touchAddressOnReadAndComputeGas(Address address, UInt256 treeIndex, UInt256 subIndex);

  List<UInt256> getStorageSlotTreeIndexes(UInt256 storageKey);

  long touchCodeChunksUponContractCreation(Address address, long codeLength);

  long accessAccountBasicData(Address address);

  long writeAccountBasicData(Address address);

  long accessAccountCodeHash(Address address);

  long writeAccountCodeHash(Address address);

  long accessAccountStorage(Address address, UInt256 storageKey);

  long writeAccountStorage(Address address, UInt256 storageKey);

  long touchCodeChunks(Address address, long offset, long readSize, long codeLength);

  default long touchCodeChunksWithoutAccessCost(
      final Address address, final long offset, final long readSize, final long codeLength) {
    return touchCodeChunks(address, offset, readSize, codeLength);
  }
}
