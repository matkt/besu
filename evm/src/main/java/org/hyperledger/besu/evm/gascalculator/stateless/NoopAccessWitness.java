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
package org.hyperledger.besu.evm.gascalculator.stateless;

import org.hyperledger.besu.datatypes.AccessWitness;
import org.hyperledger.besu.datatypes.Address;

import org.apache.tuweni.units.bigints.UInt256;

import java.util.List;

public class NoopAccessWitness extends Eip4762AccessWitness implements AccessWitness {

  @Override
  public long touchAddressOnWriteAndComputeGas(final Address address, final UInt256 treeIndex, final UInt256 subIndex) {
    return 0;
  }

  @Override
  public long touchAddressOnReadAndComputeGas(final Address address, final UInt256 treeIndex, final UInt256 subIndex) {
    return 0;
  }

  @Override
  public long touchCodeChunksUponContractCreation(final Address address, final long codeLength) {
    return 0;
  }

  @Override
  public long accessAccountBasicData(final Address address) {
    return 0;
  }

  @Override
  public long writeAccountBasicData(final Address address) {
    return 0;
  }

  @Override
  public long accessAccountCodeHash(final Address address) {
    return 0;
  }

  @Override
  public long writeAccountCodeHash(final Address address) {
    return 0;
  }

  @Override
  public long accessAccountStorage(final Address address, final UInt256 storageKey) {
    return 0;
  }

  @Override
  public long writeAccountStorage(final Address address, final UInt256 storageKey) {
    return 0;
  }

  @Override
  public long touchCodeChunks(final Address address, final long offset, final long readSize, final long codeLength) {
    return 0;
  }
}
