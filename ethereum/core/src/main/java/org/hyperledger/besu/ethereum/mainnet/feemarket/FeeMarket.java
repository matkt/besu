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
package org.hyperledger.besu.ethereum.mainnet.feemarket;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.feemarket.TransactionPriceCalculator;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

public interface FeeMarket {

  default boolean implementsBaseFee() {
    return false;
  }

  TransactionPriceCalculator getTransactionPriceCalculator();

  Wei minTransactionPriceInNextBlock(
      Transaction transaction, Supplier<Optional<Long>> baseFeeSupplier);

  static BaseFeeMarket london(final long londonForkBlockNumber) {
    return london(londonForkBlockNumber, OptionalLong.empty());
  }

  static BaseFeeMarket london(
      final long londonForkBlockNumber, final OptionalLong baseFeePerGasOverride) {
    return new LondonFeeMarket(londonForkBlockNumber, baseFeePerGasOverride);
  }

  static FeeMarket legacy() {
    return new LegacyFeeMarket();
  }
}
