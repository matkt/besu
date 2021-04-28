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
package org.hyperledger.besu.ethereum.core;

import static org.hyperledger.besu.ethereum.vm.MessageFrame.DEFAULT_MAX_STACK_SIZE;

import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.vm.BlockHashLookup;
import org.hyperledger.besu.ethereum.vm.Code;
import org.hyperledger.besu.ethereum.vm.MessageFrame;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public class MessageFrameTestFixture {

  public static final Address DEFAUT_ADDRESS = AddressHelpers.ofValue(244259721);
  private final int maxStackSize = DEFAULT_MAX_STACK_SIZE;

  private MessageFrame.Type type = MessageFrame.Type.MESSAGE_CALL;
  private Deque<MessageFrame> messageFrameStack = new ArrayDeque<>();
  private Optional<Blockchain> blockchain = Optional.empty();
  private Optional<WorldUpdater> worldState = Optional.empty();
  private Gas initialGas = Gas.MAX_VALUE;
  private Address address = DEFAUT_ADDRESS;
  private Address sender = DEFAUT_ADDRESS;
  private Address originator = DEFAUT_ADDRESS;
  private Address contract = DEFAUT_ADDRESS;
  private int contractAccountVersion = Account.DEFAULT_VERSION;
  private Wei gasPrice = Wei.ZERO;
  private Wei value = Wei.ZERO;
  private Bytes inputData = Bytes.EMPTY;
  private Code code = new Code(Bytes.EMPTY);
  private final List<Bytes32> stackItems = new ArrayList<>();
  private Optional<BlockHeader> blockHeader = Optional.empty();
  private int depth = 0;
  private Optional<BlockHashLookup> blockHashLookup = Optional.empty();
  private ReturnStack returnStack = new ReturnStack();
  private ExecutionContextTestFixture executionContextTestFixture;

  public MessageFrameTestFixture type(final MessageFrame.Type type) {
    this.type = type;
    return this;
  }

  public MessageFrameTestFixture messageFrameStack(final Deque<MessageFrame> messageFrameStack) {
    this.messageFrameStack = messageFrameStack;
    return this;
  }

  public MessageFrameTestFixture executionContextTestFixture(
      final ExecutionContextTestFixture executionContextTestFixture) {
    this.executionContextTestFixture = executionContextTestFixture;
    return this;
  }

  public MessageFrameTestFixture blockchain(final Blockchain blockchain) {
    this.blockchain = Optional.of(blockchain);
    return this;
  }

  public MessageFrameTestFixture worldState(final WorldUpdater worldState) {
    this.worldState = Optional.of(worldState);
    return this;
  }

  public MessageFrameTestFixture worldState(final MutableWorldState worldState) {
    this.worldState = Optional.of(worldState.updater());
    return this;
  }

  public MessageFrameTestFixture initialGas(final Gas initialGas) {
    this.initialGas = initialGas;
    return this;
  }

  public MessageFrameTestFixture sender(final Address sender) {
    this.sender = sender;
    return this;
  }

  public MessageFrameTestFixture address(final Address address) {
    this.address = address;
    return this;
  }

  public MessageFrameTestFixture originator(final Address originator) {
    this.originator = originator;
    return this;
  }

  public MessageFrameTestFixture contract(final Address contract) {
    this.contract = contract;
    return this;
  }

  public MessageFrameTestFixture contractAccountVersion(final int contractAccountVersion) {
    this.contractAccountVersion = contractAccountVersion;
    return this;
  }

  public MessageFrameTestFixture gasPrice(final Wei gasPrice) {
    this.gasPrice = gasPrice;
    return this;
  }

  public MessageFrameTestFixture value(final Wei value) {
    this.value = value;
    return this;
  }

  public MessageFrameTestFixture inputData(final Bytes inputData) {
    this.inputData = inputData;
    return this;
  }

  public MessageFrameTestFixture code(final Code code) {
    this.code = code;
    return this;
  }

  public MessageFrameTestFixture blockHeader(final BlockHeader blockHeader) {
    this.blockHeader = Optional.of(blockHeader);
    return this;
  }

  public MessageFrameTestFixture depth(final int depth) {
    this.depth = depth;
    return this;
  }

  public MessageFrameTestFixture pushStackItem(final Bytes32 item) {
    stackItems.add(item);
    return this;
  }

  public MessageFrameTestFixture blockHashLookup(final BlockHashLookup blockHashLookup) {
    this.blockHashLookup = Optional.of(blockHashLookup);
    return this;
  }

  public MessageFrameTestFixture returnStack(final ReturnStack returnStack) {
    this.returnStack = returnStack;
    return this;
  }

  public MessageFrame build() {
    final Blockchain blockchain = this.blockchain.orElseGet(this::createDefaultBlockchain);
    final BlockHeader blockHeader =
        this.blockHeader.orElseGet(() -> blockchain.getBlockHeader(0).get());
    final MessageFrame frame =
        MessageFrame.builder()
            .type(type)
            .messageFrameStack(messageFrameStack)
            .blockchain(blockchain)
            .worldState(worldState.orElseGet(this::createDefaultWorldState))
            .initialGas(initialGas)
            .address(address)
            .originator(originator)
            .gasPrice(gasPrice)
            .inputData(inputData)
            .sender(sender)
            .value(value)
            .apparentValue(value)
            .contract(contract)
            .contractAccountVersion(contractAccountVersion)
            .code(code)
            .blockHeader(blockHeader)
            .depth(depth)
            .completer(c -> {})
            .miningBeneficiary(blockHeader.getCoinbase())
            .blockHashLookup(
                blockHashLookup.orElseGet(() -> new BlockHashLookup(blockHeader, blockchain)))
            .maxStackSize(maxStackSize)
            .returnStack(returnStack)
            .build();
    stackItems.forEach(frame::pushStackItem);
    return frame;
  }

  private WorldUpdater createDefaultWorldState() {
    return getOrCreateExecutionContextTestFixture().getStateArchive().getMutable().updater();
  }

  private Blockchain createDefaultBlockchain() {
    return getOrCreateExecutionContextTestFixture().getBlockchain();
  }

  private ExecutionContextTestFixture getOrCreateExecutionContextTestFixture() {
    // Avoid creating a test fixture if the test supplies the blockchain and worldstate.
    if (executionContextTestFixture == null) {
      executionContextTestFixture = ExecutionContextTestFixture.create();
    }
    return executionContextTestFixture;
  }
}
