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
package org.hyperledger.besu.ethereum.trie.pathbased.common.provider;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay;
import org.hyperledger.besu.plugin.data.BlockHeader;

import java.util.Objects;
import java.util.Optional;

/** Parameters for querying the world state. */
public class WorldStateQueryParams {

  private final BlockHeader blockHeader;
  private final boolean shouldWorldStateUpdateHead;
  private final Hash blockHash;
  private final Optional<Hash> stateRoot;
  private final Optional<BlockAccessListOverlay> blockAccessListOverlay;
  private final Optional<Long> timeStamp;

  /**
   * Private constructor to enforce the use of the Builder.
   *
   * @param builder the builder to create an instance of WorldStateQueryParams
   */
  private WorldStateQueryParams(final Builder builder) {
    this.blockHeader = builder.parentBlockHeader;
    this.shouldWorldStateUpdateHead = builder.shouldWorldStateUpdateHead;
    this.blockHash = builder.blockHash;
    this.stateRoot = builder.stateRoot;
    this.blockAccessListOverlay = builder.blockAccessListOverlay;
    this.timeStamp = builder.timeStamp;
  }

  /**
   * Gets the block header.
   *
   * @return the block header
   */
  public BlockHeader getBlockHeader() {
    return blockHeader;
  }

  /**
   * Checks if the world state should update the node head.
   *
   * @return true if the world state should update the node head, false otherwise
   */
  public boolean shouldWorldStateUpdateHead() {
    return shouldWorldStateUpdateHead;
  }

  /**
   * Gets the block hash.
   *
   * @return the block hash
   */
  public Hash getBlockHash() {
    return blockHash;
  }

  /**
   * Gets the state root.
   *
   * @return the state root
   */
  public Optional<Hash> getStateRoot() {
    return stateRoot;
  }

  /** Optional BAL overlay applied when the queried world state's accumulator is created. */
  public Optional<BlockAccessListOverlay> getBlockAccessListOverlay() {
    return blockAccessListOverlay;
  }

  public Optional<Long> getTimeStamp() {
    return timeStamp;
  }

  /**
   * Creates a new builder for WorldStateQueryParams.
   *
   * @return a new builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates an instance with a block header and updates the node head.
   *
   * @param blockHeader the block header
   * @return an instance of WorldStateQueryParams
   */
  public static WorldStateQueryParams withBlockHeaderAndUpdateNodeHead(
      final BlockHeader blockHeader) {
    return newBuilder()
        .withParentBlockHeader(blockHeader)
        .withShouldWorldStateUpdateHead(true)
        .build();
  }

  /**
   * Creates an instance with a block header and does not update the node head.
   *
   * @param blockHeader the block header
   * @param timeStamp the next block timestamp
   * @return an instance of WorldStateQueryParams
   */
  public static WorldStateQueryParams withBlockHeaderAndNoUpdateNodeHead(
      final BlockHeader blockHeader, final long timeStamp) {
    return newBuilder()
        .withParentBlockHeader(blockHeader)
        .withTimeStamp(timeStamp)
        .withShouldWorldStateUpdateHead(false)
        .build();
  }

  public static WorldStateQueryParams withBlockHeaderAndNoUpdateNodeHead(
      final BlockHeader blockHeader) {
    return newBuilder()
        .withParentBlockHeader(blockHeader)
        .withShouldWorldStateUpdateHead(false)
        .build();
  }

  /**
   * Should return a worldstate instance with a state root, block hash, and should update the node
   * head.
   *
   * @param stateRoot the state root
   * @param blockHash the block hash
   * @return an instance of WorldStateQueryParams
   */
  public static WorldStateQueryParams withStateRootAndBlockHashAndUpdateNodeHead(
      final Hash stateRoot, final Hash blockHash) {
    return newBuilder()
        .withStateRoot(stateRoot)
        .withBlockHash(blockHash)
        .withShouldWorldStateUpdateHead(true)
        .build();
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    WorldStateQueryParams that = (WorldStateQueryParams) o;
    return shouldWorldStateUpdateHead == that.shouldWorldStateUpdateHead
        && Objects.equals(blockHeader, that.blockHeader)
        && Objects.equals(blockHash, that.blockHash)
        && Objects.equals(stateRoot, that.stateRoot)
        && Objects.equals(blockAccessListOverlay, that.blockAccessListOverlay);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        blockHeader, shouldWorldStateUpdateHead, blockHash, stateRoot, blockAccessListOverlay);
  }

  public static class Builder {
    private BlockHeader parentBlockHeader;
    private boolean shouldWorldStateUpdateHead = false;
    private Hash blockHash;
    private Optional<Hash> stateRoot = Optional.empty();
    private Optional<BlockAccessListOverlay> blockAccessListOverlay = Optional.empty();
    private Optional<Long> timeStamp = Optional.empty();

    private Builder() {}

    /**
     * Sets the block header.
     *
     * @param parentBlockHeader the block header
     * @return the builder
     */
    public Builder withParentBlockHeader(final BlockHeader parentBlockHeader) {
      this.parentBlockHeader = parentBlockHeader;
      this.timeStamp = Optional.of(parentBlockHeader.getTimestamp());
      this.blockHash = parentBlockHeader.getBlockHash();
      this.stateRoot = Optional.of(parentBlockHeader.getStateRoot());
      return this;
    }

    /**
     * Sets whether the world state should update the node head.
     *
     * @param shouldWorldStateUpdateHead true if the world state should update the node head, false
     *     otherwise
     * @return the builder
     */
    public Builder withShouldWorldStateUpdateHead(final boolean shouldWorldStateUpdateHead) {
      this.shouldWorldStateUpdateHead = shouldWorldStateUpdateHead;
      return this;
    }

    /**
     * Sets the block hash.
     *
     * @param blockHash the block hash
     * @return the builder
     */
    public Builder withBlockHash(final Hash blockHash) {
      this.blockHash = blockHash;
      return this;
    }

    /**
     * Sets the state root.
     *
     * @param stateRoot the state root
     * @return the builder
     */
    public Builder withStateRoot(final Hash stateRoot) {
      this.stateRoot = Optional.ofNullable(stateRoot);
      return this;
    }

    /**
     * Applies a BAL overlay when the queried world state's accumulator is created.
     *
     * @param blockAccessListOverlay the overlay to configure on the accumulator
     * @return the builder
     */
    public Builder withBalOverlay(final BlockAccessListOverlay blockAccessListOverlay) {
      this.blockAccessListOverlay = Optional.of(blockAccessListOverlay);
      return this;
    }

    public Builder withTimeStamp(final long timeStamp) {
      this.timeStamp = Optional.of(timeStamp);
      return this;
    }

    /**
     * Builds an instance of WorldStateQueryParams.
     *
     * @return an instance of WorldStateQueryParams
     */
    public WorldStateQueryParams build() {

      if (blockHash == null && stateRoot.isEmpty() && parentBlockHeader == null) {
        throw new IllegalArgumentException(
            "Either blockHash, stateRoot, or blockHeader must be provided");
      }

      return new WorldStateQueryParams(this);
    }
  }
}
