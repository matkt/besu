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
  private final WorldStateUpdateMode worldStateUpdateMode;
  private final Hash blockHash;
  private final Optional<Hash> stateRoot;
  private final Optional<BlockAccessListOverlay> blockAccessListOverlay;

  /**
   * Private constructor to enforce the use of the Builder.
   *
   * @param builder the builder to create an instance of WorldStateQueryParams
   */
  private WorldStateQueryParams(final Builder builder) {
    this.blockHeader = builder.blockHeader;
    this.worldStateUpdateMode = builder.worldStateUpdateMode;
    this.blockHash = builder.blockHash;
    this.stateRoot = builder.stateRoot;
    this.blockAccessListOverlay = builder.blockAccessListOverlay;
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
    return worldStateUpdateMode.updatesHead();
  }

  /**
   * How the retrieved world state should treat persistence after processing.
   *
   * @return the update mode
   */
  public WorldStateUpdateMode getWorldStateUpdateMode() {
    return worldStateUpdateMode;
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
        .withBlockHeader(blockHeader)
        .withWorldStateUpdateMode(WorldStateUpdateMode.HEAD)
        .build();
  }

  /**
   * Creates an instance with a block header and does not update the node head.
   *
   * @param blockHeader the block header
   * @return an instance of WorldStateQueryParams
   */
  public static WorldStateQueryParams withBlockHeaderAndNoUpdateNodeHead(
      final BlockHeader blockHeader) {
    return newBuilder()
        .withBlockHeader(blockHeader)
        .withWorldStateUpdateMode(WorldStateUpdateMode.READ_ONLY)
        .build();
  }

  /**
   * Creates an instance that retains a durable payload layer without updating head.
   *
   * @param blockHeader the parent block header used as the base for the payload layer
   * @return an instance of WorldStateQueryParams
   */
  public static WorldStateQueryParams withBlockHeaderAndPayloadLayer(
      final BlockHeader blockHeader) {
    return newBuilder()
        .withBlockHeader(blockHeader)
        .withWorldStateUpdateMode(WorldStateUpdateMode.PAYLOAD_LAYER)
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
        .withWorldStateUpdateMode(WorldStateUpdateMode.HEAD)
        .build();
  }

  /**
   * Should return a worldstate instance with a state root and should update the node head.
   *
   * @param stateRoot the state root
   * @return an instance of WorldStateQueryParams
   */
  public static WorldStateQueryParams withStateRootAndUpdateNodeHead(final Hash stateRoot) {
    return newBuilder()
        .withStateRoot(stateRoot)
        .withWorldStateUpdateMode(WorldStateUpdateMode.HEAD)
        .build();
  }

  /**
   * Creates an instance with a state root, block hash, and does not update the node head.
   *
   * @param stateRoot the state root
   * @param blockHash the block hash
   * @return an instance of WorldStateQueryParams
   */
  public static WorldStateQueryParams withStateRootAndBlockHashAndNoUpdateNodeHead(
      final Hash stateRoot, final Hash blockHash) {
    return newBuilder()
        .withStateRoot(stateRoot)
        .withBlockHash(blockHash)
        .withWorldStateUpdateMode(WorldStateUpdateMode.READ_ONLY)
        .build();
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    WorldStateQueryParams that = (WorldStateQueryParams) o;
    return worldStateUpdateMode == that.worldStateUpdateMode
        && Objects.equals(blockHeader, that.blockHeader)
        && Objects.equals(blockHash, that.blockHash)
        && Objects.equals(stateRoot, that.stateRoot)
        && Objects.equals(blockAccessListOverlay, that.blockAccessListOverlay);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        blockHeader, worldStateUpdateMode, blockHash, stateRoot, blockAccessListOverlay);
  }

  public static class Builder {
    private BlockHeader blockHeader;
    private WorldStateUpdateMode worldStateUpdateMode = WorldStateUpdateMode.READ_ONLY;
    private Hash blockHash;
    private Optional<Hash> stateRoot = Optional.empty();
    private Optional<BlockAccessListOverlay> blockAccessListOverlay = Optional.empty();

    private Builder() {}

    /**
     * Sets the block header.
     *
     * @param blockHeader the block header
     * @return the builder
     */
    public Builder withBlockHeader(final BlockHeader blockHeader) {
      this.blockHeader = blockHeader;
      this.blockHash = blockHeader.getBlockHash();
      this.stateRoot = Optional.of(blockHeader.getStateRoot());
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
      this.worldStateUpdateMode =
          WorldStateUpdateMode.fromShouldUpdateHead(shouldWorldStateUpdateHead);
      return this;
    }

    /**
     * Sets the world-state update mode for this query.
     *
     * @param worldStateUpdateMode the mode
     * @return the builder
     */
    public Builder withWorldStateUpdateMode(final WorldStateUpdateMode worldStateUpdateMode) {
      this.worldStateUpdateMode = worldStateUpdateMode;
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

    /**
     * Builds an instance of WorldStateQueryParams.
     *
     * @return an instance of WorldStateQueryParams
     */
    public WorldStateQueryParams build() {

      if (blockHash == null && stateRoot.isEmpty() && blockHeader == null) {
        throw new IllegalArgumentException(
            "Either blockHash, stateRoot, or blockHeader must be provided");
      }

      return new WorldStateQueryParams(this);
    }
  }
}
