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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID_BLOCK_HASH;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.SYNCING;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.Configuration.FAIL_ON_UNKNOWN_BUT_EMPTY;
import static org.hyperledger.besu.metrics.BesuMetricCategory.BLOCK_PROCESSING;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcRequestException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.OrderedExecutionJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.PayloadStatusV1;
import org.hyperledger.besu.ethereum.chain.BadBlockCause;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.parallelization.EarlyBlockExecution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends {@link OrderedExecutionJsonRpcMethod} so that {@code engine_newPayload} calls (and those
 * of the whole sealed V1-V5 hierarchy) are processed in the order they have been received,
 * consistent with {@code engine_forkchoiceUpdated}: both affect canonical chain state and a
 * newPayload racing ahead of (or behind) an FCU for the same or a related block could otherwise be
 * observed out of order.
 */
public sealed class EngineNewPayloadV1<
        EP extends ExecutionPayloadV1, NPRP extends NewPayloadRequestParametersV1<? extends EP>>
    extends OrderedExecutionJsonRpcMethod permits EngineNewPayloadV2 {

  private static final JsonRpcParameter PAYLOAD_PARAMETER = new JsonRpcParameter();

  protected static final String BLOCK_ACCESS_LIST_FIELD = "blockAccessList";
  private static final String TRANSACTIONS_FIELD = "transactions";

  /** Cancels what was started ahead for the payload handled on this thread. */
  private static final ThreadLocal<List<Runnable>> STARTED_AHEAD =
      ThreadLocal.withInitial(ArrayList::new);

  private static final Logger LOG = LoggerFactory.getLogger(EngineNewPayloadV1.class);
  private static final Hash OMMERS_HASH_CONSTANT = Hash.EMPTY_LIST_HASH;
  private static final BlockHeaderFunctions HEADER_FUNCTIONS = new MainnetBlockHeaderFunctions();
  private final EthPeers ethPeers;
  private long lastExecutionTimeInNs = 0L;
  private long lastInvalidWarn = 0L;
  protected final MergeMiningCoordinator mergeCoordinator;

  @Override
  protected Logger logger() {
    return LOG;
  }

  public EngineNewPayloadV1(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(constructorArguments, minSupportedFork, firstUnsupportedFork);
    this.mergeCoordinator =
        checkNotNull(constructorArguments.mergeCoordinator(), "mergeCoordinator must not be null");
    this.ethPeers = constructorArguments.ethPeers();

    constructorArguments
        .metricsSystem()
        .createLongGauge(
            BLOCK_PROCESSING,
            "execution_time_head",
            "The execution time of the last block (head)",
            this::getLastExecutionTime);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_NEW_PAYLOAD_V1.getMethodName();
  }

  /**
   * Once the payload is handled, its block is processed or rejected: what was started ahead for it
   * (its transactions, the prefetch of its state, its state root) is of no use any more and is
   * cancelled.
   */
  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext requestContext) {
    try {
      return handlePayload(requestContext);
    } finally {
      final List<Runnable> cancellations = STARTED_AHEAD.get();
      STARTED_AHEAD.remove();
      cancellations.forEach(Runnable::run);
    }
  }

  /**
   * Cancels {@code cancellation} once the payload handled on this thread is handled.
   *
   * @param cancellation what cancels something started ahead for the payload
   */
  protected static void cancelOncePayloadHandled(final Runnable cancellation) {
    STARTED_AHEAD.get().add(cancellation);
  }

  private JsonRpcResponse handlePayload(final JsonRpcRequestContext requestContext) {
    engineCallListener.executionEngineCalled();

    final Object reqId = requestContext.getRequest().getId();

    final NPRP requestParameters;
    try {
      // 1. Client software MUST validate that all transactions have non-zero length (at least 1
      // byte). Client software MUST run this validation in all cases even if this branch or any
      // other branches of the block tree are in an active sync process.
      // the validation is done during the deserialization process
      requestParameters = readRequestParameters(requestContext);
    } catch (final InvalidRequestParametersException e) {
      // 6. Client software MUST respond to this method call in the following way:
      // {status: INVALID, latestValidHash: null, validationError: errorMessage | null} if
      // transactions contain zero length or invalid entries
      return processParametersParsingException(reqId, e);
    }

    final EP blockParam = requestParameters.payloadParameter();

    final ValidationResult<RpcErrorType> forkValidationResult =
        validateForkSupported(blockParam.getTimestamp());
    if (!forkValidationResult.isValid()) {
      return new JsonRpcErrorResponse(reqId, forkValidationResult);
    }

    final ValidationResult<RpcErrorType> parameterValidationResult =
        validateParameters(requestParameters);

    if (!parameterValidationResult.isValid()) {
      return new JsonRpcErrorResponse(reqId, parameterValidationResult.getInvalidReason());
    }

    // 2. Client software MUST validate blockHash value as being equivalent to
    // Keccak256(RLP(ExecutionBlockHeader)), where ExecutionBlockHeader is the execution layer block
    // header (the former PoW block header structure). Fields of this object are set to the
    // corresponding payload values and constant values according to the Block structure section of
    // EIP-3675, extended with the corresponding section of EIP-4399. Client software MUST run this
    // validation in all cases even if this branch or any other branches of the block tree are in an
    // active sync process.
    final BlockHeaderBuilder blockHeaderBuilder = BlockHeaderBuilder.create();
    setBlockHeaderFields(blockHeaderBuilder, requestParameters);
    final BlockHeader newBlockHeader = blockHeaderBuilder.buildBlockHeader();

    // ensure the block hash matches the blockParam hash
    // this must be done before any other check
    if (!newBlockHeader.getHash().equals(blockParam.getBlockHash())) {
      String errorMessage =
          String.format(
              "Computed block hash %s does not match block hash parameter %s",
              newBlockHeader.getBlockHash(), blockParam.getBlockHash());
      logger().debug(errorMessage);
      // 6. Client software MUST respond to this method call in the following way:
      // {status: INVALID_BLOCK_HASH, latestValidHash: null, validationError: errorMessage | null}
      // if the blockHash validation has failed
      return respondWithInvalid(reqId, blockParam, null, getInvalidBlockHashStatus(), errorMessage);
    }

    final Optional<BlockHeader> maybeParentHeader =
        protocolContext.getBlockchain().getBlockHeader(blockParam.getParentHash());

    final Optional<String> maybeBadBlockError;
    if (mergeCoordinator.isBadBlock(blockParam.getBlockHash())) {
      maybeBadBlockError = Optional.of("Block is a known bad block.");
    } else if (maybeParentHeader.isEmpty()) {
      maybeBadBlockError =
          mergeCoordinator
              .checkAndMarkBadDescendant(newBlockHeader)
              .map(BadBlockCause::getDescription);
    } else {
      // a parent that made it onto the chain cannot be bad, a stale entry, e.g. left by a
      // transient local failure, must not condemn its descendants
      maybeBadBlockError = Optional.empty();
    }
    if (maybeBadBlockError.isPresent()) {
      return respondWithInvalid(
          reqId,
          blockParam,
          mergeCoordinator.getLatestValidHashOfBadBlock(blockParam.getBlockHash()).orElse(null),
          INVALID,
          maybeBadBlockError.get());
    }

    final var unvalidatedBlock = new Block(newBlockHeader, createBlockBody(blockParam));

    // 3. Client software MAY initiate a sync process if requisite data for payload validation is
    // missing. Sync process is specified in the Sync section.
    final boolean needsSync = maybeParentHeader.isEmpty();
    // Only start backward sync when the initial sync is done
    if (needsSync && mergeContext.get().isInitialSyncDone()) {
      logger()
          .atDebug()
          .setMessage("Parent of block {} is not present, append it to backward sync")
          .addArgument(unvalidatedBlock::toLogString)
          .log();
      appendNewPayloadToSync(unvalidatedBlock, blockParam);
    }

    final ProtocolSpec protocolSpec = protocolSchedule.getByBlockHeader(newBlockHeader);

    // 4. Client software MUST validate the payload if it extends the canonical chain, and requisite
    // data for the validation is locally available. The validation process is specified in the
    // Payload validation section.
    // 5. Client software MAY NOT validate the payload if the payload doesn't belong to the
    // canonical chain.
    final ValidationResult<RpcErrorType> versionSpecificBlockValidationResult =
        validateNewBlock(unvalidatedBlock, protocolSpec, maybeParentHeader, requestParameters);
    if (!versionSpecificBlockValidationResult.isValid()) {
      return respondWithInvalid(
          reqId,
          blockParam,
          mergeCoordinator.getLatestValidAncestor(blockParam.getParentHash()).orElse(null),
          INVALID,
          versionSpecificBlockValidationResult.getErrorMessage());
    }

    // block is now valid and can be processed
    final Block block = unvalidatedBlock;

    mergeContext.get().fireNewPayloadEvent(newBlockHeader);

    // do we already have this payload?
    if (protocolContext.getBlockchain().getBlockByHash(block.getHash()).isPresent()) {
      logger()
          .atDebug()
          .setMessage("block {} already present")
          .addArgument(newBlockHeader::toLogString)
          .log();
      return respondWith(reqId, blockParam, block.getHash(), VALID);
    }

    if (needsSync) {
      // 6. Client software MUST respond to this method call in the following way:
      // {status: SYNCING, latestValidHash: null, validationError: null} if requisite data for the
      // payload's acceptance or validation is missing
      return respondWith(reqId, blockParam, null, SYNCING);
    }

    // an ancestor is always found here: the parent header is present in the chain (needsSync is
    // false) and getLatestValidAncestor only returns empty when it is not; this is also why Besu
    // never responds with ACCEPTED — a payload whose parent is known is always fully validated,
    // even when it does not extend the canonical chain
    final Hash latestValidAncestor =
        mergeCoordinator
            .getLatestValidAncestor(newBlockHeader)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Internal error: latestValidAncestor should always be present at this point"));

    // async precompute sender to improve performance during transaction processing
    asyncPrecomputeSenders(blockParam.getTransactions());

    // execute block and return result response
    final long startTimeNs = System.nanoTime();
    final BlockProcessingResult executionResult = rememberBlock(block, blockParam);
    if (executionResult.isSuccessful()) {
      lastExecutionTimeInNs = System.nanoTime() - startTimeNs;
      logImportedBlockInfo(
          block, lastExecutionTimeInNs, executionResult.getNbParallelizedTransactions());
      return respondWithValid(reqId, blockParam, newBlockHeader, executionResult);
    } else {
      logger().debug("New payload is invalid: {}", executionResult);
      if (executionResult.isWorldStateUnavailable()) {
        // we respond with SYNCING here to ensure a VALID newPayload is not marked INVALID.
        // however besu should not trigger a worldstate resync until/unless this chain is
        // finalized via forkchoiceUpdated.
        return respondWith(reqId, blockParam, null, SYNCING);
      }
      if (executionResult.isLocalFailure()) {
        return new JsonRpcErrorResponse(reqId, RpcErrorType.INTERNAL_ERROR);
      }
      protocolContext.getBadBlockManager().addLatestValidHash(block.getHash(), latestValidAncestor);
      return respondWithInvalid(
          reqId,
          blockParam,
          latestValidAncestor,
          INVALID,
          executionResult.errorMessage.orElse("N/A"));
    }
  }

  /**
   * Reads the payload so that its block can start running before the payload is fully read and
   * validated: the fields that work needs come first (the block access list, see {@link
   * #decodeBlockAccessListFirst}), then the transactions are decoded one at a time, each handed
   * over to the early execution of the block as soon as it is decoded, and the rest of the payload
   * last. Decoding errors are reported as before: a payload whose transactions do not decode is
   * converted as a whole again, which fails the same way.
   */
  protected ExecutionPayloadV1 readPayloadParameter(final JsonRpcRequestContext requestContext) {
    final Object rawParameter = requestContext.getRequest().getParams()[0];
    if (!(rawParameter instanceof Map<?, ?> rawPayload)) {
      return convertPayloadParameter(rawParameter, getPayloadParameterClass());
    }
    final Optional<BlockAccessList> blockAccessList = decodeBlockAccessListFirst(rawPayload);
    final Optional<List<Transaction>> transactions =
        startAhead(requestContext, rawPayload, blockAccessList)
            .flatMap(execution -> decodeTransactions(rawPayload, execution));

    final Map<Object, Object> rest = new LinkedHashMap<>(rawPayload);
    if (blockAccessList.isPresent()) {
      rest.remove(BLOCK_ACCESS_LIST_FIELD);
    }
    if (transactions.isPresent()) {
      rest.remove(TRANSACTIONS_FIELD);
    }
    final ExecutionPayloadV1 payload = convertPayloadParameter(rest, getPayloadParameterClass());
    transactions.ifPresent(payload::setTransactions);
    blockAccessList.ifPresent(decoded -> setBlockAccessList(payload, decoded));
    return payload;
  }

  /**
   * Decodes the block access list of the payload before anything else, so that the work it allows
   * starts early. None by default.
   *
   * @param rawPayload the raw payload parameter
   * @return the decoded block access list, if the payload has one
   */
  protected Optional<BlockAccessList> decodeBlockAccessListFirst(final Map<?, ?> rawPayload) {
    return Optional.empty();
  }

  /**
   * Sets on the payload the block access list decoded first. Nothing by default.
   *
   * @param payload the payload converted without it
   * @param blockAccessList the block access list decoded first
   */
  protected void setBlockAccessList(
      final ExecutionPayloadV1 payload, final BlockAccessList blockAccessList) {}

  /**
   * Starts the work on the block access list that only needs the parent state, before the block is
   * processed. Nothing by default.
   *
   * @param parentHeader the header of the parent of the block
   * @param protocolSpec the protocol spec of the block
   * @param gasLimit the gas limit of the block
   * @param blockAccessList the block access list of the block
   * @return whether the block may be valid: false if its block access list cannot belong to a valid
   *     block, in which case nothing is started for it
   */
  protected boolean startBlockAccessListWorkAhead(
      final BlockHeader parentHeader,
      final ProtocolSpec protocolSpec,
      final long gasLimit,
      final BlockAccessList blockAccessList) {
    return true;
  }

  /**
   * Starts, before the payload is fully read and validated, what only needs its parent state and
   * the first fields of the payload: the work on its block access list and the execution of its
   * transactions, which are then handed over as they are decoded. Whatever is started is cancelled
   * once the payload is handled; a payload that turns out to be invalid only cost that work.
   */
  private Optional<EarlyBlockExecution> startAhead(
      final JsonRpcRequestContext requestContext,
      final Map<?, ?> rawPayload,
      final Optional<BlockAccessList> blockAccessList) {
    try {
      final Hash parentHash = Hash.fromHexString((String) rawPayload.get("parentHash"));
      final long timestamp = Long.decode((String) rawPayload.get("timestamp"));
      final long gasLimit = Long.decode((String) rawPayload.get("gasLimit"));
      final Optional<BlockHeader> maybeParentHeader =
          protocolContext.getBlockchain().getBlockHeader(parentHash);
      if (maybeParentHeader.isEmpty()) {
        return Optional.empty();
      }
      final BlockHeader parentHeader = maybeParentHeader.get();
      final ProtocolSpec protocolSpec =
          protocolSchedule.getForNextBlockHeader(parentHeader, timestamp);
      if (blockAccessList.isPresent()
          && !startBlockAccessListWorkAhead(
              parentHeader, protocolSpec, gasLimit, blockAccessList.get())) {
        return Optional.empty();
      }
      if (!(rawPayload.get(TRANSACTIONS_FIELD) instanceof List<?> rawTransactions)) {
        return Optional.empty();
      }
      final Optional<EarlyBlockExecution> execution =
          protocolSpec
              .getBlockProcessor()
              .startBlockExecution(
                  protocolContext,
                  parentHeader,
                  executionContext(requestContext, rawPayload, parentHash, timestamp, gasLimit),
                  blockAccessList,
                  rawTransactions.size());
      execution.ifPresent(started -> cancelOncePayloadHandled(started::cancel));
      return execution;
    } catch (final RuntimeException e) {
      logger().debug("Could not start the work ahead of a payload", e);
      return Optional.empty();
    }
  }

  /**
   * The header fields the transactions of the block run with, from the raw payload: its header
   * without roots.
   */
  private static ProcessableBlockHeader executionContext(
      final JsonRpcRequestContext requestContext,
      final Map<?, ?> rawPayload,
      final Hash parentHash,
      final long timestamp,
      final long gasLimit) {
    final BlockHeaderBuilder header =
        BlockHeaderBuilder.create()
            .parentHash(parentHash)
            .coinbase(Address.fromHexString((String) rawPayload.get("feeRecipient")))
            .difficulty(Difficulty.ZERO)
            .number(Long.decode((String) rawPayload.get("blockNumber")))
            .gasLimit(gasLimit)
            .timestamp(timestamp)
            .prevRandao(Bytes32.fromHexString((String) rawPayload.get("prevRandao")));
    if (rawPayload.get("baseFeePerGas") instanceof String baseFee) {
      header.baseFee(Wei.fromHexString(baseFee));
    }
    if (rawPayload.get("slotNumber") instanceof String slotNumber) {
      header.slotNumber(Long.decode(slotNumber));
    }
    // from engine_newPayloadV3 on, the parent beacon block root is the third parameter
    final Object[] params = requestContext.getRequest().getParams();
    if (params.length > 2 && params[2] instanceof String parentBeaconBlockRoot) {
      header.parentBeaconBlockRoot(Bytes32.fromHexString(parentBeaconBlockRoot));
    }
    return header.buildProcessableBlockHeader();
  }

  /**
   * Decodes the transactions of the payload one at a time, handing each over to the early execution
   * as soon as it is decoded. Empty, with the execution cancelled, if one does not decode: the
   * payload is then converted as a whole, which reports the error as before.
   */
  private static Optional<List<Transaction>> decodeTransactions(
      final Map<?, ?> rawPayload, final EarlyBlockExecution execution) {
    if (!(rawPayload.get(TRANSACTIONS_FIELD) instanceof List<?> rawTransactions)) {
      execution.cancel();
      return Optional.empty();
    }
    final List<Transaction> transactions = new ArrayList<>(rawTransactions.size());
    try {
      for (final Object rawTransaction : rawTransactions) {
        final Transaction transaction =
            TransactionDecoder.decodeOpaqueBytes(
                Bytes.fromHexString((String) rawTransaction), EncodingContext.BLOCK_BODY);
        execution.submit(transactions.size(), transaction);
        transactions.add(transaction);
      }
    } catch (final RuntimeException e) {
      execution.cancel();
      return Optional.empty();
    }
    return Optional.of(transactions);
  }

  /**
   * Converts (part of) the raw payload parameter, failing like reading the payload parameter does.
   *
   * @param rawPayload the raw payload parameter, or part of it
   * @param parameterClass the class to convert it to
   * @return the converted parameter
   * @param <T> the type of the converted parameter
   */
  protected <T> T convertPayloadParameter(final Object rawPayload, final Class<T> parameterClass) {
    try {
      return PAYLOAD_PARAMETER.required(
          new Object[] {rawPayload}, 0, parameterClass, FAIL_ON_UNKNOWN_BUT_EMPTY);
    } catch (JsonRpcParameterException e) {
      throw new InvalidRequestParametersException(
          "Invalid engine payload parameter (index 0)",
          RpcErrorType.INVALID_ENGINE_NEW_PAYLOAD_PARAMS,
          e);
    }
  }

  @SuppressWarnings("unchecked")
  protected NPRP readRequestParameters(final JsonRpcRequestContext requestContext) {
    final int requestNumOfParams = requestContext.getRequest().getParamLength();
    if (requestNumOfParams != getNumberOfParameters()) {
      throw new InvalidRequestParametersException(
          "Expected %d parameters but got %d"
              .formatted(getNumberOfParameters(), requestNumOfParams),
          RpcErrorType.INVALID_PARAM_COUNT);
    }
    return (NPRP) new NewPayloadRequestParametersV1<>(readPayloadParameter(requestContext));
  }

  protected int getNumberOfParameters() {
    return 1;
  }

  protected Class<? extends ExecutionPayloadV1> getPayloadParameterClass() {
    return ExecutionPayloadV1.class;
  }

  private void asyncPrecomputeSenders(final List<Transaction> transactions) {
    transactions.forEach(
        transaction -> {
          mergeCoordinator
              .getEthScheduler()
              .scheduleComputationTask(
                  () -> {
                    final var sender = transaction.getSender();
                    logger()
                        .atTrace()
                        .setMessage("The sender for transaction {} is calculated : {}")
                        .addArgument(transaction::getHash)
                        .addArgument(sender)
                        .log();
                    return sender;
                  });
          if (transaction.getType().supportsDelegateCode()) {
            asyncPrecomputeAuthorities(transaction);
          }
        });
  }

  private void asyncPrecomputeAuthorities(final Transaction transaction) {
    final var codeDelegations = transaction.getCodeDelegationList().get();
    int index = 0;
    for (final var codeDelegation : codeDelegations) {
      final var constIndex = index++;
      mergeCoordinator
          .getEthScheduler()
          .scheduleComputationTask(
              () -> {
                final var authority = codeDelegation.authorizer();
                logger()
                    .atTrace()
                    .setMessage(
                        "The code delegation authority at index {} for transaction {} is calculated : {}")
                    .addArgument(constIndex)
                    .addArgument(transaction::getHash)
                    .addArgument(authority)
                    .log();
                return authority;
              });
    }
  }

  /**
   * Responds to a payload that was just executed and imported. Overridable so variants can answer
   * with data derived from block processing (e.g. the EIP-8025 execution witness), or with an error
   * if they cannot produce it; the default responds with the standard VALID payload status.
   *
   * <p>Note this covers only the freshly-executed path: a payload whose block is already present
   * returns VALID without passing through here.
   *
   * @param requestId the JSON-RPC request id
   * @param param the execution payload parameter
   * @param newBlockHeader the header of the imported block
   * @param executionResult the result of processing the block
   * @return the JSON-RPC response
   */
  protected JsonRpcResponse respondWithValid(
      final Object requestId,
      final ExecutionPayloadV1 param,
      final BlockHeader newBlockHeader,
      final BlockProcessingResult executionResult) {
    return respondWith(requestId, param, newBlockHeader.getHash(), VALID);
  }

  JsonRpcResponse respondWith(
      final Object requestId,
      final ExecutionPayloadV1 param,
      final Hash latestValidHash,
      final EngineStatus status) {
    if (INVALID.equals(status) || INVALID_BLOCK_HASH.equals(status)) {
      throw new IllegalArgumentException(
          "Don't call respondWith() with invalid status of " + status);
    }
    logNewPayloadResponse(param, latestValidHash, status);
    return new JsonRpcSuccessResponse(
        requestId, new PayloadStatusV1(status, latestValidHash, Optional.empty()));
  }

  protected void logNewPayloadResponse(
      final ExecutionPayloadV1 param, final Hash latestValidHash, final EngineStatus status) {
    logger()
        .atDebug()
        .setMessage(
            "New payload: number: {}, hash: {}, parentHash: {}, latestValidHash: {}, status: {}")
        .addArgument(param::getBlockNumber)
        .addArgument(param::getBlockHash)
        .addArgument(param::getParentHash)
        .addArgument(
            () -> latestValidHash == null ? null : latestValidHash.getBytes().toHexString())
        .addArgument(status::name)
        .log();
  }

  JsonRpcResponse respondWithInvalid(final Object requestId, final String validationError) {
    return respondWithInvalid(requestId, null, null, INVALID, validationError);
  }

  JsonRpcResponse respondWithInvalid(
      final Object requestId,
      final ExecutionPayloadV1 param,
      final Hash latestValidHash,
      final EngineStatus invalidStatus,
      final String validationError) {
    if (!INVALID.equals(invalidStatus) && !INVALID_BLOCK_HASH.equals(invalidStatus)) {
      throw new IllegalArgumentException(
          "Don't call respondWithInvalid() with non-invalid status of " + invalidStatus.toString());
    }
    final String invalidBlockLogMessage =
        String.format(
            "Invalid new payload: number: %s, hash: %s, parentHash: %s, latestValidHash: %s, status: %s, validationError: %s",
            param == null ? null : param.getBlockNumber(),
            param == null ? null : param.getBlockHash(),
            param == null ? null : param.getParentHash(),
            latestValidHash == null ? null : latestValidHash.getBytes().toHexString(),
            invalidStatus.name(),
            validationError);
    // always log invalid at DEBUG
    logger().debug(invalidBlockLogMessage);
    // periodically log at WARN
    if (lastInvalidWarn + ENGINE_API_LOGGING_THRESHOLD < System.currentTimeMillis()) {
      lastInvalidWarn = System.currentTimeMillis();
      logger().warn(invalidBlockLogMessage);
    }
    return new JsonRpcSuccessResponse(
        requestId,
        new PayloadStatusV1(invalidStatus, latestValidHash, Optional.of(validationError)));
  }

  protected EngineStatus getInvalidBlockHashStatus() {
    return INVALID_BLOCK_HASH;
  }

  protected ValidationResult<RpcErrorType> validateParameters(final NPRP requestParameters) {
    return ValidationResult.valid();
  }

  protected ValidationResult<RpcErrorType> validateNewBlock(
      final Block newBlock,
      final ProtocolSpec protocolSpec,
      final Optional<BlockHeader> maybeParentHeader,
      final NPRP requestParameters) {
    final BlockHeader newBlockHeader = newBlock.getHeader();
    if (newBlockHeader.getExtraData().size() > 32) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_EXTRA_DATA_PARAMS, "extra data field larger than 32 bytes");
    }
    if (maybeParentHeader.isPresent()
        && Long.compareUnsigned(
                maybeParentHeader.get().getTimestamp(), newBlock.getHeader().getTimestamp())
            >= 0) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_TIMESTAMP_PARAMS, "block timestamp not greater than parent");
    }
    return ValidationResult.valid();
  }

  protected void setBlockHeaderFields(
      final BlockHeaderBuilder blockHeaderBuilder, final NPRP requestParameters) {
    final ExecutionPayloadV1 blockParam = requestParameters.payloadParameter();
    blockHeaderBuilder
        .parentHash(blockParam.getParentHash())
        .ommersHash(OMMERS_HASH_CONSTANT)
        .coinbase(blockParam.getFeeRecipient())
        .stateRoot(blockParam.getStateRoot())
        .transactionsRoot(BodyValidation.transactionsRoot(blockParam.getTransactions()))
        .receiptsRoot(blockParam.getReceiptsRoot())
        .logsBloom(blockParam.getLogsBloom())
        .difficulty(Difficulty.ZERO)
        .number(blockParam.getBlockNumber())
        .gasLimit(blockParam.getGasLimit())
        .gasUsed(blockParam.getGasUsed())
        .timestamp(blockParam.getTimestamp())
        .extraData(blockParam.getExtraData())
        .baseFee(blockParam.getBaseFeePerGas())
        .prevRandao(blockParam.getPrevRandao())
        .nonce(0)
        .blockHeaderFunctions(HEADER_FUNCTIONS);
  }

  protected BlockBody createBlockBody(final EP executionPayload) {
    return new BlockBody(executionPayload.getTransactions(), Collections.emptyList());
  }

  protected BlockProcessingResult rememberBlock(final Block block, final EP executionPayload) {
    return mergeCoordinator.rememberBlock(block, Optional.empty());
  }

  protected void appendNewPayloadToSync(final Block block, final EP executionPayload) {
    mergeCoordinator.appendNewPayloadToSync(block, Optional.empty());
  }

  private void logImportedBlockInfo(
      final Block block, final long timeInNs, final Optional<Integer> nbParallelizedTransactions) {
    final StringBuilder message = new StringBuilder();
    final int nbTransactions = block.getBody().getTransactions().size();
    message.append("Imported #%,d  (%s)| %4d tx");
    final List<Object> messageArgs =
        new ArrayList<>(
            List.of(
                block.getHeader().getNumber(), block.getHash().toShortLogString(), nbTransactions));
    if (nbParallelizedTransactions.isPresent()) {
      double parallelizedTxPercentage =
          (double) (nbParallelizedTransactions.get() * 100) / nbTransactions;
      message.append(" (%5.1f%% parallel)");
      messageArgs.add(parallelizedTxPercentage);
    }
    appendVersionSpecificLogInfo(message, messageArgs, block);
    double mgasPerSec =
        (timeInNs != 0) ? (double) (block.getHeader().getGasUsed() * 1_000) / timeInNs : 0;
    double timeInMs = (double) timeInNs / 1_000_000;
    boolean timeOverOrEq1second = timeInMs >= 1_000;
    if (timeOverOrEq1second) {
      message.append("| %s bfee| %,11d (%5.1f%%) gas used| %01.3fs exec| %6.2f Mgas/s| %2d peers");
    } else {
      message.append("| %s bfee| %,11d (%5.1f%%) gas used| %03.1fms exec| %6.2f Mgas/s| %2d peers");
    }
    messageArgs.addAll(
        List.of(
            block.getHeader().getBaseFee().map(Wei::toHumanReadablePaddedString).orElse("N/A"),
            block.getHeader().getGasUsed(),
            (block.getHeader().getGasUsed() * 100.0) / block.getHeader().getGasLimit(),
            timeOverOrEq1second ? timeInMs / 1_000 : timeInMs,
            mgasPerSec,
            ethPeers.peerCount()));
    logger().info(String.format(message.toString(), messageArgs.toArray()));
  }

  protected void appendVersionSpecificLogInfo(
      final StringBuilder message, final List<Object> messageArgs, final Block block) {}

  private long getLastExecutionTime() {
    return this.lastExecutionTimeInNs;
  }

  protected JsonRpcResponse processParametersParsingException(
      final Object reqId, final InvalidRequestParametersException e) {
    final Optional<JsonMappingException> maybeFieldEx =
        extractCauseByType(e, JsonMappingException.class);

    // specific invalid field with custom error response
    String customMessage = null;
    if (maybeFieldEx.isPresent()) {
      final JsonMappingException fieldEx = maybeFieldEx.get();
      final Optional<String> maybeJsonPath = extractJsonPath(fieldEx);
      if (maybeJsonPath.isPresent()) {
        final String jsonPath = maybeJsonPath.get();
        if (jsonPath.equals("transactions")) {
          return respondWithInvalid(
              reqId,
              "Failed to decode transactions from block parameter (" + describe(fieldEx) + ")");
        } else if (jsonPath.equals("extraData")) {
          customMessage =
              "Failed to decode extraData from block parameter (" + describe(fieldEx) + ")";
        }
      }
    }

    return new JsonRpcErrorResponse(
        reqId,
        ValidationResult.invalid(
            RpcErrorType.INVALID_ENGINE_NEW_PAYLOAD_PARAMS,
            Objects.requireNonNullElse(
                customMessage, "Failed to decode block parameter (" + e.getMessage() + ")")));
  }

  /**
   * Describes a decoding failure, appending the root cause to the mapping exception's own message.
   *
   * <p>The outermost message is the generic wrapper the decoder adds — for a transaction list,
   * "Error applying element decoding function on element N of the list" — which says where the
   * failure was but nothing about what was wrong with it. The cause carries that, so a caller is
   * told the versioned hash was invalid rather than only that decoding stopped at element 0.
   */
  private static String describe(final JsonMappingException fieldEx) {
    final String message = fieldEx.getOriginalMessage();
    Throwable cause = fieldEx.getCause();
    while (cause != null && cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    final String rootMessage = cause == null ? null : cause.getMessage();
    return rootMessage == null || rootMessage.isBlank() || rootMessage.equals(message)
        ? message
        : message + ": " + rootMessage;
  }

  protected static class InvalidRequestParametersException extends InvalidJsonRpcRequestException {
    private final ExecutionPayloadV1 payloadParameter;

    InvalidRequestParametersException(final String message, final RpcErrorType rpcErrorType) {
      super(message, rpcErrorType);
      this.payloadParameter = null;
    }

    InvalidRequestParametersException(
        final String message, final RpcErrorType rpcErrorType, final Throwable cause) {
      this(null, message, rpcErrorType, cause);
    }

    InvalidRequestParametersException(
        final @Nullable ExecutionPayloadV1 payloadParameter,
        final String message,
        final RpcErrorType rpcErrorType,
        final Throwable cause) {
      super(message, rpcErrorType, cause);
      this.payloadParameter = payloadParameter;
    }

    boolean hasPayloadParameter() {
      return payloadParameter != null;
    }

    @NonNull ExecutionPayloadV1 getPayloadParameter() {
      checkNotNull(payloadParameter, "Payload parameter not present");
      return payloadParameter;
    }

    @Override
    public String getMessage() {
      return super.getMessage()
          + " (payloadParameter "
          + (payloadParameter != null ? " present)" : " absent)");
    }
  }
}
