package org.hyperledger.besu.ethereum.eth.manager.snap;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.task.AbstractRetryingPeerTask;
import org.hyperledger.besu.ethereum.eth.manager.task.EthTask;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.tuweni.bytes.Bytes;

public class RetryingGetTrieNodeFromPeerTask extends AbstractRetryingPeerTask<Map<Hash, Bytes>> {

  private final EthContext ethContext;
  private final List<Hash> hashes;
  private final Optional<List<List<Bytes>>> paths;
  private final BlockHeader blockHeader;
  private final MetricsSystem metricsSystem;

  private RetryingGetTrieNodeFromPeerTask(
      final EthContext ethContext,
      final List<Hash> hashes,
      final Optional<List<List<Bytes>>> paths,
      final BlockHeader blockHeader,
      final MetricsSystem metricsSystem) {
    super(ethContext, 3, data -> false, metricsSystem);
    this.ethContext = ethContext;
    this.hashes = hashes;
    this.paths = paths;
    this.blockHeader = blockHeader;
    this.metricsSystem = metricsSystem;
  }

  public static EthTask<Map<Hash, Bytes>> forTrieNodes(
      final EthContext ethContext,
      final List<Hash> hashes,
      final Optional<List<List<Bytes>>> paths,
      final BlockHeader blockHeader,
      final MetricsSystem metricsSystem) {
    return new RetryingGetTrieNodeFromPeerTask(
        ethContext, hashes, paths, blockHeader, metricsSystem);
  }

  @Override
  protected CompletableFuture<Map<Hash, Bytes>> executePeerTask(
      final Optional<EthPeer> assignedPeer) {
    final GetTrieNodeFromPeerTask task =
        GetTrieNodeFromPeerTask.forTrieNodes(
            ethContext, hashes, paths.get(), blockHeader, metricsSystem);
    assignedPeer.ifPresent(task::assignPeer);
    return executeSubTask(task::run)
        .thenApply(
            peerResult -> {
              result.complete(peerResult.getResult());
              return peerResult.getResult();
            });
  }
}
