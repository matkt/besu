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
package org.hyperledger.besu.ethereum.eth.sync;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.task.GetBlockAccessListsFromPeerTask;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches BALs for the PBT migrator over eth/71 {@code GetBlockAccessLists}: batches of {@link
 * #BATCH_SIZE}, each retried a few times against whatever peer the executor picks, re-asking only
 * for the BALs still missing. Each BAL is checked against its header's BAL hash by the peer task.
 */
public final class PeerBlockAccessListFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(PeerBlockAccessListFetcher.class);

  static final int BATCH_SIZE = 128;
  static final int ATTEMPTS = 5;

  private final EthContext ethContext;

  public PeerBlockAccessListFetcher(final EthContext ethContext) {
    this.ethContext = ethContext;
  }

  /**
   * Fetches the BALs of {@code headers} from peers, each checked against its header's BAL hash.
   *
   * @param headers blocks whose BAL this node lacks
   * @return the BALs obtained, by block hash; missing ones are simply absent
   */
  public Map<Hash, BlockAccessList> fetch(final List<BlockHeader> headers) {
    final List<BlockHeader> wanted =
        headers.stream().filter(header -> header.getBalHash().isPresent()).toList();
    final Map<Hash, BlockAccessList> fetched = new HashMap<>();
    for (int from = 0; from < wanted.size(); from += BATCH_SIZE) {
      List<BlockHeader> missing = wanted.subList(from, Math.min(from + BATCH_SIZE, wanted.size()));
      for (int attempt = 0; attempt < ATTEMPTS && !missing.isEmpty(); attempt++) {
        final PeerTaskExecutorResult<List<Optional<BlockAccessList>>> result =
            ethContext.getPeerTaskExecutor().execute(new GetBlockAccessListsFromPeerTask(missing));
        if (result.responseCode() != PeerTaskExecutorResponseCode.SUCCESS
            || result.result().isEmpty()) {
          LOG.debug("BAL request failed ({}), attempt {}", result.responseCode(), attempt + 1);
          continue;
        }
        final List<Optional<BlockAccessList>> bals = result.result().get();
        final List<BlockHeader> stillMissing = new ArrayList<>();
        for (int i = 0; i < missing.size(); i++) {
          final Optional<BlockAccessList> bal = i < bals.size() ? bals.get(i) : Optional.empty();
          if (bal.isPresent()) {
            fetched.put(missing.get(i).getBlockHash(), bal.get());
          } else {
            stillMissing.add(missing.get(i));
          }
        }
        missing = stillMissing;
      }
    }
    return fetched;
  }
}
