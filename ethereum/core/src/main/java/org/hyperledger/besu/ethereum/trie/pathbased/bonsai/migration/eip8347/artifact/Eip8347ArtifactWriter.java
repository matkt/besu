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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.migration.eip8347.artifact;

import org.hyperledger.besu.ethereum.rlp.RLP;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Writers for EIP-8347 snapshot and preimage artifacts (used by tests and tooling). */
public final class Eip8347ArtifactWriter {

  private Eip8347ArtifactWriter() {}

  public static void writeSnapshot(
      final Path path, final Bytes32 pbtRoot, final List<Eip8347SnapshotLeaf> leaves)
      throws IOException {
    final List<Eip8347SnapshotLeaf> sorted = new ArrayList<>(leaves);
    sorted.sort(Comparator.comparing(Eip8347SnapshotLeaf::key));
    try (final OutputStream out = Files.newOutputStream(path)) {
      out.write(pbtRoot.toArray());
      out.write(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(sorted.size()).array());
      for (final Eip8347SnapshotLeaf leaf : sorted) {
        final Bytes value = leaf.value().trimLeadingZeros();
        if (value.isEmpty()) {
          throw new Eip8347ArtifactVerificationException("cannot write zero-valued snapshot leaf");
        }
        final Bytes encoded =
            RLP.encode(
                rlp -> {
                  rlp.startList();
                  rlp.writeBytes(leaf.key());
                  rlp.writeBytes(value);
                  rlp.endList();
                });
        out.write(encoded.toArray());
      }
    }
  }

  public static void writePreimages(final Path path, final List<Eip8347PreimageRecord> records)
      throws IOException {
    final List<Eip8347PreimageRecord> sorted = new ArrayList<>(records);
    sorted.sort(Comparator.comparing(Eip8347PreimageRecord::addressHash));
    try (final OutputStream out = Files.newOutputStream(path)) {
      for (final Eip8347PreimageRecord record : sorted) {
        out.write(record.address().getBytes().toArrayUnsafe());
        final List<Integer> order = new ArrayList<>(record.slotKeys().size());
        for (int i = 0; i < record.slotKeys().size(); i++) {
          order.add(i);
        }
        order.sort(Comparator.comparing(i -> record.slotKeyHashes().get(i)));
        out.write(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(order.size()).array());
        for (final int i : order) {
          out.write(record.slotKeys().get(i).toArray());
        }
      }
    }
  }
}
