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
package org.hyperledger.besu.ethereum.eth.sync.snap2;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * File marking a SNAP2 sync in progress. A node restarted while it exists has an incomplete world
 * state and syncs again from scratch.
 */
final class Snap2Marker {

  private final Path file;

  public Snap2Marker(final Path dataDirectory) {
    this.file = dataDirectory.resolve("snap2-sync-in-progress");
  }

  public boolean exists() {
    return Files.exists(file);
  }

  public void create() {
    try {
      Files.createDirectories(file.getParent());
      if (!Files.exists(file)) {
        Files.createFile(file);
      }
    } catch (final IOException e) {
      throw new UncheckedIOException("cannot create " + file, e);
    }
  }

  public void delete() {
    try {
      Files.deleteIfExists(file);
    } catch (final IOException e) {
      throw new UncheckedIOException("cannot delete " + file, e);
    }
  }
}
