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
package org.hyperledger.besu.plugin.services.storage;

import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.io.Closeable;

/**
 * Writes one file of a segment's entries, in key order, for {@link
 * SegmentedKeyValueStorage#ingestSortedFiles}.
 */
@Unstable
public interface SortedFileWriter extends Closeable {

  /**
   * Adds an entry. Keys must be strictly ascending.
   *
   * @param key the key
   * @param value the value
   * @throws StorageException the storage exception
   */
  void put(byte[] key, byte[] value) throws StorageException;

  /**
   * Completes the file, which must hold at least one entry.
   *
   * @throws StorageException the storage exception
   */
  void finish() throws StorageException;

  /** Releases the writer. A file not finished is incomplete. */
  @Override
  void close();
}
