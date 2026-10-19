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
package org.hyperledger.besu.plugin.services.storage.rocksdb;

import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SortedFileWriter;

import java.nio.file.Path;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

/** A {@link SortedFileWriter} that builds an SST file with a column family's options. */
public class RocksDBSortedFileWriter implements SortedFileWriter {

  private final EnvOptions envOptions = new EnvOptions();
  private final Options options;
  private final SstFileWriter writer;

  /**
   * Opens an SST file.
   *
   * @param dbOptions the database options
   * @param columnFamilyOptions the options of the column family the file is for
   * @param file the file to write
   * @throws StorageException the storage exception
   */
  public RocksDBSortedFileWriter(
      final DBOptions dbOptions, final ColumnFamilyOptions columnFamilyOptions, final Path file)
      throws StorageException {
    this.options = new Options(dbOptions, columnFamilyOptions);
    this.writer = new SstFileWriter(envOptions, options);
    try {
      writer.open(file.toString());
    } catch (final RocksDBException e) {
      close();
      throw new StorageException(e);
    }
  }

  @Override
  public void put(final byte[] key, final byte[] value) throws StorageException {
    try {
      writer.put(key, value);
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void finish() throws StorageException {
    try {
      writer.finish();
    } catch (final RocksDBException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void close() {
    writer.close();
    options.close();
    envOptions.close();
  }
}
