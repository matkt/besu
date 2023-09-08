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
package org.hyperledger.besu.services.kvstore;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.kvstore.AbstractKeyValueStorageTest;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class InMemoryKeyValueStorageTest extends AbstractKeyValueStorageTest {

  @Override
  protected KeyValueStorage createStore() {
    return new InMemoryKeyValueStorage();
  }

  @Test
  public void assertSegmentedIsNearestTo() {
    final InMemoryKeyValueStorage store1 = (InMemoryKeyValueStorage) createStore();

    // create 10 entries
    final KeyValueStorageTransaction tx = store1.startTransaction();
    IntStream.range(0, 10)
        .forEach(
            i -> {
              final byte[] key = bytesFromHexString("000" + i);
              final byte[] value = bytesFromHexString("0FFF");
              tx.put(key, value);
              // different common prefix, and reversed order of bytes:
              final byte[] key2 = bytesFromHexString("010" + (9 - i));
              final byte[] value2 = bytesFromHexString("0FFF");
              tx.put(key2, value2);
            });
    tx.commit();

    // assert 0009 is closest to 000F
    var val =
        store1
            .getSegmentedStore()
            .getNearestTo(InMemoryKeyValueStorage.SEGMENT_IDENTIFIER, Bytes.fromHexString("000F"));
    assertThat(val).isPresent();
    assertThat(val.get().key()).isEqualTo(Bytes.fromHexString("0009"));

    // assert 0109 is closest to 010D
    var val2 =
        store1
            .getSegmentedStore()
            .getNearestTo(InMemoryKeyValueStorage.SEGMENT_IDENTIFIER, Bytes.fromHexString("010D"));
    assertThat(val2).isPresent();
    assertThat(val2.get().key()).isEqualTo(Bytes.fromHexString("0109"));

    // assert 0103 is closest to 0103
    var val3 =
        store1
            .getSegmentedStore()
            .getNearestTo(InMemoryKeyValueStorage.SEGMENT_IDENTIFIER, Bytes.fromHexString("0103"));
    assertThat(val3).isPresent();
    assertThat(val3.get().key()).isEqualTo(Bytes.fromHexString("0103"));

    // assert 0003 is closest to 0003
    var val4 =
        store1
            .getSegmentedStore()
            .getNearestTo(InMemoryKeyValueStorage.SEGMENT_IDENTIFIER, Bytes.fromHexString("0003"));
    assertThat(val4).isPresent();
    assertThat(val4.get().key()).isEqualTo(Bytes.fromHexString("0003"));

    // assert 0000 is closest to 0000
    var val5 =
        store1
            .getSegmentedStore()
            .getNearestTo(InMemoryKeyValueStorage.SEGMENT_IDENTIFIER, Bytes.fromHexString("0000"));
    assertThat(val5).isPresent();
    assertThat(val5.get().key()).isEqualTo(Bytes.fromHexString("0000"));
  }
}
