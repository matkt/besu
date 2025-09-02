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
package org.hyperledger.besu.ethereum.trie.pathbased.transition;

import okhttp3.ConnectionPool;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.hyperledger.besu.datatypes.Hash;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.tuweni.bytes.Bytes;

public class DebugPreImageClient {

  private static final String NODE_URL = "http://10.0.34.178:8545";

  private static final OkHttpClient client =
      new OkHttpClient.Builder()
          .connectionPool(new ConnectionPool(50, 5, TimeUnit.MINUTES))
          .connectTimeout(10, TimeUnit.SECONDS)
          .readTimeout(10, TimeUnit.SECONDS)
          .writeTimeout(10, TimeUnit.SECONDS)
          .build();

  public static Bytes getPreImage(final Hash hash) throws Exception {
    JsonObject request =
        new JsonObject()
            .put("jsonrpc", "2.0")
            .put("method", "debug_getPreImage")
            .put("params", new JsonArray().add(hash.toHexString()))
            .put("id", 1);

    Request postRequest =
        new Request.Builder()
            .url(NODE_URL)
            .post(
                RequestBody.create(
                    request.encode(), MediaType.get("application/json; charset=utf-8")))
            .build();

    try (Response response = client.newCall(postRequest).execute()) {
      if (!response.isSuccessful()) {
        throw new RuntimeException("HTTP error, code: " + response.code());
      }

      String responseBody = response.body().string();
      JsonObject jsonResponse = new JsonObject(responseBody);

      String result = jsonResponse.getString("result");
      if (result == null) {
        throw new RuntimeException("Invalid response: " + responseBody);
      }
      return Bytes.fromHexString(result);

    } catch (IOException e) {
      throw new RuntimeException("Error during HTTP request", e);
    }
  }
}
