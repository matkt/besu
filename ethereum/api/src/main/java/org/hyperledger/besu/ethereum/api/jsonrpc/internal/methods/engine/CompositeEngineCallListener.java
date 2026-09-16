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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import java.util.Arrays;
import java.util.List;

/** Delegates engine API listener callbacks to multiple listeners. */
public final class CompositeEngineCallListener implements EngineCallListener {

  private final List<EngineCallListener> delegates;

  private CompositeEngineCallListener(final List<EngineCallListener> delegates) {
    this.delegates = delegates;
  }

  public static EngineCallListener of(final EngineCallListener... listeners) {
    return new CompositeEngineCallListener(Arrays.asList(listeners));
  }

  @Override
  public void executionEngineCalled() {
    delegates.forEach(EngineCallListener::executionEngineCalled);
  }

  @Override
  public void forkchoiceApplied() {
    delegates.forEach(EngineCallListener::forkchoiceApplied);
  }

  @Override
  public void stop() {
    delegates.forEach(EngineCallListener::stop);
  }
}
