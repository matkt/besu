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
package org.hyperledger.besu.evm.v2;

import org.hyperledger.besu.evm.v2.OperationGroups.Link;

/**
 * The links of a code, from each pc where a run of operations started to its group, added the first
 * time the run starts. Only the pcs that run get a link, so a code doesn't pay for the parts that
 * never run.
 *
 * <p>Threads read the table without a lock. A link has final fields, so a thread that sees it sees
 * them set, and a thread that misses a link just added looks again under the lock.
 */
public final class OperationGroupLinks {

  private final OperationGroups groups;

  /** Open addressing by pc, at most half full. */
  private volatile Link[] table = new Link[16];

  private int size;

  OperationGroupLinks(final OperationGroups groups) {
    this.groups = groups;
  }

  OperationGroups groups() {
    return groups;
  }

  /**
   * The link of the run starting at a pc, added the first time.
   *
   * @param code the code
   * @param pc the pc, inside the code
   * @return the link
   */
  public Link at(final byte[] code, final int pc) {
    final Link link = find(table, pc);
    return link != null ? link : add(code, pc);
  }

  private synchronized Link add(final byte[] code, final int pc) {
    Link[] current = table;
    final Link existing = find(current, pc);
    if (existing != null) {
      return existing;
    }
    final Link link = groups.link(code, pc);
    if ((size + 1) * 2 > current.length) {
      final Link[] larger = new Link[current.length * 2];
      for (final Link old : current) {
        if (old != null) {
          insert(larger, old);
        }
      }
      current = larger;
    }
    insert(current, link);
    size++;
    table = current;
    return link;
  }

  private static Link find(final Link[] table, final int pc) {
    final int mask = table.length - 1;
    int index = slot(pc, mask);
    Link link = table[index];
    while (link != null && link.pc != pc) {
      index = (index + 1) & mask;
      link = table[index];
    }
    return link;
  }

  private static void insert(final Link[] table, final Link link) {
    final int mask = table.length - 1;
    int index = slot(link.pc, mask);
    while (table[index] != null) {
      index = (index + 1) & mask;
    }
    table[index] = link;
  }

  private static int slot(final int pc, final int mask) {
    // spreads the pcs of a code, which are close together
    return ((pc * 0x9E3779B9) >>> 16) & mask;
  }
}
