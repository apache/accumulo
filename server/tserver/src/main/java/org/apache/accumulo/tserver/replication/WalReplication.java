/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.replication;

import java.util.Objects;

import org.apache.accumulo.core.replication.thrift.WalEdits;

/**
 * A "struct" to avoid a nested Entry. Contains the resultant information from collecting data for
 * replication
 */
@Deprecated
public class WalReplication extends ReplicationStats {
  /**
   * The data to send over the wire
   */
  public WalEdits walEdits;

  /**
   * The number of updates contained in this batch
   */
  public long numUpdates;

  public WalReplication(WalEdits edits, long size, long entriesConsumed, long numMutations) {
    super(size, edits.getEditsSize(), entriesConsumed);
    this.walEdits = edits;
    this.numUpdates = numMutations;
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(walEdits) + Objects.hashCode(numUpdates);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof WalReplication) {
      WalReplication other = (WalReplication) o;

      return super.equals(other) && walEdits.equals(other.walEdits)
          && numUpdates == other.numUpdates;
    }

    return false;
  }
}
