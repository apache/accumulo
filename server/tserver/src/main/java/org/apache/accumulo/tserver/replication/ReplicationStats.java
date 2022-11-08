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

import org.apache.accumulo.server.replication.proto.Replication.Status;

@Deprecated
public class ReplicationStats {
  /**
   * The size, in bytes, of the data sent
   */
  public long sizeInBytes;

  /**
   * The number of records sent
   */
  public long sizeInRecords;

  /**
   * The number of entries consumed from the log (to increment {@link Status}'s begin)
   */
  public long entriesConsumed;

  public ReplicationStats(long sizeInBytes, long sizeInRecords, long entriesConsumed) {
    this.sizeInBytes = sizeInBytes;
    this.sizeInRecords = sizeInRecords;
    this.entriesConsumed = entriesConsumed;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sizeInBytes + sizeInRecords + entriesConsumed);
  }

  @Override
  public boolean equals(Object o) {
    if (o != null) {
      if (ReplicationStats.class.isAssignableFrom(o.getClass())) {
        ReplicationStats other = (ReplicationStats) o;
        return sizeInBytes == other.sizeInBytes && sizeInRecords == other.sizeInRecords
            && entriesConsumed == other.entriesConsumed;
      }
    }
    return false;
  }
}
