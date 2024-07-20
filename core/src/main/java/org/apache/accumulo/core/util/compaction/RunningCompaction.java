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
package org.apache.accumulo.core.util.compaction;

import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.grpc.compaction.protobuf.PCompactionStatusUpdate;
import org.apache.accumulo.grpc.compaction.protobuf.PExternalCompaction;
import org.apache.accumulo.grpc.compaction.protobuf.PExternalCompactionJob;

public class RunningCompaction {

  private final PExternalCompactionJob job;
  private final String compactorAddress;
  private final String groupName;
  private final Map<Long,PCompactionStatusUpdate> updates = new TreeMap<>();

  public RunningCompaction(PExternalCompactionJob job, String compactorAddress, String groupName) {
    this.job = job;
    this.compactorAddress = compactorAddress;
    this.groupName = groupName;
  }

  public RunningCompaction(PExternalCompaction tEC) {
    this(tEC.getJob(), tEC.getCompactor(), tEC.getGroupName());
  }

  public Map<Long,PCompactionStatusUpdate> getUpdates() {
    synchronized (updates) {
      return new TreeMap<>(updates);
    }
  }

  public void addUpdate(Long timestamp, PCompactionStatusUpdate update) {
    synchronized (updates) {
      this.updates.put(timestamp, update);
    }
  }

  public PExternalCompactionJob getJob() {
    return job;
  }

  public String getCompactorAddress() {
    return compactorAddress;
  }

  public String getGroupName() {
    return groupName;
  }

}
