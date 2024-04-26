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

import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;

public class RunningCompaction {

  private final TExternalCompactionJob job;
  private final String compactorAddress;
  private final String queueName;
  private final Map<Long,TCompactionStatusUpdate> updates = new TreeMap<>();

  public RunningCompaction(TExternalCompactionJob job, String compactorAddress, String queueName) {
    this.job = job;
    this.compactorAddress = compactorAddress;
    this.queueName = queueName;
  }

  public RunningCompaction(TExternalCompaction tEC) {
    this(tEC.getJob(), tEC.getCompactor(), tEC.getQueueName());
  }

  public Map<Long,TCompactionStatusUpdate> getUpdates() {
    synchronized (updates) {
      return new TreeMap<>(updates);
    }
  }

  public void addUpdate(Long timestamp, TCompactionStatusUpdate update) {
    synchronized (updates) {
      this.updates.put(timestamp, update);
    }
  }

  public TExternalCompactionJob getJob() {
    return job;
  }

  public String getCompactorAddress() {
    return compactorAddress;
  }

  public String getQueueName() {
    return queueName;
  }

}
