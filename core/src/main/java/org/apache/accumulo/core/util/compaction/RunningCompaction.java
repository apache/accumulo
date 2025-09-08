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
import java.util.Objects;
import java.util.TreeMap;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

public class RunningCompaction {

  private final static Logger LOG = LoggerFactory.getLogger(RunningCompaction.class);

  private final TExternalCompactionJob job;
  private final ServerId compactor;
  private final Map<Long,TCompactionStatusUpdate> updates = new TreeMap<>();

  // If this object were to be added to a time sorted list before the start time
  // is set, then it will end up at the end of the list.
  private Long startTime = Long.MAX_VALUE;

  public RunningCompaction(TExternalCompactionJob job, ServerId compactor) {
    this.job = Objects.requireNonNull(job, "job cannot be null");
    this.compactor = Objects.requireNonNull(compactor, "compactor address cannot be null");
  }

  public RunningCompaction(TExternalCompaction tEC) {
    this(tEC.getJob(), ServerId.compactor(ResourceGroupId.of(tEC.getGroupName()),
        HostAndPort.fromString(tEC.getCompactor())));
  }

  public Map<Long,TCompactionStatusUpdate> getUpdates() {
    synchronized (updates) {
      return new TreeMap<>(updates);
    }
  }

  public void addUpdate(Long timestamp, TCompactionStatusUpdate update) {
    synchronized (updates) {
      this.updates.put(timestamp, update);
      if (update.getState() == TCompactionState.STARTED) {
        startTime = timestamp;
      }
    }
  }

  public TExternalCompactionJob getJob() {
    return job;
  }

  public ServerId getCompactor() {
    return compactor;
  }

  public boolean isStartTimeSet() {
    return startTime != Long.MAX_VALUE;
  }

  public Long getStartTime() {
    if (startTime == Long.MAX_VALUE) {
      LOG.warn("Programming error, RunningCompaction::startTime not set before being compared.");
    }
    return startTime;
  }

  public void setStartTime(Long time) {
    // Ignore update if startTime has already been set
    if (startTime == Long.MAX_VALUE) {
      startTime = time;
    }
  }

}
