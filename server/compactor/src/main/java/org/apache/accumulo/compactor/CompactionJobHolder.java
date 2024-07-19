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
package org.apache.accumulo.compactor;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.grpc.compaction.protobuf.PCompactionStats;
import org.apache.accumulo.grpc.compaction.protobuf.PExternalCompactionJob;
import org.apache.accumulo.server.compaction.FileCompactor;

public class CompactionJobHolder {

  private PExternalCompactionJob job;
  private Thread compactionThread;
  private AtomicReference<FileCompactor> compactor;
  private volatile boolean cancelled = false;
  private volatile PCompactionStats stats = null;

  CompactionJobHolder() {}

  public synchronized void reset() {
    job = null;
    compactionThread = null;
    compactor = null;
    cancelled = false;
    stats = null;
  }

  public synchronized PExternalCompactionJob getJob() {
    return job;
  }

  public TableId getTableId() {
    var pKeyExtent = getJob().getExtent();
    return KeyExtent.fromProtobuf(pKeyExtent).tableId();
  }

  public PCompactionStats getStats() {
    return stats;
  }

  public void setStats(PCompactionStats stats) {
    this.stats = stats;
  }

  public synchronized boolean cancel(String extCompId) {
    if (isSet() && getJob().getExternalCompactionId().equals(extCompId)) {
      cancelled = true;
      if (compactor.get() != null) {
        compactor.get().interrupt();
      }
      compactionThread.interrupt();
      return true;
    }
    return false;
  }

  public boolean isCancelled() {
    return cancelled;
  }

  public synchronized boolean isSet() {
    return (null != this.job);
  }

  public synchronized void set(PExternalCompactionJob job, Thread compactionThread,
      AtomicReference<FileCompactor> compactor) {
    Objects.requireNonNull(job, "CompactionJob is null");
    Objects.requireNonNull(compactionThread, "Compaction thread is null");
    Objects.requireNonNull(compactor, "Compactor object is null");
    this.job = job;
    this.compactionThread = compactionThread;
    this.compactor = compactor;
  }

}
