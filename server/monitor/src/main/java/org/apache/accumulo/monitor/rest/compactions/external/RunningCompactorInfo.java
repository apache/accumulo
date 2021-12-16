/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.monitor.rest.compactions.external;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import jakarta.validation.constraints.NotNull;

import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunningCompactorInfo extends CompactorInfo {
  private static final Logger log = LoggerFactory.getLogger(RunningCompactorInfo.class);

  // Variable names become JSON keys
  public String ecid;
  public String kind;
  public String tableId;
  public int numFiles;
  public float progress = 0f;
  public long duration;
  public String status;
  public long lastUpdate;

  public RunningCompactorInfo() {
    super();
  }

  public RunningCompactorInfo(long fetchedTime, String ecid, @NotNull TExternalCompaction ec) {
    super(fetchedTime, ec.getQueueName(), ec.getCompactor());
    this.ecid = ecid;
    var updates = ec.getUpdates();
    var job = ec.getJob();
    kind = job.getKind().name();
    tableId = KeyExtent.fromThrift(job.extent).tableId().canonical();
    numFiles = job.files.size();
    updateProgress(updates);
    log.debug("Parsed running compaction {} for {} with progress = {}%", status, ecid, progress);
  }

  /**
   * Calculate progress: the percentage of bytesRead out of bytesToBeCompacted of the last update.
   * Also update the status.
   */
  private void updateProgress(Map<Long,TCompactionStatusUpdate> updates) {
    if (updates.isEmpty()) {
      progress = 0f;
      status = "na";
    }
    long nowMillis = System.currentTimeMillis();
    long startedMillis = nowMillis;
    long updateMillis;
    TCompactionStatusUpdate last;

    // sort updates by key, which is a timestamp
    TreeMap<Long,TCompactionStatusUpdate> sorted = new TreeMap<>(updates);
    var firstEntry = sorted.firstEntry();
    var lastEntry = sorted.lastEntry();
    if (firstEntry != null) {
      startedMillis = firstEntry.getKey();
    }
    duration = nowMillis - startedMillis;
    long durationMinutes = TimeUnit.MILLISECONDS.toMinutes(duration);
    if (durationMinutes > 15) {
      log.warn("Compaction {} has been running for {} minutes", ecid, durationMinutes);
    }

    // last entry is all we care about so bail if null
    if (lastEntry != null) {
      last = lastEntry.getValue();
      updateMillis = lastEntry.getKey();
    } else {
      log.debug("No updates found for {}", ecid);
      return;
    }

    long sinceLastUpdateSeconds = TimeUnit.MILLISECONDS.toSeconds(nowMillis - updateMillis);
    log.debug("Time since Last update {} - {} = {} seconds", nowMillis, updateMillis,
        sinceLastUpdateSeconds);
    if (sinceLastUpdateSeconds > 30) {
      log.debug("Compaction hasn't progressed from {} in {} seconds.", progress,
          sinceLastUpdateSeconds);
    }

    float percent;
    var total = last.getEntriesToBeCompacted();
    if (total <= 0) {
      percent = 0f;
    } else {
      percent = (last.getEntriesRead() / (float) total) * 100;
    }

    lastUpdate = nowMillis - updateMillis;
    status = last.state.name();
    progress = percent;
  }

  @Override
  public String toString() {
    return ecid + ": " + status + " progress: " + progress;
  }
}
