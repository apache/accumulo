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
package org.apache.accumulo.master.metrics;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.replication.ReplicationUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationMetrics extends MasterMetrics {

  private static final Logger log = LoggerFactory.getLogger(ReplicationMetrics.class);

  private final Master master;
  private final ReplicationUtil replicationUtil;
  private final MutableQuantiles replicationQueueTimeQuantiles;
  private final MutableStat replicationQueueTimeStat;
  private final Map<Path,Long> pathModTimes;

  ReplicationMetrics(Master master) {
    super("Replication", "Data-Center Replication Metrics", "MasterReplication");
    this.master = master;

    pathModTimes = new HashMap<>();

    replicationUtil = new ReplicationUtil(master.getContext());
    MetricsRegistry registry = super.getRegistry();
    replicationQueueTimeQuantiles = registry.newQuantiles("replicationQueue10m",
        "Replication queue time quantiles in milliseconds", "ops", "latency", 600);
    replicationQueueTimeStat = registry.newStat("replicationQueue",
        "Replication queue time statistics in milliseconds", "ops", "latency", true);
  }

  @Override
  protected void prepareMetrics() {
    final String PENDING_FILES = "filesPendingReplication";
    // Only add these metrics if the replication table is online and there are peers
    if (TableState.ONLINE == Tables.getTableState(master.getContext(), ReplicationTable.ID)
        && !replicationUtil.getPeers().isEmpty()) {
      getRegistry().add(PENDING_FILES, getNumFilesPendingReplication());
      addReplicationQueueTimeMetrics();
    } else {
      getRegistry().add(PENDING_FILES, 0);
    }

    getRegistry().add("numPeers", getNumConfiguredPeers());
    getRegistry().add("maxReplicationThreads", getMaxReplicationThreads());
  }

  protected long getNumFilesPendingReplication() {
    // The total set of configured targets
    Set<ReplicationTarget> allConfiguredTargets = replicationUtil.getReplicationTargets();

    // Number of files per target we have to replicate
    Map<ReplicationTarget,Long> targetCounts = replicationUtil.getPendingReplications();

    long filesPending = 0;

    // Sum pending replication over all targets
    for (ReplicationTarget configuredTarget : allConfiguredTargets) {
      Long numFiles = targetCounts.get(configuredTarget);

      if (numFiles != null) {
        filesPending += numFiles;
      }
    }

    return filesPending;
  }

  protected int getNumConfiguredPeers() {
    return replicationUtil.getPeers().size();
  }

  protected int getMaxReplicationThreads() {
    return replicationUtil.getMaxReplicationThreads(master.getMasterMonitorInfo());
  }

  protected void addReplicationQueueTimeMetrics() {
    Set<Path> paths = replicationUtil.getPendingReplicationPaths();

    // We'll take a snap of the current time and use this as a diff between any deleted
    // file's modification time and now. The reported latency will be off by at most a
    // number of seconds equal to the metric polling period
    long currentTime = getCurrentTime();

    // Iterate through all the pending paths and update the mod time if we don't know it yet
    for (Path path : paths) {
      if (!pathModTimes.containsKey(path)) {
        try {
          pathModTimes.put(path,
              master.getVolumeManager().getFileStatus(path).getModificationTime());
        } catch (IOException e) {
          // Ignore all IOExceptions
          // Either the system is unavailable or the file was deleted
          // since the initial scan and this check
          log.trace(
              "Failed to get file status for {}, file system is unavailable or it does not exist",
              path);
        }
      }
    }

    // Remove all currently pending files
    Set<Path> deletedPaths = new HashSet<>(pathModTimes.keySet());
    deletedPaths.removeAll(paths);

    // Exit early if we have no replicated files to report on
    if (deletedPaths.isEmpty()) {
      return;
    }

    replicationQueueTimeStat.resetMinMax();

    for (Path path : deletedPaths) {
      // Remove this path and add the latency
      Long modTime = pathModTimes.remove(path);
      if (modTime != null) {
        long diff = Math.max(0, currentTime - modTime);
        replicationQueueTimeQuantiles.add(diff);
        replicationQueueTimeStat.add(diff);
      }
    }
  }

  protected long getCurrentTime() {
    return System.currentTimeMillis();
  }
}
