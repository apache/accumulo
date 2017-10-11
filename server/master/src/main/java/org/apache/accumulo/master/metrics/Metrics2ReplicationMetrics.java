/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.master.metrics;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.replication.ReplicationUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Metrics2ReplicationMetrics implements Metrics, MetricsSource {
  public static final String NAME = MASTER_NAME + ",sub=Replication", DESCRIPTION = "Data-Center Replication Metrics", CONTEXT = "master",
      RECORD = "MasterReplication";
  public static final String PENDING_FILES = "filesPendingReplication", NUM_PEERS = "numPeers", MAX_REPLICATION_THREADS = "maxReplicationThreads",
      REPLICATION_QUEUE_TIME_QUANTILES = "replicationQueue10m", REPLICATION_QUEUE_TIME = "replicationQueue";

  private final static Logger log = LoggerFactory.getLogger(Metrics2ReplicationMetrics.class);

  private final Master master;
  private final MetricsSystem system;
  private final MetricsRegistry registry;
  private final ReplicationUtil replicationUtil;
  private final MutableQuantiles replicationQueueTimeQuantiles;
  private final MutableStat replicationQueueTimeStat;
  private final Map<Path,Long> pathModTimes;

  Metrics2ReplicationMetrics(Master master, MetricsSystem system) {
    this.master = master;
    this.system = system;

    pathModTimes = new HashMap<>();

    registry = new MetricsRegistry(Interns.info(NAME, DESCRIPTION));
    replicationUtil = new ReplicationUtil(master);
    replicationQueueTimeQuantiles = registry.newQuantiles(REPLICATION_QUEUE_TIME_QUANTILES, "Replication queue time quantiles in milliseconds", "ops",
        "latency", 600);
    replicationQueueTimeStat = registry.newStat(REPLICATION_QUEUE_TIME, "Replication queue time statistics in milliseconds", "ops", "latency", true);
  }

  protected void snapshot() {
    // Only add these metrics if the replication table is online and there are peers
    if (TableState.ONLINE == Tables.getTableState(master.getInstance(), ReplicationTable.ID) && !replicationUtil.getPeers().isEmpty()) {
      registry.add(PENDING_FILES, getNumFilesPendingReplication());
      addReplicationQueueTimeMetrics();
    } else {
      registry.add(PENDING_FILES, 0);
    }

    registry.add(NUM_PEERS, getNumConfiguredPeers());
    registry.add(MAX_REPLICATION_THREADS, getMaxReplicationThreads());
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(RECORD).setContext(CONTEXT);

    snapshot();

    registry.snapshot(builder, all);
    replicationQueueTimeQuantiles.snapshot(builder, all);
    replicationQueueTimeStat.snapshot(builder, all);
  }

  @Override
  public void register() throws Exception {
    system.register(NAME, DESCRIPTION, this);
  }

  @Override
  public void add(String name, long time) {
    throw new UnsupportedOperationException("add() is not implemented");
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  protected int getNumFilesPendingReplication() {
    // The total set of configured targets
    Set<ReplicationTarget> allConfiguredTargets = replicationUtil.getReplicationTargets();

    // Number of files per target we have to replicate
    Map<ReplicationTarget,Long> targetCounts = replicationUtil.getPendingReplications();

    int filesPending = 0;

    // Sum pending replication over all targets
    for (ReplicationTarget configuredTarget : allConfiguredTargets) {
      Long numFiles = targetCounts.get(configuredTarget);

      if (null != numFiles) {
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
          pathModTimes.put(path, master.getFileSystem().getFileStatus(path).getModificationTime());
        } catch (IOException e) {
          // Ignore all IOExceptions
          // Either the system is unavailable or the file was deleted
          // since the initial scan and this check
          log.trace("Failed to get file status for {}, file system is unavailable or it does not exist", path);
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
