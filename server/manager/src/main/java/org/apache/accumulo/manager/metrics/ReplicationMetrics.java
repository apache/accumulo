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
package org.apache.accumulo.manager.metrics;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.replication.ReplicationUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Deprecated
public class ReplicationMetrics implements MetricsProducer {

  private static final Logger log = LoggerFactory.getLogger(ReplicationMetrics.class);

  private final Manager manager;
  private final ReplicationUtil replicationUtil;
  private final Map<Path,Long> pathModTimes;

  private Timer replicationQueueTimer;
  private AtomicLong pendingFiles;
  private AtomicInteger numPeers;
  private AtomicInteger maxReplicationThreads;

  ReplicationMetrics(Manager manager) {
    this.manager = manager;
    pathModTimes = new HashMap<>();
    replicationUtil = new ReplicationUtil(manager.getContext());
  }

  protected void update() {
    // Only add these metrics if the replication table is online and there are peers
    if (TableState.ONLINE == manager.getContext().getTableState(ReplicationTable.ID)
        && !replicationUtil.getPeers().isEmpty()) {
      pendingFiles.set(getNumFilesPendingReplication());
      addReplicationQueueTimeMetrics();
    } else {
      pendingFiles.set(0);
    }
    numPeers.set(getNumConfiguredPeers());
    maxReplicationThreads.set(getMaxReplicationThreads());
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
    return replicationUtil.getMaxReplicationThreads(manager.getManagerMonitorInfo());
  }

  protected void addReplicationQueueTimeMetrics() {
    Set<Path> paths = replicationUtil.getPendingReplicationPaths();

    // We'll take a snap of the current time and use this as a diff between any deleted
    // file's modification time and now. The reported latency will be off by at most a
    // number of seconds equal to the metric polling period
    long currentTime = System.currentTimeMillis();

    // Iterate through all the pending paths and update the mod time if we don't know it yet
    for (Path path : paths) {
      if (!pathModTimes.containsKey(path)) {
        try {
          pathModTimes.put(path,
              manager.getVolumeManager().getFileStatus(path).getModificationTime());
        } catch (IOException e) {
          // Ignore all IOExceptions
          // Either the system is unavailable, or the file was deleted since the initial scan and
          // this check
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

    for (Path path : deletedPaths) {
      // Remove this path and add the latency
      Long modTime = pathModTimes.remove(path);
      if (modTime != null) {
        long diff = Math.max(0, currentTime - modTime);
        // micrometer timer
        replicationQueueTimer.record(Duration.ofMillis(diff));
      }
    }
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    replicationQueueTimer = registry.timer(METRICS_REPLICATION_QUEUE);
    pendingFiles = registry.gauge(METRICS_REPLICATION_PENDING_FILES, new AtomicLong(0));
    numPeers = registry.gauge(METRICS_REPLICATION_PEERS, new AtomicInteger(0));
    maxReplicationThreads = registry.gauge(METRICS_REPLICATION_THREADS, new AtomicInteger(0));

    ScheduledExecutorService scheduler = ThreadPools.getServerThreadPools()
        .createScheduledExecutorService(1, "replicationMetricsPoller");
    Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdownNow));
    long minimumRefreshDelay = TimeUnit.SECONDS.toMillis(5);
    ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(this::update, minimumRefreshDelay,
        minimumRefreshDelay, TimeUnit.MILLISECONDS);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

}
