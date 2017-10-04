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

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.replication.ReplicationUtil;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

/**
 *
 */
public class Metrics2ReplicationMetrics implements Metrics, MetricsSource {
  public static final String NAME = MASTER_NAME + ",sub=Replication", DESCRIPTION = "Data-Center Replication Metrics", CONTEXT = "master",
      RECORD = "MasterReplication";
  public static final String PENDING_FILES = "filesPendingReplication", NUM_PEERS = "numPeers", MAX_REPLICATION_THREADS = "maxReplicationThreads",
      LATENCY = "replicationLatency";

  private final Master master;
  private final MetricsSystem system;
  private final MetricsRegistry registry;
  private final ReplicationUtil replicationUtil;

  Metrics2ReplicationMetrics(Master master, MetricsSystem system) {
    this.master = master;
    this.system = system;

    registry = new MetricsRegistry(Interns.info(NAME, DESCRIPTION));
    replicationUtil = new ReplicationUtil(master);
  }

  protected void snapshot() {
    registry.add(PENDING_FILES, getNumFilesPendingReplication());
    registry.add(NUM_PEERS, getNumConfiguredPeers());
    registry.add(MAX_REPLICATION_THREADS, getMaxReplicationThreads());
    registry.add(LATENCY, getLatencyInSeconds());
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(RECORD).setContext(CONTEXT);

    snapshot();

    registry.snapshot(builder, all);
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
    if (TableState.ONLINE != Tables.getTableState(master.getInstance(), ReplicationTable.ID)) {
      return 0;
    }

    // Get all of the configured replication peers
    Map<String,String> peers = replicationUtil.getPeers();

    // A quick lookup to see if have any replication peer configured
    if (peers.isEmpty()) {
      return 0;
    }

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

  protected long getLatencyInSeconds() {
    long latency = master.getReplicationLatency();
    master.setReplicationLatency(0l);
    // convert to seconds
    return latency / 1000;
  }
}
