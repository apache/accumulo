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
package org.apache.accumulo.monitor.servlets;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.util.Table;
import org.apache.accumulo.monitor.util.celltypes.NumberType;
import org.apache.accumulo.server.replication.DistributedWorkQueueWorkAssignerHelper;
import org.apache.accumulo.server.replication.ReplicationUtil;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ReplicationServlet extends BasicServlet {
  private static final Logger log = LoggerFactory.getLogger(ReplicationServlet.class);

  private static final long serialVersionUID = 1L;

  // transient because it's not serializable and servlets are serializable
  private transient volatile ReplicationUtil replicationUtil = null;

  private synchronized ReplicationUtil getReplicationUtil() {
    // make transient replicationUtil available as needed
    if (replicationUtil == null) {
      replicationUtil = new ReplicationUtil(Monitor.getContext());
    }
    return replicationUtil;
  }

  @Override
  protected String getTitle(HttpServletRequest req) {
    return "Replication Overview";
  }

  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse response, StringBuilder sb) throws Exception {
    final Connector conn = Monitor.getContext().getConnector();
    final MasterMonitorInfo mmi = Monitor.getMmi();

    // The total number of "slots" we have to replicate data
    int totalWorkQueueSize = getReplicationUtil().getMaxReplicationThreads(mmi);

    TableOperations tops = conn.tableOperations();
    if (!ReplicationTable.isOnline(conn)) {
      banner(sb, "", "Replication table is offline");
      return;
    }

    Table replicationStats = new Table("replicationStats", "Replication Status");
    replicationStats.addSortableColumn("Table");
    replicationStats.addSortableColumn("Peer");
    replicationStats.addSortableColumn("Remote Identifier");
    replicationStats.addSortableColumn("ReplicaSystem Type");
    replicationStats.addSortableColumn("Files needing replication", new NumberType<Long>(), null);

    Map<String,String> peers = getReplicationUtil().getPeers();

    // The total set of configured targets
    Set<ReplicationTarget> allConfiguredTargets = getReplicationUtil().getReplicationTargets();

    // Number of files per target we have to replicate
    Map<ReplicationTarget,Long> targetCounts = getReplicationUtil().getPendingReplications();

    Map<String,String> tableNameToId = tops.tableIdMap();
    Map<String,String> tableIdToName = getReplicationUtil().invert(tableNameToId);

    long filesPendingOverAllTargets = 0l;
    for (ReplicationTarget configuredTarget : allConfiguredTargets) {
      String tableName = tableIdToName.get(configuredTarget.getSourceTableId());
      if (null == tableName) {
        log.trace("Could not determine table name from id {}", configuredTarget.getSourceTableId());
        continue;
      }

      String replicaSystemClass = peers.get(configuredTarget.getPeerName());
      if (null == replicaSystemClass) {
        log.trace("Could not determine configured ReplicaSystem for {}", configuredTarget.getPeerName());
        continue;
      }

      Long numFiles = targetCounts.get(configuredTarget);

      if (null == numFiles) {
        replicationStats.addRow(tableName, configuredTarget.getPeerName(), configuredTarget.getRemoteIdentifier(), replicaSystemClass, 0);
      } else {
        replicationStats.addRow(tableName, configuredTarget.getPeerName(), configuredTarget.getRemoteIdentifier(), replicaSystemClass, numFiles);
        filesPendingOverAllTargets += numFiles;
      }
    }

    // Up to 2x the number of slots for replication available, WARN
    // More than 2x the number of slots for replication available, ERROR
    NumberType<Long> filesPendingFormat = new NumberType<Long>(Long.valueOf(0), Long.valueOf(2 * totalWorkQueueSize), Long.valueOf(0),
        Long.valueOf(4 * totalWorkQueueSize));

    String utilization = filesPendingFormat.format(filesPendingOverAllTargets);

    sb.append("<div><center><br/><span class=\"table-caption\">Total files pending replication: ").append(utilization).append("</span></center></div>");

    replicationStats.generate(req, sb);

    // Make a table for the replication data in progress
    Table replicationInProgress = new Table("replicationInProgress", "In-Progress Replication");
    replicationInProgress.addSortableColumn("File");
    replicationInProgress.addSortableColumn("Peer");
    replicationInProgress.addSortableColumn("Source Table ID");
    replicationInProgress.addSortableColumn("Peer Identifier");
    replicationInProgress.addUnsortableColumn("Status");

    // Read the files from the workqueue in zk
    String zkRoot = ZooUtil.getRoot(Monitor.getContext().getInstance());
    final String workQueuePath = zkRoot + ReplicationConstants.ZOO_WORK_QUEUE;

    DistributedWorkQueue workQueue = new DistributedWorkQueue(workQueuePath, Monitor.getContext().getConfiguration());

    try {
      for (String queueKey : workQueue.getWorkQueued()) {
        Entry<String,ReplicationTarget> queueKeyPair = DistributedWorkQueueWorkAssignerHelper.fromQueueKey(queueKey);
        String filename = queueKeyPair.getKey();
        ReplicationTarget target = queueKeyPair.getValue();

        String path = getReplicationUtil().getAbsolutePath(conn, workQueuePath, queueKey);
        String progress = getReplicationUtil().getProgress(conn, path, target);

        // Add a row in the table
        replicationInProgress.addRow(null == path ? ".../" + filename : path, target.getPeerName(), target.getSourceTableId(), target.getRemoteIdentifier(),
            progress);
      }
    } catch (KeeperException | InterruptedException e) {
      log.warn("Could not calculate replication in progress", e);
    }

    replicationInProgress.generate(req, sb);
  }
}
