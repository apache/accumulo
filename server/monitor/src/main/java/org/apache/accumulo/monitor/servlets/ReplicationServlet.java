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
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.util.Table;
import org.apache.accumulo.monitor.util.celltypes.NumberType;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.replication.DistributedWorkQueueWorkAssignerHelper;
import org.apache.accumulo.server.replication.ReplicationUtil;
import org.apache.accumulo.server.security.SystemCredentials;
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

  private ReplicationUtil replicationUtil;

  public ReplicationServlet() {
    replicationUtil = new ReplicationUtil();
  }

  @Override
  protected String getTitle(HttpServletRequest req) {
    return "Replication Overview";
  }

  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse response, StringBuilder sb) throws Exception {
    final Instance inst = HdfsZooInstance.getInstance();
    final Credentials creds = SystemCredentials.get();
    final Connector conn = inst.getConnector(creds.getPrincipal(), creds.getToken());
    final Map<String,String> systemProps = conn.instanceOperations().getSystemConfiguration();
    final MasterMonitorInfo mmi = Monitor.getMmi();

    // The total number of "slots" we have to replicate data
    int totalWorkQueueSize = replicationUtil.getMaxReplicationThreads(systemProps, mmi);

    TableOperations tops = conn.tableOperations();
    if (!tops.exists(ReplicationConstants.TABLE_NAME)) {
      banner(sb, "", "Replication table does not yet exist");
      return;
    }

    Table replicationStats = new Table("replicationStats", "Replication Status");
    replicationStats.addSortableColumn("Table");
    replicationStats.addSortableColumn("Peer");
    replicationStats.addSortableColumn("Remote Identifier");
    replicationStats.addSortableColumn("ReplicaSystem Type");
    replicationStats.addSortableColumn("Files needing replication", new NumberType<Long>(), null);

    Map<String,String> peers = replicationUtil.getPeers(systemProps);

    // The total set of configured targets
    Set<ReplicationTarget> allConfiguredTargets = replicationUtil.getReplicationTargets(tops);

    // Number of files per target we have to replicate
    Map<ReplicationTarget,Long> targetCounts = replicationUtil.getPendingReplications(conn);

    Map<String,String> tableNameToId = tops.tableIdMap();
    Map<String,String> tableIdToName = replicationUtil.invert(tableNameToId);

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
    String zkRoot = ZooUtil.getRoot(inst);
    final String workQueuePath = zkRoot + ReplicationConstants.ZOO_WORK_QUEUE;

    DistributedWorkQueue workQueue = new DistributedWorkQueue(workQueuePath, new ServerConfigurationFactory(inst).getConfiguration());

    try {
      for (String queueKey : workQueue.getWorkQueued()) {
        Entry<String,ReplicationTarget> queueKeyPair = DistributedWorkQueueWorkAssignerHelper.fromQueueKey(queueKey);
        String filename = queueKeyPair.getKey();
        ReplicationTarget target = queueKeyPair.getValue();

        String path = replicationUtil.getAbsolutePath(conn, workQueuePath, queueKey);
        String progress = replicationUtil.getProgress(conn, path, target);

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
