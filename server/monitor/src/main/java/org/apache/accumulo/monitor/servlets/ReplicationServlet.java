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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.replication.ReplicaSystem;
import org.apache.accumulo.core.client.replication.ReplicaSystemFactory;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.monitor.util.Table;
import org.apache.accumulo.monitor.util.celltypes.NumberType;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.replication.DistributedWorkQueueWorkAssignerHelper;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 
 */
public class ReplicationServlet extends BasicServlet {
  private static final Logger log = LoggerFactory.getLogger(ReplicationServlet.class);

  private static final long serialVersionUID = 1L;

  private ZooCache zooCache = new ZooCache();

  @Override
  protected String getTitle(HttpServletRequest req) {
    return "Replication Overview";
  }
  
  @Override
  protected void pageBody(HttpServletRequest req, HttpServletResponse response, StringBuilder sb) throws Exception {
    Instance inst = HdfsZooInstance.getInstance();
    Credentials creds = SystemCredentials.get();
    Connector conn = inst.getConnector(creds.getPrincipal(), creds.getToken());

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

    Map<String,String> properties = conn.instanceOperations().getSystemConfiguration();
    Map<String,String> peers = new HashMap<>();
    String definedPeersPrefix = Property.REPLICATION_PEERS.getKey();

    // Get the defined peers and what ReplicaSystem impl they're using
    for (Entry<String,String> property : properties.entrySet()) {
      String key = property.getKey();
      // Filter out cruft that we don't want
      if (key.startsWith(definedPeersPrefix) && !key.startsWith(Property.REPLICATION_PEER_USER.getKey()) && !key.startsWith(Property.REPLICATION_PEER_PASSWORD.getKey())) {
        String peerName = property.getKey().substring(definedPeersPrefix.length());
        ReplicaSystem replica;
        try {
         replica = ReplicaSystemFactory.get(property.getValue());
        } catch (Exception e) {
          log.warn("Could not instantiate ReplicaSystem for {} with configuration {}", property.getKey(), property.getValue(), e);
          continue;
        }

        peers.put(peerName, replica.getClass().getName());
      }
    }

    final String targetPrefix = Property.TABLE_REPLICATION_TARGET.getKey();

    // The total set of configured targets
    Set<ReplicationTarget> allConfiguredTargets = new HashSet<>();

    // Number of files per target we have to replicate
    Map<ReplicationTarget,Long> targetCounts = new HashMap<>();

    Map<String,String> tableNameToId = tops.tableIdMap();
    Map<String,String> tableIdToName = invert(tableNameToId);

    for (String table : tops.list()) {
      if (MetadataTable.NAME.equals(table) || RootTable.NAME.equals(table)) {
        continue;
      }
      String localId = tableNameToId.get(table);
      if (null == localId) {
        log.trace("Could not determine ID for {}", table);
        continue;
      }

      Iterable<Entry<String,String>> propertiesForTable = tops.getProperties(table);
      for (Entry<String,String> prop : propertiesForTable) {
        if (prop.getKey().startsWith(targetPrefix)) {
          String peerName = prop.getKey().substring(targetPrefix.length());
          String remoteIdentifier = prop.getValue();
          ReplicationTarget target = new ReplicationTarget(peerName, remoteIdentifier, localId);

          allConfiguredTargets.add(target);
        }
      }
    }

    // Read over the queued work
    BatchScanner bs = conn.createBatchScanner(ReplicationConstants.TABLE_NAME, Authorizations.EMPTY, 4);
    bs.setRanges(Collections.singleton(new Range()));
    WorkSection.limit(bs);
    try {
      Text buffer = new Text();
      for (Entry<Key,Value> entry : bs) {
        Key k = entry.getKey();
        k.getColumnQualifier(buffer);
        ReplicationTarget target = ReplicationTarget.from(buffer);

        // TODO ACCUMULO-2835 once explicit lengths are tracked, we can give size-based estimates instead of just file-based
        Long count = targetCounts.get(target);
        if (null == count) {
          targetCounts.put(target, Long.valueOf(1l));
        } else {
          targetCounts.put(target, count + 1);
        }
      }
    } finally {
      bs.close();
    }

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

      replicationStats.addRow(tableName, configuredTarget.getPeerName(), configuredTarget.getRemoteIdentifier(), replicaSystemClass, (null == numFiles) ? 0 : numFiles); 
    }

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
  
        byte[] data = zooCache.get(workQueuePath + "/" + queueKey);
  
        // We could try to grep over the table, but without knowing the full file path, we
        // can't find the status quickly
        String status = "Unknown";
        String path = null;
        if (null != data) {
          path = new String(data);
          Scanner s = conn.createScanner(ReplicationConstants.TABLE_NAME, Authorizations.EMPTY);
          s.setRange(Range.exact(path));
          s.fetchColumn(WorkSection.NAME, target.toText());
    
          // Fetch the work entry for this item
          Entry<Key,Value> kv = null;
          try {
            kv = Iterables.getOnlyElement(s);
          } catch (NoSuchElementException e) {
           log.trace("Could not find status of {} replicating to {}", filename, target);
           status = "Unknown";
          } finally {
            s.close();
          }
    
          // If we found the work entry for it, try to compute some progress
          if (null != kv) {
            try {
              Status stat = Status.parseFrom(kv.getValue().get());
              if (StatusUtil.isFullyReplicated(stat)) {
                status = "Finished";
              } else {
                if (stat.getInfiniteEnd()) {
                  status = stat.getBegin() + "/&infin; records";
                } else {
                  status = stat.getBegin() + "/" + stat.getEnd() + " records";
                }
              }
            } catch (InvalidProtocolBufferException e) {
              log.warn("Could not deserialize protobuf for {}", kv.getKey(), e);
              status = "Unknown";
            }
          }
        }
  
        // Add a row in the table
        replicationInProgress.addRow(null == path ? ".../" + filename : path, target.getPeerName(), target.getSourceTableId(), target.getRemoteIdentifier(), status);
      }
    } catch (KeeperException | InterruptedException e) {
      log.warn("Could not calculate replication in progress", e);
    }

    replicationInProgress.generate(req, sb);
  }

  protected Map<String,String> invert(Map<String,String> map) {
    Map<String,String> newMap = Maps.newHashMapWithExpectedSize(map.size());
    for(Entry<String,String> entry : map.entrySet()) {
      newMap.put(entry.getValue(), entry.getKey());
    }
    return newMap;
  }
}
