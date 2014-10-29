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

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.metrics.AbstractMetricsImpl;
import org.apache.accumulo.server.replication.ReplicationUtil;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.trace.instrument.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMX bindings to expose 'high-level' metrics about Replication
 */
public class ReplicationMetrics extends AbstractMetricsImpl implements ReplicationMetricsMBean {
  private static final Logger log = LoggerFactory.getLogger(ReplicationMetrics.class);
  private static final String METRICS_PREFIX = "replication";

  private Connector conn;
  private TableOperations tops;
  private ObjectName objectName = null;
  private ReplicationUtil replicationUtil;

  public ReplicationMetrics(Connector conn) throws MalformedObjectNameException {
    super();
    this.conn = conn;
    this.tops = conn.tableOperations();
    objectName = new ObjectName("accumulo.server.metrics:service=Replication Metrics,name=ReplicationMBean,instance=" + Thread.currentThread().getName());
    replicationUtil = new ReplicationUtil();
  }

  @Override
  public int getNumFilesPendingReplication() {
    if (!tops.exists(ReplicationTable.NAME)) {
      return 0;
    }

    Map<String,String> properties;
    try {
      properties = conn.instanceOperations().getSystemConfiguration();
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.debug("Could not extract system configuration", e);
      return 0;
    }

    // Get all of the configured replication peers
    Map<String,String> peers = replicationUtil.getPeers(properties);

    // A quick lookup to see if have any replication peer configured
    if (peers.isEmpty()) {
      return 0;
    }

    // The total set of configured targets
    Set<ReplicationTarget> allConfiguredTargets = replicationUtil.getReplicationTargets(conn.tableOperations());

    // Number of files per target we have to replicate
    Map<ReplicationTarget,Long> targetCounts = replicationUtil.getPendingReplications(conn);

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

  @Override
  public int getNumConfiguredPeers() {
    Map<String,String> properties;
    try {
      properties = conn.instanceOperations().getSystemConfiguration();
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.debug("Could not extract system configuration", e);
      return 0;
    }

    // Get all of the configured replication peers
    return replicationUtil.getPeers(properties).size();
  }

  @Override
  public int getMaxReplicationThreads() {
    MasterMonitorInfo mmi = null;
    for (int i = 0; i < 10; i++) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnection(HdfsZooInstance.getInstance());
        if (client != null) {
          mmi = client.getMasterStats(Tracer.traceInfo(), SystemCredentials.get().toThrift(HdfsZooInstance.getInstance()));
          break;
        }
      } catch (Exception e) {
        log.debug("Error fetching stats: " + e);
      } finally {
        if (client != null) {
          MasterClient.close(client);
        }
      }
    }

    if (null != mmi) {
      try {
        return replicationUtil.getMaxReplicationThreads(conn, mmi);
      } catch (AccumuloException e) {
        log.warn("Failed to fetch replication work queue size", e);
      } catch (AccumuloSecurityException e) {
        log.warn("Failed to fetch replication work queue size", e);
      }
    }

    log.warn("Could not fetch metrics information from Master");
    return -1;
  }

  @Override
  protected ObjectName getObjectName() {
    return objectName;
  }

  @Override
  protected String getMetricsPrefix() {
    return METRICS_PREFIX;
  }

}
