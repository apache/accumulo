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
package org.apache.accumulo.server.replication;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationUtil {
  private static final Logger log = LoggerFactory.getLogger(ReplicationUtil.class);
  public static final String STATUS_FORMATTER_CLASS_NAME = StatusFormatter.class.getName();

  private final ServerContext context;
  private final ReplicaSystemFactory factory;

  public ReplicationUtil(ServerContext context) {
    this(context, new ReplicaSystemFactory());
  }

  public ReplicationUtil(ServerContext context, ReplicaSystemFactory factory) {
    this.context = context;
    this.factory = factory;
  }

  public int getMaxReplicationThreads(MasterMonitorInfo mmi) {
    int activeTservers = mmi.getTServerInfoSize();

    // The number of threads each tserver will use at most to replicate data
    int replicationThreadsPerServer =
        Integer.parseInt(context.getConfiguration().get(Property.REPLICATION_WORKER_THREADS));

    // The total number of "slots" we have to replicate data
    return activeTservers * replicationThreadsPerServer;
  }

  /**
   * Extract replication peers from system configuration
   *
   * @return Configured replication peers
   */
  public Map<String,String> getPeers() {
    Map<String,String> peers = new HashMap<>();

    // Get the defined peers and what ReplicaSystem impl they're using
    for (Entry<String,String> property : context.getConfiguration()
        .getAllPropertiesWithPrefix(Property.REPLICATION_PEERS).entrySet()) {
      String key = property.getKey();
      // Filter out cruft that we don't want
      if (!key.startsWith(Property.REPLICATION_PEER_USER.getKey())
          && !key.startsWith(Property.REPLICATION_PEER_PASSWORD.getKey())
          && !key.startsWith(Property.REPLICATION_PEER_KEYTAB.getKey())) {
        String peerName = property.getKey().substring(Property.REPLICATION_PEERS.getKey().length());
        Entry<String,String> entry;
        try {
          entry = factory.parseReplicaSystemConfiguration(property.getValue());
        } catch (Exception e) {
          log.warn("Could not instantiate ReplicaSystem for {} with configuration {}",
              property.getKey(), property.getValue(), e);
          continue;
        }

        peers.put(peerName, entry.getKey());
      }
    }

    return peers;
  }

  public Set<ReplicationTarget> getReplicationTargets() {
    // The total set of configured targets
    final Set<ReplicationTarget> allConfiguredTargets = new HashSet<>();
    final Map<String,TableId> tableNameToId = Tables.getNameToIdMap(context);

    for (String table : tableNameToId.keySet()) {
      if (MetadataTable.NAME.equals(table) || RootTable.NAME.equals(table)) {
        continue;
      }

      TableId localId = tableNameToId.get(table);
      if (localId == null) {
        log.trace("Could not determine ID for {}", table);
        continue;
      }

      TableConfiguration tableConf = context.getTableConfiguration(localId);
      if (tableConf == null) {
        log.trace("Could not get configuration for table {} (it no longer exists)", table);
        continue;
      }

      for (Entry<String,String> prop : tableConf
          .getAllPropertiesWithPrefix(Property.TABLE_REPLICATION_TARGET).entrySet()) {
        String peerName =
            prop.getKey().substring(Property.TABLE_REPLICATION_TARGET.getKey().length());
        String remoteIdentifier = prop.getValue();
        ReplicationTarget target = new ReplicationTarget(peerName, remoteIdentifier, localId);

        allConfiguredTargets.add(target);
      }
    }

    return allConfiguredTargets;
  }

  public Map<ReplicationTarget,Long> getPendingReplications() {
    final Map<ReplicationTarget,Long> counts = new HashMap<>();

    // Read over the queued work
    BatchScanner bs;
    try {
      bs = context.createBatchScanner(ReplicationTable.NAME, Authorizations.EMPTY, 4);
    } catch (TableNotFoundException e) {
      log.debug("No replication table exists", e);
      return counts;
    }

    bs.setRanges(Collections.singleton(new Range()));
    WorkSection.limit(bs);
    try {
      Text buffer = new Text();
      for (Entry<Key,Value> entry : bs) {
        Key k = entry.getKey();
        k.getColumnQualifier(buffer);
        ReplicationTarget target = ReplicationTarget.from(buffer);

        // TODO ACCUMULO-2835 once explicit lengths are tracked, we can give size-based estimates
        // instead of just file-based
        Long count = counts.get(target);
        if (count == null) {
          counts.put(target, 1L);
        } else {
          counts.put(target, count + 1);
        }
      }
    } finally {
      bs.close();
    }

    return counts;
  }

  public Set<Path> getPendingReplicationPaths() {
    final Set<Path> paths = new HashSet<>();

    // Read over the queued work
    BatchScanner bs;
    try {
      bs = context.createBatchScanner(ReplicationTable.NAME, Authorizations.EMPTY, 4);
    } catch (TableNotFoundException e) {
      log.debug("No replication table exists", e);
      return paths;
    }

    bs.setRanges(Collections.singleton(new Range()));
    StatusSection.limit(bs);
    try {
      Text buffer = new Text();
      for (Entry<Key,Value> entry : bs) {
        Key k = entry.getKey();
        k.getRow(buffer);
        paths.add(new Path(buffer.toString()));
      }
    } finally {
      bs.close();
    }

    return paths;
  }

}
