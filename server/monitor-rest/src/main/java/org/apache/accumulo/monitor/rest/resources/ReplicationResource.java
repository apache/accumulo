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
package org.apache.accumulo.monitor.rest.resources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.replication.ReplicaSystem;
import org.apache.accumulo.core.client.replication.ReplicaSystemFactory;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.api.ReplicationInformation;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 *
 */
@Path("/replication")
@Produces(MediaType.APPLICATION_JSON)
public class ReplicationResource {
  private static final Logger log = LoggerFactory.getLogger(ReplicationResource.class);

  @GET
  public List<ReplicationInformation> getReplicationInformation() throws AccumuloException, AccumuloSecurityException {
    Connector conn = Monitor.getContext().getConnector();

    TableOperations tops = conn.tableOperations();
    if (!tops.exists(ReplicationTable.NAME)) {
      return Collections.emptyList();
    }

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

      Iterable<Entry<String,String>> propertiesForTable;
      try {
        propertiesForTable = tops.getProperties(table);
      } catch (TableNotFoundException e) {
        log.warn("Could not fetch properties for {}", table, e);
        continue;
      }

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
    BatchScanner bs;
    try {
      bs = conn.createBatchScanner(ReplicationTable.NAME, Authorizations.EMPTY, 4);
    } catch (TableNotFoundException e) {
      // We verified that the replication table did exist, but was deleted befor we could read it
      log.error("Replication table was deleted", e);
      return Collections.emptyList();
    }

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

    List<ReplicationInformation> replicationInformation = new ArrayList<>();
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

      replicationInformation.add(new ReplicationInformation(tableName, configuredTarget.getPeerName(), configuredTarget.getRemoteIdentifier(),
          replicaSystemClass, (null == numFiles) ? 0 : numFiles));
    }

    return replicationInformation;
  }

  protected Map<String,String> invert(Map<String,String> map) {
    Map<String,String> newMap = Maps.newHashMapWithExpectedSize(map.size());
    for(Entry<String,String> entry : map.entrySet()) {
      newMap.put(entry.getValue(), entry.getKey());
    }
    return newMap;
  }
}
