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
package org.apache.accumulo.server.replication;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTableOfflineException;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.protobuf.InvalidProtocolBufferException;

public class ReplicationUtil {
  private static final Logger log = LoggerFactory.getLogger(ReplicationUtil.class);
  public static final String STATUS_FORMATTER_CLASS_NAME = StatusFormatter.class.getName();

  private final AccumuloServerContext context;
  private final ZooCache zooCache;
  private final ReplicaSystemFactory factory;

  public ReplicationUtil(AccumuloServerContext context) {
    this(context, new ZooCache(), new ReplicaSystemFactory());
  }

  public ReplicationUtil(AccumuloServerContext context, ZooCache cache, ReplicaSystemFactory factory) {
    this.zooCache = cache;
    this.context = context;
    this.factory = factory;
  }

  public int getMaxReplicationThreads(MasterMonitorInfo mmi) {
    int activeTservers = mmi.getTServerInfoSize();

    // The number of threads each tserver will use at most to replicate data
    int replicationThreadsPerServer = Integer.parseInt(context.getConfiguration().get(Property.REPLICATION_WORKER_THREADS));

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
    for (Entry<String,String> property : context.getConfiguration().getAllPropertiesWithPrefix(Property.REPLICATION_PEERS).entrySet()) {
      String key = property.getKey();
      // Filter out cruft that we don't want
      if (!key.startsWith(Property.REPLICATION_PEER_USER.getKey()) && !key.startsWith(Property.REPLICATION_PEER_PASSWORD.getKey())
          && !key.startsWith(Property.REPLICATION_PEER_KEYTAB.getKey())) {
        String peerName = property.getKey().substring(Property.REPLICATION_PEERS.getKey().length());
        Entry<String,String> entry;
        try {
          entry = factory.parseReplicaSystemConfiguration(property.getValue());
        } catch (Exception e) {
          log.warn("Could not instantiate ReplicaSystem for {} with configuration {}", property.getKey(), property.getValue(), e);
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
    final Map<String,Table.ID> tableNameToId = Tables.getNameToIdMap(context.getInstance());

    for (String table : tableNameToId.keySet()) {
      if (MetadataTable.NAME.equals(table) || RootTable.NAME.equals(table)) {
        continue;
      }

      Table.ID localId = tableNameToId.get(table);
      if (null == localId) {
        log.trace("Could not determine ID for {}", table);
        continue;
      }

      TableConfiguration tableConf = context.getServerConfigurationFactory().getTableConfiguration(localId);
      if (null == tableConf) {
        log.trace("Could not get configuration for table {} (it no longer exists)", table);
        continue;
      }

      for (Entry<String,String> prop : tableConf.getAllPropertiesWithPrefix(Property.TABLE_REPLICATION_TARGET).entrySet()) {
        String peerName = prop.getKey().substring(Property.TABLE_REPLICATION_TARGET.getKey().length());
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
      bs = context.getConnector().createBatchScanner(ReplicationTable.NAME, Authorizations.EMPTY, 4);
    } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
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

        // TODO ACCUMULO-2835 once explicit lengths are tracked, we can give size-based estimates instead of just file-based
        Long count = counts.get(target);
        if (null == count) {
          counts.put(target, Long.valueOf(1l));
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
      bs = context.getConnector().createBatchScanner(ReplicationTable.NAME, Authorizations.EMPTY, 4);
    } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
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

  /**
   * Fetches the absolute path of the file to be replicated.
   *
   * @param conn
   *          Accumulo Connector
   * @param workQueuePath
   *          Root path for the Replication WorkQueue
   * @param queueKey
   *          The Replication work queue key
   * @return The absolute path for the file, or null if the key is no longer in ZooKeeper
   */
  public String getAbsolutePath(Connector conn, String workQueuePath, String queueKey) {
    byte[] data = zooCache.get(workQueuePath + "/" + queueKey);
    if (null != data) {
      return new String(data, UTF_8);
    }

    return null;
  }

  /**
   * Compute a progress string for the replication of the given WAL
   *
   * @param conn
   *          Accumulo Connector
   * @param path
   *          Absolute path to a WAL, or null
   * @param target
   *          ReplicationTarget the WAL is being replicated to
   * @return A status message for a file being replicated
   */
  public String getProgress(Connector conn, String path, ReplicationTarget target) {
    // We could try to grep over the table, but without knowing the full file path, we
    // can't find the status quickly
    String status = "Unknown";
    if (null != path) {
      Scanner s;
      try {
        s = ReplicationTable.getScanner(conn);
      } catch (ReplicationTableOfflineException e) {
        log.debug("Replication table no longer online", e);
        return status;
      }

      s.setRange(Range.exact(path));
      s.fetchColumn(WorkSection.NAME, target.toText());

      // Fetch the work entry for this item
      Entry<Key,Value> kv = null;
      try {
        kv = Iterables.getOnlyElement(s);
      } catch (NoSuchElementException e) {
        log.trace("Could not find status of {} replicating to {}", path, target);
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

    return status;
  }

  public Map<String,String> invert(Map<String,String> map) {
    Map<String,String> newMap = new HashMap<>(map.size());
    for (Entry<String,String> entry : map.entrySet()) {
      newMap.put(entry.getValue(), entry.getKey());
    }
    return newMap;
  }

}
