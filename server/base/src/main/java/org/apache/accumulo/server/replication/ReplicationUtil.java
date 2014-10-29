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

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
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
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.replication.ReplicaSystem;
import org.apache.accumulo.core.client.replication.ReplicaSystemFactory;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;

public class ReplicationUtil {
  private static final Logger log = LoggerFactory.getLogger(ReplicationUtil.class);

  private static final String REPLICATION_TARGET_PREFIX = Property.TABLE_REPLICATION_TARGET.getKey();

  private ZooCache zooCache;

  public ReplicationUtil() {
    this(new ZooCache());
  }

  public ReplicationUtil(ZooCache cache) {
    this.zooCache = cache;
  }

  public int getMaxReplicationThreads(Map<String,String> systemProperties, MasterMonitorInfo mmi) {
    int activeTservers = mmi.getTServerInfoSize();

    // The number of threads each tserver will use at most to replicate data
    int replicationThreadsPerServer = Integer.parseInt(systemProperties.get(Property.REPLICATION_WORKER_THREADS.getKey()));

    // The total number of "slots" we have to replicate data
    return activeTservers * replicationThreadsPerServer;
  }

  public int getMaxReplicationThreads(Connector conn, MasterMonitorInfo mmi) throws AccumuloException, AccumuloSecurityException {
    return getMaxReplicationThreads(conn.instanceOperations().getSystemConfiguration(), mmi);
  }

  /**
   * Extract replication peers from system configuration
   *
   * @param systemProperties
   *          System properties, typically from Connector.instanceOperations().getSystemConfiguration()
   * @return Configured replication peers
   */
  public Map<String,String> getPeers(Map<String,String> systemProperties) {
    Map<String,String> peers = new HashMap<>();
    String definedPeersPrefix = Property.REPLICATION_PEERS.getKey();

    // Get the defined peers and what ReplicaSystem impl they're using
    for (Entry<String,String> property : systemProperties.entrySet()) {
      String key = property.getKey();
      // Filter out cruft that we don't want
      if (key.startsWith(definedPeersPrefix) && !key.startsWith(Property.REPLICATION_PEER_USER.getKey())
          && !key.startsWith(Property.REPLICATION_PEER_PASSWORD.getKey())) {
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

    return peers;
  }

  public Set<ReplicationTarget> getReplicationTargets(TableOperations tops) {
    // The total set of configured targets
    final Set<ReplicationTarget> allConfiguredTargets = new HashSet<>();
    final Map<String,String> tableNameToId = tops.tableIdMap();

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
      } catch (AccumuloException e) {
        log.debug("Could not fetch properties for " + table, e);
        continue;
      } catch (TableNotFoundException e) {
        log.debug("Could not fetch properties for " + table, e);
        continue;
      }

      for (Entry<String,String> prop : propertiesForTable) {
        if (prop.getKey().startsWith(REPLICATION_TARGET_PREFIX)) {
          String peerName = prop.getKey().substring(REPLICATION_TARGET_PREFIX.length());
          String remoteIdentifier = prop.getValue();
          ReplicationTarget target = new ReplicationTarget(peerName, remoteIdentifier, localId);

          allConfiguredTargets.add(target);
        }
      }
    }

    return allConfiguredTargets;
  }

  public Map<ReplicationTarget,Long> getPendingReplications(Connector conn) {
    final Map<ReplicationTarget,Long> counts = new HashMap<>();

    // Read over the queued work
    BatchScanner bs;
    try {
      bs = conn.createBatchScanner(ReplicationTable.NAME, Authorizations.EMPTY, 4);
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
        s = conn.createScanner(ReplicationTable.NAME, Authorizations.EMPTY);
      } catch (TableNotFoundException e) {
        log.debug("Replication table no long exists", e);
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
    Map<String,String> newMap = Maps.newHashMapWithExpectedSize(map.size());
    for (Entry<String,String> entry : map.entrySet()) {
      newMap.put(entry.getValue(), entry.getKey());
    }
    return newMap;
  }

  public static synchronized void createReplicationTable(Connector conn) {
    TableOperations tops = conn.tableOperations();
    if (tops.exists(ReplicationTable.NAME)) {
      if (configureReplicationTable(conn)) {
        return;
      }
    }

    for (int i = 0; i < 5; i++) {
      try {
        if (!tops.exists(ReplicationTable.NAME)) {
          tops.create(ReplicationTable.NAME, false);
        }
        break;
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("Failed to create replication table", e);
      } catch (TableExistsException e) {
        // Shouldn't happen unless someone else made the table
      }
      log.error("Retrying table creation in 1 second...");
      UtilWaitThread.sleep(1000);
    }

    for (int i = 0; i < 5; i++) {
      if (configureReplicationTable(conn)) {
        return;
      }

      log.error("Failed to configure the replication table, retying...");
      UtilWaitThread.sleep(1000);
    }

    throw new RuntimeException("Could not configure replication table");
  }

  /**
   * Attempts to configure the replication table, will return false if it fails
   *
   * @param conn
   *          Connector for the instance
   * @return True if the replication table is properly configured
   */
  protected static synchronized boolean configureReplicationTable(Connector conn) {
    try {
      conn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.READ);
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.warn("Could not grant root user read access to replication table", e);
      // Should this be fatal? It's only for convenience, all r/w is done by !SYSTEM
    }

    TableOperations tops = conn.tableOperations();
    Map<String,EnumSet<IteratorScope>> iterators = null;
    try {
      iterators = tops.listIterators(ReplicationTable.NAME);
    } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
      log.error("Could not fetch iterators for " + ReplicationTable.NAME, e);
      return false;
    }

    if (!iterators.containsKey(ReplicationTable.COMBINER_NAME)) {
      // Set our combiner and combine all columns
      IteratorSetting setting = new IteratorSetting(30, ReplicationTable.COMBINER_NAME, StatusCombiner.class);
      Combiner.setColumns(setting, Arrays.asList(new Column(StatusSection.NAME), new Column(WorkSection.NAME)));
      try {
        tops.attachIterator(ReplicationTable.NAME, setting);
      } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
        log.error("Could not set StatusCombiner on replication table", e);
        return false;
      }
    }

    Map<String,Set<Text>> localityGroups;
    try {
      localityGroups = tops.getLocalityGroups(ReplicationTable.NAME);
    } catch (TableNotFoundException | AccumuloException e) {
      log.error("Could not fetch locality groups", e);
      return false;
    }

    Set<Text> statusColfams = localityGroups.get(ReplicationTable.STATUS_LG_NAME), workColfams = localityGroups.get(ReplicationTable.WORK_LG_NAME);
    if (null == statusColfams || null == workColfams) {
      try {
        tops.setLocalityGroups(ReplicationTable.NAME, ReplicationTable.LOCALITY_GROUPS);
      } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
        log.error("Could not set locality groups on replication table", e);
        return false;
      }
    }

    // Make sure the StatusFormatter is set on the metadata table
    Iterable<Entry<String,String>> properties;
    try {
      properties = tops.getProperties(ReplicationTable.NAME);
    } catch (AccumuloException | TableNotFoundException e) {
      log.error("Could not fetch table properties on replication table", e);
      return false;
    }

    boolean formatterConfigured = false;
    for (Entry<String,String> property : properties) {
      if (Property.TABLE_FORMATTER_CLASS.getKey().equals(property.getKey())) {
        if (!ReplicationTable.STATUS_FORMATTER_CLASS_NAME.equals(property.getValue())) {
          log.info("Changing formatter for {} table from {} to {}", ReplicationTable.NAME, property.getValue(), ReplicationTable.STATUS_FORMATTER_CLASS_NAME);
          try {
            tops.setProperty(ReplicationTable.NAME, Property.TABLE_FORMATTER_CLASS.getKey(), ReplicationTable.STATUS_FORMATTER_CLASS_NAME);
          } catch (AccumuloException | AccumuloSecurityException e) {
            log.error("Could not set formatter on replication table", e);
            return false;
          }
        }

        formatterConfigured = true;

        // Don't need to keep iterating over the properties after we found the one we were looking for
        break;
      }
    }

    if (!formatterConfigured) {
      try {
        tops.setProperty(ReplicationTable.NAME, Property.TABLE_FORMATTER_CLASS.getKey(), ReplicationTable.STATUS_FORMATTER_CLASS_NAME);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("Could not set formatter on replication table", e);
        return false;
      }
    }

    log.debug("Successfully configured replication table");

    return true;
  }

}
