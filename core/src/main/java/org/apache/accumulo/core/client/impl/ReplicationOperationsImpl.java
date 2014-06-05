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
package org.apache.accumulo.core.client.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ReplicationOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.replication.PeerExistsException;
import org.apache.accumulo.core.client.replication.PeerNotFoundException;
import org.apache.accumulo.core.client.replication.ReplicaSystem;
import org.apache.accumulo.core.client.replication.ReplicationTable;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.thrift.MasterClientService.Client;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 
 */
public class ReplicationOperationsImpl implements ReplicationOperations {
  private static final Logger log = LoggerFactory.getLogger(ReplicationOperationsImpl.class);

  private final Instance inst;
  private final Credentials creds;

  public ReplicationOperationsImpl(Instance inst, Credentials creds) {
    checkNotNull(inst);
    checkNotNull(creds);
    this.inst = inst;
    this.creds = creds;
  }

  @Override
  public void addPeer(String name, Class<? extends ReplicaSystem> system) throws AccumuloException, AccumuloSecurityException, PeerExistsException {
    checkNotNull(name);
    checkNotNull(system);

    addPeer(name, system.getName());
  }

  @Override
  public void addPeer(String name, ReplicaSystem system) throws AccumuloException, AccumuloSecurityException, PeerExistsException {
    checkNotNull(name);
    checkNotNull(system);

    addPeer(name, system.getClass().getName());
  }

  @Override
  public void addPeer(final String name, final String replicaType) throws AccumuloException, AccumuloSecurityException, PeerExistsException {
    checkNotNull(name);
    checkNotNull(replicaType);

    MasterClient.execute(inst, new ClientExec<Client>() {

      @Override
      public void execute(Client client) throws Exception {
        client.setSystemProperty(Tracer.traceInfo(), creds.toThrift(inst), Property.REPLICATION_PEERS.getKey() + name, replicaType);
      }

    });
  }

  @Override
  public void removePeer(final String name) throws AccumuloException, AccumuloSecurityException, PeerNotFoundException {
    checkNotNull(name);

    MasterClient.execute(inst, new ClientExec<Client>() {

      @Override
      public void execute(Client client) throws Exception {
        client.removeSystemProperty(Tracer.traceInfo(), creds.toThrift(inst), Property.REPLICATION_PEERS.getKey() + name);
      }

    });
  }

  @Override
  public void drain(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    checkNotNull(tableName);

    Set<String> wals = referencedFiles(tableName);

    drain(tableName, wals);
  }

  @Override
  public void drain(String tableName, Set<String> wals) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    checkNotNull(tableName);

    Connector conn = inst.getConnector(creds.getPrincipal(), creds.getToken());
    Text tableId = getTableId(conn, tableName);

    log.info("Waiting for {} to be replicated for {}", wals, tableId);

    log.info("Reading from metadata table");
    boolean allMetadataRefsReplicated = false;
    final Set<Range> range = Collections.singleton(new Range(ReplicationSection.getRange()));
    while (!allMetadataRefsReplicated) {
      BatchScanner bs = conn.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 4);
      bs.setRanges(range);
      bs.fetchColumnFamily(ReplicationSection.COLF);
      try {
        allMetadataRefsReplicated = allReferencesReplicated(bs, tableId, wals);
      } finally {
        bs.close();
      }

      if (!allMetadataRefsReplicated) {
        UtilWaitThread.sleep(1000);
      }
    }

    log.info("reading from replication table");
    boolean allReplicationRefsReplicated = false;
    while (!allReplicationRefsReplicated) {
      BatchScanner bs = conn.createBatchScanner(ReplicationTable.NAME, Authorizations.EMPTY, 4);
      bs.setRanges(Collections.singleton(new Range()));
      try {
        allReplicationRefsReplicated = allReferencesReplicated(bs, tableId, wals);
      } finally {
        bs.close();
      }

      if (!allReplicationRefsReplicated) {
        UtilWaitThread.sleep(1000);
      }
    }
  }

  /**
   * @return return true records are only in place which are fully replicated
   */
  protected boolean allReferencesReplicated(BatchScanner bs, Text tableId, Set<String> relevantLogs) {
    Text rowHolder = new Text(), colfHolder = new Text();
    for (Entry<Key,Value> entry : bs) {
      log.info("Got key {}", entry.getKey().toStringNoTruncate());

      entry.getKey().getColumnQualifier(rowHolder);
      if (tableId.equals(rowHolder)) {
        entry.getKey().getRow(rowHolder);
        entry.getKey().getColumnFamily(colfHolder);

        String file;
        if (colfHolder.equals(ReplicationSection.COLF)) {
          file = rowHolder.toString();
          file = file.substring(ReplicationSection.getRowPrefix().length());
        } else if (colfHolder.equals(OrderSection.NAME)) {
          file = OrderSection.getFile(entry.getKey(), rowHolder);
          long timeClosed = OrderSection.getTimeClosed(entry.getKey(), rowHolder);
          log.debug("Order section: {} and {}", timeClosed, file);
        } else {
          file = rowHolder.toString();
        }

        // Skip files that we didn't observe when we started (new files/data)
        if (!relevantLogs.contains(file)) {
          log.debug("Found file that we didn't care about {}", file);
          continue;
        } else {
          log.debug("Found file that we *do* care about {}", file);
        }

        try {
          Status stat = Status.parseFrom(entry.getValue().get());
          if (!StatusUtil.isFullyReplicated(stat)) {
            log.trace("{} and {} is not fully replicated", entry.getKey().getRow(), ProtobufUtil.toString(stat));
            return false;
          }
        } catch (InvalidProtocolBufferException e) {
          log.warn("Could not parse protobuf for {}", entry.getKey(), e);
        }
      }
    }

    return true;
  }

  protected Text getTableId(Connector conn, String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    TableOperations tops = conn.tableOperations();
    while (!tops.exists(ReplicationTable.NAME)) {
      UtilWaitThread.sleep(200);
    }

    if (!conn.tableOperations().exists(tableName)) {
      throw new TableNotFoundException(null, tableName, null);
    }

    String strTableId = null;
    while (null == strTableId) {
      strTableId = tops.tableIdMap().get(tableName);
      if (null == strTableId) {
        UtilWaitThread.sleep(200);
      }
    }

    return new Text(strTableId);    
  }

  @Override
  public Set<String> referencedFiles(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    checkNotNull(tableName);

    log.debug("Collecting referenced files for replication of table {}", tableName);

    Connector conn = inst.getConnector(creds.getPrincipal(), creds.getToken());
    Text tableId = getTableId(conn, tableName);

    log.debug("Found id of {} for name {}", tableId, tableName);

    // Get the WALs currently referenced by the table
    BatchScanner metaBs = conn.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 4);
    metaBs.setRanges(Collections.singleton(MetadataSchema.TabletsSection.getRange(tableId.toString())));
    metaBs.fetchColumnFamily(LogColumnFamily.NAME);
    Set<String> wals = new HashSet<>();
    try {
      for (Entry<Key,Value> entry : metaBs) {
        LogEntry logEntry = LogEntry.fromKeyValue(entry.getKey(), entry.getValue());
        for (String log : logEntry.logSet) {
          wals.add(new Path(log).toString());
        }
      }
    } finally {
      metaBs.close();
    }

    // And the WALs that need to be replicated for this table
    metaBs = conn.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 4);
    metaBs.setRanges(Collections.singleton(ReplicationSection.getRange()));
    metaBs.fetchColumnFamily(ReplicationSection.COLF);
    try {
      Text buffer = new Text();
      for (Entry<Key,Value> entry : metaBs) {
        ReplicationSection.getTableId(entry.getKey(), buffer);
        if (buffer.equals(tableId)) {
          ReplicationSection.getFile(entry.getKey(), buffer);
          wals.add(buffer.toString());
        }
      }
    } finally {
      metaBs.close();
    }

    return wals;
  }
}
