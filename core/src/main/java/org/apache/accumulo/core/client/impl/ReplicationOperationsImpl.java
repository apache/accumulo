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
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ReplicationOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.replication.PeerExistsException;
import org.apache.accumulo.core.client.replication.PeerNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.thrift.MasterClientService.Client;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationOperationsImpl implements ReplicationOperations {
  private static final Logger log = LoggerFactory.getLogger(ReplicationOperationsImpl.class);

  private final ClientContext context;

  public ReplicationOperationsImpl(ClientContext context) {
    checkNotNull(context);
    this.context = context;
  }

  @Override
  public void addPeer(final String name, final String replicaType) throws AccumuloException, AccumuloSecurityException, PeerExistsException {
    checkNotNull(name);
    checkNotNull(replicaType);
    context.getConnector().instanceOperations().setProperty(Property.REPLICATION_PEERS.getKey() + name, replicaType);
  }

  @Override
  public void removePeer(final String name) throws AccumuloException, AccumuloSecurityException, PeerNotFoundException {
    checkNotNull(name);
    context.getConnector().instanceOperations().removeProperty(Property.REPLICATION_PEERS.getKey() + name);
  }

  @Override
  public void drain(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    checkNotNull(tableName);

    Set<String> wals = referencedFiles(tableName);

    drain(tableName, wals);
  }

  @Override
  public void drain(final String tableName, final Set<String> wals) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    checkNotNull(tableName);

    final TInfo tinfo = Tracer.traceInfo();
    final TCredentials rpcCreds = context.rpcCreds();

    // Ask the master if the table is fully replicated given these WALs, but don't poll inside the master
    boolean drained = false;
    while (!drained) {
      drained = getMasterDrain(tinfo, rpcCreds, tableName, wals);

      if (!drained) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Thread interrupted", e);
        }
      }
    }
  }

  protected boolean getMasterDrain(final TInfo tinfo, final TCredentials rpcCreds, final String tableName, final Set<String> wals) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException {
    return MasterClient.execute(context, new ClientExecReturn<Boolean,Client>() {
      @Override
      public Boolean execute(Client client) throws Exception {
        return client.drainReplicationTable(tinfo, rpcCreds, tableName, wals);
      }
    });
  }

  protected Text getTableId(Connector conn, String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    TableOperations tops = conn.tableOperations();

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

    Connector conn = context.getConnector();
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
