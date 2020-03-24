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
package org.apache.accumulo.core.clientImpl;

import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ReplicationOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationOperationsImpl implements ReplicationOperations {
  private static final Logger log = LoggerFactory.getLogger(ReplicationOperationsImpl.class);

  private final ClientContext context;

  public ReplicationOperationsImpl(ClientContext context) {
    requireNonNull(context);
    this.context = context;
  }

  @Override
  public void addPeer(final String name, final String replicaType)
      throws AccumuloException, AccumuloSecurityException {
    requireNonNull(name);
    requireNonNull(replicaType);
    context.instanceOperations().setProperty(Property.REPLICATION_PEERS.getKey() + name,
        replicaType);
  }

  @Override
  public void removePeer(final String name) throws AccumuloException, AccumuloSecurityException {
    requireNonNull(name);
    context.instanceOperations().removeProperty(Property.REPLICATION_PEERS.getKey() + name);
  }

  @Override
  public void drain(String tableName)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    requireNonNull(tableName);

    Set<String> wals = referencedFiles(tableName);

    drain(tableName, wals);
  }

  @Override
  public void drain(final String tableName, final Set<String> wals)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    requireNonNull(tableName);

    final TInfo tinfo = TraceUtil.traceInfo();
    final TCredentials rpcCreds = context.rpcCreds();

    // Ask the master if the table is fully replicated given these WALs, but don't poll inside the
    // master
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

  protected boolean getMasterDrain(final TInfo tinfo, final TCredentials rpcCreds,
      final String tableName, final Set<String> wals)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    return MasterClient.execute(context,
        client -> client.drainReplicationTable(tinfo, rpcCreds, tableName, wals));
  }

  protected TableId getTableId(AccumuloClient client, String tableName)
      throws TableNotFoundException {
    TableOperations tops = client.tableOperations();

    if (!client.tableOperations().exists(tableName)) {
      throw new TableNotFoundException(null, tableName, null);
    }

    String tableId = null;
    while (tableId == null) {
      tableId = tops.tableIdMap().get(tableName);
      if (tableId == null) {
        sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
      }
    }

    return TableId.of(tableId);
  }

  @Override
  public Set<String> referencedFiles(String tableName) throws TableNotFoundException {
    requireNonNull(tableName);

    log.debug("Collecting referenced files for replication of table {}", tableName);

    TableId tableId = getTableId(context, tableName);

    log.debug("Found id of {} for name {}", tableId, tableName);

    // Get the WALs currently referenced by the table
    BatchScanner metaBs = context.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 4);
    metaBs.setRanges(Collections.singleton(MetadataSchema.TabletsSection.getRange(tableId)));
    metaBs.fetchColumnFamily(LogColumnFamily.NAME);
    Set<String> wals = new HashSet<>();
    try {
      for (Entry<Key,Value> entry : metaBs) {
        LogEntry logEntry = LogEntry.fromKeyValue(entry.getKey(), entry.getValue());
        wals.add(new Path(logEntry.filename).toString());
      }
    } finally {
      metaBs.close();
    }

    // And the WALs that need to be replicated for this table
    metaBs = context.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 4);
    metaBs.setRanges(Collections.singleton(ReplicationSection.getRange()));
    metaBs.fetchColumnFamily(ReplicationSection.COLF);
    try {
      Text buffer = new Text();
      for (Entry<Key,Value> entry : metaBs) {
        if (tableId.equals(ReplicationSection.getTableId(entry.getKey()))) {
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
