/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ReplicationOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class ReplicationOperationsImpl implements ReplicationOperations {
  private static final Logger log = LoggerFactory.getLogger(ReplicationOperationsImpl.class);

  private final ClientContext context;

  public ReplicationOperationsImpl(ClientContext context) {
    this.context = requireNonNull(context);
  }

  @Override
  public void addPeer(final String name, final String replicaType)
      throws AccumuloException, AccumuloSecurityException {
    context.instanceOperations().setProperty(
        Property.REPLICATION_PEERS.getKey() + requireNonNull(name), requireNonNull(replicaType));
  }

  @Override
  public void removePeer(final String name) throws AccumuloException, AccumuloSecurityException {
    context.instanceOperations()
        .removeProperty(Property.REPLICATION_PEERS.getKey() + requireNonNull(name));
  }

  @Override
  public void drain(String tableName)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    drain(tableName, referencedFiles(requireNonNull(tableName)));
  }

  @Override
  public void drain(final String tableName, final Set<String> wals)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    requireNonNull(tableName);

    final TInfo tinfo = TraceUtil.traceInfo();
    final TCredentials rpcCreds = context.rpcCreds();

    // Ask the manager if the table is fully replicated given these WALs, but don't poll inside the
    // manager
    boolean drained = false;
    while (!drained) {
      drained = getManagerDrain(tinfo, rpcCreds, tableName, wals);

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

  protected boolean getManagerDrain(final TInfo tinfo, final TCredentials rpcCreds,
      final String tableName, final Set<String> wals)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    return ThriftClientTypes.MANAGER.execute(context,
        client -> client.drainReplicationTable(tinfo, rpcCreds, tableName, wals));
  }

  @Override
  public Set<String> referencedFiles(String tableName) throws TableNotFoundException {
    log.debug("Collecting referenced files for replication of table {}", tableName);
    TableId tableId = context.getTableId(tableName);
    log.debug("Found id of {} for name {}", tableId, tableName);

    // Get the WALs currently referenced by the table
    BatchScanner metaBs = context.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 4);
    metaBs.setRanges(Collections.singleton(TabletsSection.getRange(tableId)));
    metaBs.fetchColumnFamily(LogColumnFamily.NAME);
    Set<String> wals = new HashSet<>();
    try {
      for (Entry<Key,Value> entry : metaBs) {
        LogEntry logEntry = LogEntry.fromMetaWalEntry(entry);
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
