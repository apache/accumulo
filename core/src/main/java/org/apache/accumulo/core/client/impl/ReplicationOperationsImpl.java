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
import java.util.Map.Entry;

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
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 
 */
public class ReplicationOperationsImpl implements ReplicationOperations {
  private static final Logger log = LoggerFactory.getLogger(ReplicationOperationsImpl.class);

  private Instance inst;
  private Credentials creds;

  public ReplicationOperationsImpl(Instance inst, Credentials creds) {
    this.inst = inst;
    this.creds = creds;
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

    Connector conn = inst.getConnector(creds.getPrincipal(), creds.getToken());
    TableOperations tops = conn.tableOperations();
    while (!tops.exists(ReplicationTable.NAME)) {
      UtilWaitThread.sleep(200);
    }

    String strTableId = null;
    while (null == strTableId) {
      strTableId = tops.tableIdMap().get(tableName);
      if (null == strTableId) {
        UtilWaitThread.sleep(200);
      }
    }

    Text tableId = new Text(strTableId);

    boolean allMetadataRefsReplicated = false;
    while (!allMetadataRefsReplicated) {
      BatchScanner bs = conn.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 4);
      bs.setRanges(Collections.singleton(new Range(ReplicationSection.getRange())));
      bs.fetchColumnFamily(ReplicationSection.COLF);
      try {
        allMetadataRefsReplicated = allReferencesReplicated(bs, tableId);
      } finally {
        bs.close();
      }
    }

    boolean allReplicationRefsReplicated = false;
    while (!allReplicationRefsReplicated) {
      BatchScanner bs = conn.createBatchScanner(ReplicationTable.NAME, Authorizations.EMPTY, 4);
      bs.setRanges(Collections.singleton(new Range()));
      try {
        allReplicationRefsReplicated = allReferencesReplicated(bs, tableId);
      } finally {
        bs.close();
      }
    }
  }

  protected boolean allReferencesReplicated(BatchScanner bs, Text tableId) {
    Text holder = new Text();
    for (Entry<Key,Value> entry : bs) {
      entry.getKey().getColumnQualifier(holder);
      if (tableId.equals(holder)) {
        try {
          Status stat = Status.parseFrom(entry.getValue().get());
          if (!StatusUtil.isFullyReplicated(stat)) {
            return false;
          }
        } catch (InvalidProtocolBufferException e) {
          log.warn("Could not parse protobuf for {}", entry.getKey(), e);
        }
      }
    }

    return true;
  }
}
