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
package org.apache.accumulo.tserver.replication;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.replication.RemoteReplicationErrorCode;
import org.apache.accumulo.core.replication.thrift.KeyValues;
import org.apache.accumulo.core.replication.thrift.RemoteReplicationException;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer.Iface;
import org.apache.accumulo.core.replication.thrift.WalEdits;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class ReplicationServicerHandler implements Iface {
  private static final Logger log = LoggerFactory.getLogger(ReplicationServicerHandler.class);

  private Instance inst;
  private AccumuloConfiguration conf;

  public ReplicationServicerHandler(Instance inst, AccumuloConfiguration conf) {
    this.inst = inst;
    this.conf = conf;
  }
  

  @Override
  public long replicateLog(int remoteTableId, WalEdits data) throws RemoteReplicationException, TException {
    log.debug("Got replication request to tableID {} with {} edits", remoteTableId, data.getEditsSize());

    String tableId = Integer.toString(remoteTableId);
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();

    BatchWriter bw = null;
    try {
      for (ByteBuffer edit : data.getEdits()) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(edit.array()));
        try {
          key.readFields(dis);
          value.readFields(dis);
        } catch (IOException e) {
          log.error("Could not deserialize edit from stream", e);
          throw new RemoteReplicationException(RemoteReplicationErrorCode.COULD_NOT_DESERIALIZE.ordinal(), "Could not deserialize edit from stream");
        }
    
        // Create the batchScanner if we don't already have one.
        if (null == bw) {
          bw = getBatchWriter(tableId);
        }

        try {
          bw.addMutations(value.mutations);
        } catch (MutationsRejectedException e) {
          log.error("Could not apply mutations to {}", remoteTableId);
          throw new RemoteReplicationException(RemoteReplicationErrorCode.COULD_NOT_APPLY.ordinal(), "Could not apply mutations to " + remoteTableId);
        }
      }
    } finally {
      if (null != bw) {
        try {
          bw.close();
        } catch (MutationsRejectedException e) {
          log.error("Could not apply mutations to {}", remoteTableId);
          throw new RemoteReplicationException(RemoteReplicationErrorCode.COULD_NOT_APPLY.ordinal(), "Could not apply mutations to " + remoteTableId);
        }
      }
    }
      
    
    return data.getEditsSize();
  }

  protected BatchWriter getBatchWriter(String tableId) throws RemoteReplicationException {
    Credentials creds = SystemCredentials.get();
    Connector conn;
    String tableName = null;
    try {
      conn = inst.getConnector(creds.getPrincipal(), creds.getToken());
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.error("Could not get connector with system credentials. Something is very wrong", e);
      throw new RemoteReplicationException(RemoteReplicationErrorCode.CANNOT_AUTHENTICATE.ordinal(), "Cannot get Connector with system credentials");
    }

    for (Entry<String,String> nameToId : conn.tableOperations().tableIdMap().entrySet()) {
      if (tableId.equals(nameToId.getValue())) {
        tableName = nameToId.getKey();
        break;
      }
    }

    if (null == tableName) {
      log.error("Table with id of " + tableId + " does not exist");
      throw new RemoteReplicationException(RemoteReplicationErrorCode.TABLE_DOES_NOT_EXIST.ordinal(), "Table with id of " + tableId + " does not exist");
    }

    try {
      return conn.createBatchWriter(tableName, new BatchWriterConfig());
    } catch (TableNotFoundException e) {
      log.error("Table with id of " + tableId + " does not exist");
      throw new RemoteReplicationException(RemoteReplicationErrorCode.TABLE_DOES_NOT_EXIST.ordinal(), "Table with id of " + tableId + " does not exist");
    }
  }

  @Override
  public long replicateKeyValues(int remoteTableId, KeyValues data) throws RemoteReplicationException, TException {
    throw new UnsupportedOperationException();
  }

}
