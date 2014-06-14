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

import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ServerConfigurationUtil;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.replication.AccumuloReplicationReplayer;
import org.apache.accumulo.core.replication.thrift.KeyValues;
import org.apache.accumulo.core.replication.thrift.RemoteReplicationErrorCode;
import org.apache.accumulo.core.replication.thrift.RemoteReplicationException;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer.Iface;
import org.apache.accumulo.core.replication.thrift.WalEdits;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class ReplicationServicerHandler implements Iface {
  private static final Logger log = LoggerFactory.getLogger(ReplicationServicerHandler.class);

  private Instance inst;

  public ReplicationServicerHandler(Instance inst) {
    this.inst = inst;
  }

  @Override
  public long replicateLog(int remoteTableId, WalEdits data, TCredentials tcreds) throws RemoteReplicationException, TException {
    log.debug("Got replication request to tableID {} with {} edits", remoteTableId, data.getEditsSize());

    String tableId = Integer.toString(remoteTableId);
    Credentials creds = Credentials.fromThrift(tcreds);
    Connector conn;
    String tableName;

    try {
      conn = inst.getConnector(creds.getPrincipal(), creds.getToken());
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.error("Could not get connection", e);
      throw new RemoteReplicationException(RemoteReplicationErrorCode.CANNOT_AUTHENTICATE, "Cannot get connector as " + creds.getPrincipal());
    }

    try {
      tableName = Tables.getTableName(inst, tableId);
    } catch (TableNotFoundException e) {
      log.error("Could not find table with id {}", tableId);
      throw new RemoteReplicationException(RemoteReplicationErrorCode.TABLE_DOES_NOT_EXIST, "Table with id " + tableId + " does not exist");
    }

    AccumuloConfiguration conf = ServerConfigurationUtil.getConfiguration(inst);

    Map<String,String> replicationHandlers = conf.getAllPropertiesWithPrefix(Property.TSERV_REPLICATION_REPLAYERS);
    String propertyForHandlerTable = Property.TSERV_REPLICATION_REPLAYERS.getKey() + tableId;

    String handlerClassForTable = replicationHandlers.get(propertyForHandlerTable);
    if (null == handlerClassForTable) {
      if (!replicationHandlers.isEmpty()) {
        log.debug("Could not find replication replayer for {}", tableId);
      }
      handlerClassForTable = conf.get(Property.TSERV_REPLICATION_DEFAULT_HANDLER);
    }

    log.debug("Using {} replication replayer for table {}", handlerClassForTable, tableId);

    // Get class for replayer
    Class<? extends AccumuloReplicationReplayer> clz;
    try {
      Class<?> untypedClz = Class.forName(handlerClassForTable);
      clz = untypedClz.asSubclass(AccumuloReplicationReplayer.class);
    } catch (ClassNotFoundException e) {
      log.error("Could not instantiate replayer class {}", handlerClassForTable, e);
      throw new RemoteReplicationException(RemoteReplicationErrorCode.CANNOT_INSTANTIATE_REPLAYER, "Could not instantiate replayer class "
          + handlerClassForTable);
    }

    // Create an instance
    AccumuloReplicationReplayer replayer;
    try {
      replayer = clz.newInstance();
    } catch (InstantiationException | IllegalAccessException e1) {
      log.error("Could not instantiate replayer class {}", clz.getName());
      throw new RemoteReplicationException(RemoteReplicationErrorCode.CANNOT_INSTANTIATE_REPLAYER, "Could not instantiate replayer class"
          + clz.getName());
    }

    long entriesReplicated = replayer.replicateLog(conn, tableName, data);

    return entriesReplicated;
  }

  @Override
  public long replicateKeyValues(int remoteTableId, KeyValues data, TCredentials creds) throws RemoteReplicationException, TException {
    throw new UnsupportedOperationException();
  }

}
