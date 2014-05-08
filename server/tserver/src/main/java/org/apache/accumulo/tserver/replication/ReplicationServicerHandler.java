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

import org.apache.accumulo.core.replication.thrift.KeyValues;
import org.apache.accumulo.core.replication.thrift.RemoteReplicationException;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer.Iface;
import org.apache.accumulo.core.replication.thrift.WalEdits;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class ReplicationServicerHandler implements Iface {
  private static final Logger log = LoggerFactory.getLogger(ReplicationServicerHandler.class);

  @Override
  public long replicateLog(int remoteTableId, WalEdits data) throws RemoteReplicationException, TException {
    log.error("Got replication request to tableID {} with {} edits", remoteTableId, data.getEditsSize());
    return data.getEditsSize();
  }

  @Override
  public long replicateKeyValues(int remoteTableId, KeyValues data) throws RemoteReplicationException, TException {
    throw new UnsupportedOperationException();
  }

}
