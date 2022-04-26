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
package org.apache.accumulo.core.rpc.clients;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.ClientService.Client;
import org.apache.accumulo.core.clientImpl.thrift.ClientService.Client.Factory;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes.ThriftClientType;
import org.apache.accumulo.core.util.Pair;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientServiceThriftClient extends ThriftClientType<Client,Factory>
    implements TServerClient<Client> {

  private static final Logger LOG = LoggerFactory.getLogger(ClientServiceThriftClient.class);
  private volatile Boolean warnedAboutTServersBeingDown = false;

  ClientServiceThriftClient(String serviceName, Factory clientFactory) {
    super(serviceName, clientFactory);
  }

  @Override
  public Pair<String,Client> getTabletServerConnection(ClientContext context,
      boolean preferCachedConnections) throws TTransportException {
    return getTabletServerConnection(LOG, this, context, preferCachedConnections,
        warnedAboutTServersBeingDown);
  }

  @Override
  public <R> R execute(ClientContext context, Exec<R,Client> exec)
      throws AccumuloException, AccumuloSecurityException {
    return execute(LOG, context, exec);
  }

  @Override
  public void executeVoid(ClientContext context, ExecVoid<Client> exec)
      throws AccumuloException, AccumuloSecurityException {
    executeVoid(LOG, context, exec);
  }

}
