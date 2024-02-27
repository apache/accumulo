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
package org.apache.accumulo.core.rpc.clients;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.ClientService.Client;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShellAuthenticatorThriftClient extends ThriftClientTypes<Client>
    implements TServerClient<Client> {

  private static final Logger LOG = LoggerFactory.getLogger(ShellAuthenticatorThriftClient.class);
  private final AtomicBoolean warnedAboutTServersBeingDown = new AtomicBoolean(false);

  public ShellAuthenticatorThriftClient(String serviceName) {
    super(serviceName, new Client.Factory());
  }

  @Override
  public Pair<String,Client> getTabletServerConnection(Logger LOG, ThriftClientTypes<Client> type,
      ClientContext context, boolean preferCachedConnections, AtomicBoolean warned)
      throws TTransportException {
    checkArgument(context != null, "context is null");
    final long rpcTimeout = context.getClientTimeoutInMillis();

    final ZooCache zc = context.getZooCache();
    final List<String> tservers = new ArrayList<>();

    for (int retries = 0; retries < 10; retries++) {
      // Cluster may not be up, wait for tservers to come online
      while (true) {
        for (String tserver : zc.getChildren(context.getZooKeeperRoot() + Constants.ZTSERVERS)) {
          tservers.add(tserver);
        }
        if (!tservers.isEmpty()) {
          break;
        }
        if (!tservers.isEmpty() && !warnedAboutTServersBeingDown.get()) {
          LOG.warn("There are no tablet servers: check that zookeeper and accumulo are running.");
          warnedAboutTServersBeingDown.set(true);
        }
      }

      // Try to connect to an online tserver
      Collections.shuffle(tservers);
      for (String tserver : tservers) {
        var zLocPath =
            ServiceLock.path(context.getZooKeeperRoot() + Constants.ZTSERVERS + "/" + tserver);
        byte[] data = zc.getLockData(zLocPath);
        if (data != null) {
          String strData = new String(data, UTF_8);
          if (!strData.equals("manager")) {
            final HostAndPort tserverClientAddress =
                new ServerServices(strData).getAddress(Service.TSERV_CLIENT);
            try {
              TTransport transport = context.getTransportPool().getTransport(tserverClientAddress,
                  rpcTimeout, context);
              Client client = ThriftUtil.createClient(type, transport);
              return new Pair<String,Client>(tserverClientAddress.toString(), client);
            } catch (TTransportException e) {
              LOG.trace("Error creating transport to {}", tserverClientAddress);
              continue;
            }
          }
        }
        LOG.warn("Failed to find an available server in the list of servers: {}", tservers);
      }
    }
    throw new TTransportException("Failed to connect to a server");
  }

  @Override
  public Pair<String,Client> getTabletServerConnection(ClientContext context,
      boolean preferCachedConnections) throws TTransportException {
    return getTabletServerConnection(LOG, this, context, preferCachedConnections,
        warnedAboutTServersBeingDown);
  }

  @Override
  public Client getConnection(ClientContext context) {
    try {
      return getTabletServerConnection(context, true).getSecond();
    } catch (TTransportException e) {
      throw new RuntimeException("Error creating client connection", e);
    }
  }

}
