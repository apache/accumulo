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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.manager.thrift.FateService;
import org.apache.accumulo.core.rpc.ThriftClientTypes;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FateManagerClient {
  private static final Logger log = LoggerFactory.getLogger(FateManagerClient.class);

  public static FateService.Client getConnectionWithRetry(ClientContext context) {
    while (true) {

      FateService.Client result = getConnection(context);
      if (result != null)
        return result;
      sleepUninterruptibly(250, MILLISECONDS);
    }
  }

  private static FateService.Client getConnection(ClientContext context) {
    checkArgument(context != null, "context is null");

    List<String> locations = context.getManagerLocations();

    if (locations.isEmpty()) {
      log.debug("No managers...");
      return null;
    }

    HostAndPort manager = HostAndPort.fromString(locations.get(0));
    if (manager.getPort() == 0)
      return null;

    try {
      // Manager requests can take a long time: don't ever time out
      return ThriftUtil.getClientNoTimeout(ThriftClientTypes.FATE, manager, context);
    } catch (TTransportException tte) {
      Throwable cause = tte.getCause();
      if (cause != null && cause instanceof UnknownHostException) {
        // do not expect to recover from this
        throw new RuntimeException(tte);
      }
      log.debug("Failed to connect to manager=" + manager + ", will retry... ", tte);
      return null;
    }
  }

  public static void close(FateService.Iface iface, ClientContext context) {
    TServiceClient client = (TServiceClient) iface;
    if (client != null && client.getInputProtocol() != null
        && client.getInputProtocol().getTransport() != null) {
      context.getTransportPool().returnTransport(client.getInputProtocol().getTransport());
    } else {
      log.debug("Attempt to close null connection to the manager", new Exception());
    }
  }

  public static boolean cancelFateOperation(ClientContext context, long txid)
      throws AccumuloException, AccumuloSecurityException {
    while (true) {
      FateService.Client client = null;
      try {
        client = getConnectionWithRetry(context);
        return client.cancelFateOperation(TraceUtil.traceInfo(), context.rpcCreds(), txid);
      } catch (TTransportException tte) {
        log.debug("ManagerClient request failed, retrying ... ", tte);
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (ThriftTableOperationException e) {
        throw new AccumuloException(e);
      } catch (ThriftNotActiveServiceException e) {
        // Let it loop, fetching a new location
        log.debug("Contacted a Manager which is no longer active, re-creating"
            + " the connection to the active Manager");
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          close(client, context);
      }
    }
  }

}
