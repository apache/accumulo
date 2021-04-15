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
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagerClient {
  private static final Logger log = LoggerFactory.getLogger(ManagerClient.class);

  public static ManagerClientService.Client getConnectionWithRetry(ClientContext context) {
    while (true) {

      ManagerClientService.Client result = getConnection(context);
      if (result != null)
        return result;
      sleepUninterruptibly(250, TimeUnit.MILLISECONDS);
    }
  }

  public static ManagerClientService.Client getConnection(ClientContext context) {
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
      return ThriftUtil.getClientNoTimeout(new ManagerClientService.Client.Factory(), manager,
          context);
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

  public static void close(ManagerClientService.Iface iface) {
    TServiceClient client = (TServiceClient) iface;
    if (client != null && client.getInputProtocol() != null
        && client.getInputProtocol().getTransport() != null) {
      ThriftTransportPool.getInstance().returnTransport(client.getInputProtocol().getTransport());
    } else {
      log.debug("Attempt to close null connection to the manager", new Exception());
    }
  }

  public static <T> T execute(ClientContext context,
      ClientExecReturn<T,ManagerClientService.Client> exec)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    ManagerClientService.Client client = null;
    while (true) {
      try {
        client = getConnectionWithRetry(context);
        return exec.execute(client);
      } catch (TTransportException tte) {
        log.debug("ManagerClient request failed, retrying ... ", tte);
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (AccumuloException e) {
        throw e;
      } catch (ThriftTableOperationException e) {
        switch (e.getType()) {
          case NAMESPACE_NOTFOUND:
            throw new TableNotFoundException(e.getTableName(), new NamespaceNotFoundException(e));
          case NOTFOUND:
            throw new TableNotFoundException(e);
          default:
            throw new AccumuloException(e);
        }
      } catch (ThriftNotActiveServiceException e) {
        // Let it loop, fetching a new location
        log.debug("Contacted a Manager which is no longer active, retrying");
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          close(client);
      }
    }
  }

  public static void executeGeneric(ClientContext context,
      ClientExec<ManagerClientService.Client> exec)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    ManagerClientService.Client client = null;
    while (true) {
      try {
        client = getConnectionWithRetry(context);
        exec.execute(client);
        break;
      } catch (TTransportException tte) {
        log.debug("ManagerClient request failed, retrying ... ", tte);
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (AccumuloException e) {
        throw e;
      } catch (ThriftTableOperationException e) {
        switch (e.getType()) {
          case NAMESPACE_NOTFOUND:
            throw new TableNotFoundException(e.getTableName(), new NamespaceNotFoundException(e));
          case NOTFOUND:
            throw new TableNotFoundException(e);
          default:
            throw new AccumuloException(e);
        }
      } catch (ThriftNotActiveServiceException e) {
        // Let it loop, fetching a new location
        log.debug("Contacted a Manager which is no longer active, re-creating"
            + " the connection to the active Manager");
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          close(client);
      }
    }
  }

  public static void executeTable(ClientContext context,
      ClientExec<ManagerClientService.Client> exec)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    executeGeneric(context, exec);
  }

  public static void executeNamespace(ClientContext context,
      ClientExec<ManagerClientService.Client> exec)
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    try {
      executeGeneric(context, exec);
    } catch (TableNotFoundException e) {
      if (e.getCause() instanceof NamespaceNotFoundException)
        throw (NamespaceNotFoundException) e.getCause();
    }
  }

  public static void executeVoid(ClientContext context,
      ClientExec<ManagerClientService.Client> exec)
      throws AccumuloException, AccumuloSecurityException {
    try {
      executeGeneric(context, exec);
    } catch (TableNotFoundException e) {
      throw new AssertionError(e);
    }
  }
}
