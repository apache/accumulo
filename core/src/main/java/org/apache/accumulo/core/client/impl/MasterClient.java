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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterClient {
  private static final Logger log = LoggerFactory.getLogger(MasterClient.class);

  public static MasterClientService.Client getConnectionWithRetry(ClientContext context) {
    while (true) {

      MasterClientService.Client result = getConnection(context);
      if (result != null)
        return result;
      sleepUninterruptibly(250, TimeUnit.MILLISECONDS);
    }
  }

  public static MasterClientService.Client getConnection(ClientContext context) {
    checkArgument(context != null, "context is null");

    List<String> locations = context.getInstance().getMasterLocations();

    if (locations.size() == 0) {
      log.debug("No masters...");
      return null;
    }

    HostAndPort master = HostAndPort.fromString(locations.get(0));
    if (0 == master.getPort())
      return null;

    try {
      // Master requests can take a long time: don't ever time out
      MasterClientService.Client client = ThriftUtil.getClientNoTimeout(new MasterClientService.Client.Factory(), master, context);
      return client;
    } catch (TTransportException tte) {
      Throwable cause = tte.getCause();
      if (null != cause && cause instanceof UnknownHostException) {
        // do not expect to recover from this
        throw new RuntimeException(tte);
      }
      log.debug("Failed to connect to master=" + master + ", will retry... ", tte);
      return null;
    }
  }

  public static void close(MasterClientService.Iface iface) {
    TServiceClient client = (TServiceClient) iface;
    if (client != null && client.getInputProtocol() != null && client.getInputProtocol().getTransport() != null) {
      ThriftTransportPool.getInstance().returnTransport(client.getInputProtocol().getTransport());
    } else {
      log.debug("Attempt to close null connection to the master", new Exception());
    }
  }

  public static <T> T execute(ClientContext context, ClientExecReturn<T,MasterClientService.Client> exec) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    MasterClientService.Client client = null;
    while (true) {
      try {
        client = getConnectionWithRetry(context);
        return exec.execute(client);
      } catch (TTransportException tte) {
        log.debug("MasterClient request failed, retrying ... ", tte);
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
        log.debug("Contacted a Master which is no longer active, retrying");
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          close(client);
      }
    }
  }

  public static void executeGeneric(ClientContext context, ClientExec<MasterClientService.Client> exec) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    MasterClientService.Client client = null;
    while (true) {
      try {
        client = getConnectionWithRetry(context);
        exec.execute(client);
        break;
      } catch (TTransportException tte) {
        log.debug("MasterClient request failed, retrying ... ", tte);
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
        log.debug("Contacted a Master which is no longer active, re-creating the connection to the active Master");
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          close(client);
      }
    }
  }

  public static void executeTable(ClientContext context, ClientExec<MasterClientService.Client> exec) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    executeGeneric(context, exec);
  }

  public static void executeNamespace(ClientContext context, ClientExec<MasterClientService.Client> exec) throws AccumuloException, AccumuloSecurityException,
      NamespaceNotFoundException {
    try {
      executeGeneric(context, exec);
    } catch (TableNotFoundException e) {
      if (e.getCause() instanceof NamespaceNotFoundException)
        throw (NamespaceNotFoundException) e.getCause();
    }
  }

  public static void executeVoid(ClientContext context, ClientExec<MasterClientService.Client> exec) throws AccumuloException, AccumuloSecurityException {
    try {
      executeGeneric(context, exec);
    } catch (TableNotFoundException e) {
      throw new AssertionError(e);
    }
  }
}
