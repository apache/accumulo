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

import java.net.UnknownHostException;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.log4j.Logger;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;

public class MasterClient {
  private static final Logger log = Logger.getLogger(MasterClient.class);

  public static MasterClientService.Client getConnectionWithRetry(Instance instance) {
    ArgumentChecker.notNull(instance);

    while (true) {

      MasterClientService.Client result = getConnection(instance);
      if (result != null)
        return result;
      UtilWaitThread.sleep(250);
    }

  }

  public static MasterClientService.Client getConnection(Instance instance) {
    List<String> locations = instance.getMasterLocations();

    if (locations.size() == 0) {
      log.debug("No masters...");
      return null;
    }

    String master = locations.get(0);
    if (master.endsWith(":0"))
      return null;

    try {
      // Master requests can take a long time: don't ever time out
      MasterClientService.Client client = ThriftUtil.getClientNoTimeout(new MasterClientService.Client.Factory(), master,
          ServerConfigurationUtil.getConfiguration(instance));
      return client;
    } catch (TTransportException tte) {
      if (tte.getCause().getClass().equals(UnknownHostException.class)) {
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

  public static <T> T execute(Instance instance, ClientExecReturn<T,MasterClientService.Client> exec) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    MasterClientService.Client client = null;
    while (true) {
      try {
        client = getConnectionWithRetry(instance);
        return exec.execute(client);
      } catch (TTransportException tte) {
        log.debug("MasterClient request failed, retrying ... ", tte);
        UtilWaitThread.sleep(100);
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
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          close(client);
      }
    }
  }

  public static void executeGeneric(Instance instance, ClientExec<MasterClientService.Client> exec) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    MasterClientService.Client client = null;
    while (true) {
      try {
        client = getConnectionWithRetry(instance);
        exec.execute(client);
        break;
      } catch (TTransportException tte) {
        log.debug("MasterClient request failed, retrying ... ", tte);
        UtilWaitThread.sleep(100);
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
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          close(client);
      }
    }
  }

  public static void executeTable(Instance instance, ClientExec<MasterClientService.Client> exec) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    executeGeneric(instance, exec);
  }

  public static void executeNamespace(Instance instance, ClientExec<MasterClientService.Client> exec) throws AccumuloException, AccumuloSecurityException,
      NamespaceNotFoundException {
    try {
      executeGeneric(instance, exec);
    } catch (TableNotFoundException e) {
      if (e.getCause() instanceof NamespaceNotFoundException)
        throw (NamespaceNotFoundException) e.getCause();
    }
  }

  public static void execute(Instance instance, ClientExec<MasterClientService.Client> exec) throws AccumuloException, AccumuloSecurityException {
    try {
      executeGeneric(instance, exec);
    } catch (TableNotFoundException e) {
      throw new AssertionError(e);
    }
  }
}
